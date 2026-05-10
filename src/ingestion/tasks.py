"""Taskiq broker + ``process_document`` task.

Stage 8 wiring.  The worker is intentionally thin — it composes the
modules built in Stages 2 / 3 / 6 / 7 with the live storage
backends.

Run::

    uv run taskiq worker src.ingestion.tasks:broker --workers 1
"""

from __future__ import annotations

import uuid
from pathlib import Path

from loguru import logger
from taskiq_aio_pika import AioPikaBroker

from src.config import settings
from src.ingestion.embeddings import build_embedding_model
from src.ingestion.identifier_transform import (
    IdentifierCanonicalizationTransform,
    inject_canonical_entities,
)
from src.ingestion.pipeline import (
    build_ingestion_pipeline,
    read_documents,
)
from src.retrieval.llm import build_llm
from src.retrieval.vector_index import (
    build_vector_index,
    build_vector_store,
    index_nodes,
)
from src.storage.postgres import AsyncPostgres


broker = AioPikaBroker(settings.rabbitmq.url)


@broker.task
async def process_document(doc_id: str, path: str) -> None:
    """Run the full ingestion chain for one uploaded file.

    parse → chunk → identifier-canonicalize → vector index →
    (optional) graph inject → mark `completed` in Postgres.

    Failures are caught and surfaced via Postgres status `failed`
    + `error` text — the API client polling
    `GET /api/v1/ingest/{job_id}` sees the failure mode.
    """
    pg = AsyncPostgres()
    target = Path(path)
    job_uuid = uuid.UUID(doc_id)

    try:
        await pg.update_status(job_uuid, status="processing")

        embed_model = build_embedding_model()
        pipeline = build_ingestion_pipeline(
            extra_transformations=[IdentifierCanonicalizationTransform()],
        )
        docs = read_documents(target.parent, recursive=False)
        # Filter to just this file (SimpleDirectoryReader pulled the dir).
        docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
        if not docs:
            raise FileNotFoundError(f"file not found in reader output: {target}")

        nodes = pipeline.run(documents=docs)
        store = build_vector_store()
        index = build_vector_index(store, embed_model)
        index_nodes(index, nodes)

        # Graph build — best-effort, two phases:
        #   1. Deterministic canonical injection: phone E.164, INN,
        #      OGRN, ... are upserted as labelled nodes BEFORE the
        #      LLM extractor runs so it can attach relations to them.
        #   2. SchemaLLMPathExtractor over the chunks pulls
        #      free-form entities (Person, Organization, Event, ...)
        #      AND relations between them via an LLM call per chunk.
        # Both phases are wrapped in try/except so a Neo4j or LLM
        # outage doesn't block the vector-only path from completing.
        try:
            from src.graph.index import (
                build_kg_extractor,
                build_property_graph_index,
            )
            from src.graph.store import build_neo4j_graph_store

            graph_store = build_neo4j_graph_store()
            inject_canonical_entities(graph_store, nodes)
            try:
                extractor = build_kg_extractor(build_llm())
                build_property_graph_index(
                    graph_store=graph_store,
                    embed_model=embed_model,
                    extractor=extractor,
                    nodes=nodes,
                )
                logger.info(
                    "graph LLM extraction done  doc_id={d}", d=doc_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "graph LLM extraction failed: {err}", err=exc,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph injection failed: {err}", err=exc)

        await pg.update_status(job_uuid, status="completed")
        logger.info("ingestion done  doc_id={d}  nodes={n}", d=doc_id, n=len(nodes))
    except Exception as exc:  # noqa: BLE001 — surface to client
        logger.exception("ingestion failed  doc_id={d}", d=doc_id)
        await pg.update_status(job_uuid, status="failed", error=str(exc))

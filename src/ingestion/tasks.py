"""Taskiq broker + `process_document` task.

The worker is intentionally thin — it composes existing factory
helpers (`build_ingestion_pipeline`, `build_vector_index`,
`build_kg_extractor`, `EntityDescriptionEnricher`) against live
storage backends.  Identifier canonicalization is built into
`build_ingestion_pipeline` by default — no need to pass it as an
extra transformation any more.

Run::

    uv run taskiq worker src.ingestion.tasks:broker --workers 1
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from loguru import logger
from taskiq_aio_pika import AioPikaBroker

from src.config import settings
from src.graph.enrich import EntityDescriptionEnricher
from src.graph.index import build_kg_extractor, build_property_graph_index
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.ingestion.identifier_transform import inject_canonical_entities
from src.ingestion.pipeline import build_ingestion_pipeline, read_documents
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

    Flow:
      1. Read → split → identifier-canon (built into pipeline).
      2. Insert chunks into Milvus.
      3. (best-effort) Inject canonical entities into Neo4j.
      4. (best-effort) Run KG extractor over chunks → triples land
         in Neo4j.
      5. (best-effort) Enrich entities with LLM-generated
         descriptions.
      6. Mark `completed` in Postgres (or `failed` with error).

    Steps 3-5 are wrapped in try/except so a Neo4j or LLM outage
    doesn't block the vector-only path from completing.
    """
    pg = AsyncPostgres()
    target = Path(path)
    job_uuid = uuid.UUID(doc_id)
    llm = build_llm()
    embed_model = build_embedding_model()

    try:
        await pg.update_status(job_uuid, status="processing")

        # 1. parse + chunk + identifier-canon (canon is built-in)
        pipeline = build_ingestion_pipeline()
        docs = read_documents(target.parent, recursive=False)
        docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
        if not docs:
            raise FileNotFoundError(f"file not in reader output: {target}")

        # `pipeline.arun` is the async variant — sync `.run` internally
        # calls `asyncio.run` and explodes inside the taskiq event loop.
        nodes = await pipeline.arun(documents=docs)

        # 2. vector indexing
        store = build_vector_store()
        index = build_vector_index(store, embed_model)
        index_nodes(index, nodes)

        # 3-5. graph build — best-effort
        try:
            graph_store = build_neo4j_graph_store()
            inject_canonical_entities(graph_store, nodes)
            try:
                extractor = build_kg_extractor(llm)
                # `PropertyGraphIndex(nodes=...)` constructor runs the
                # kg_extractors synchronously via `asyncio.run`; that
                # blows up inside our running event loop, so offload to
                # a worker thread (which gets its own loop).
                await asyncio.to_thread(
                    build_property_graph_index,
                    graph_store=graph_store,
                    embed_model=embed_model,
                    extractor=extractor,
                    nodes=nodes,
                )
                # Second pass: fill description on each entity.  Use
                # the async path directly — `__call__` would also hit
                # the `asyncio.run` trap.
                await EntityDescriptionEnricher(llm=llm).acall(nodes)
                logger.info(
                    "graph extraction + enrichment done  doc_id={d}",
                    d=doc_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "graph LLM extraction failed: {err}", err=exc,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph injection failed: {err}", err=exc)

        await pg.update_status(job_uuid, status="completed")
        logger.info(
            "ingestion done  doc_id={d}  nodes={n}",
            d=doc_id, n=len(nodes),
        )
    except Exception as exc:  # noqa: BLE001 — surface to client
        logger.exception("ingestion failed  doc_id={d}", d=doc_id)
        await pg.update_status(job_uuid, status="failed", error=str(exc))

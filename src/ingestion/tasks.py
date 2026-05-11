"""Taskiq broker + `process_document` task.

The worker is intentionally thin — it composes existing factory
helpers (`build_ingestion_pipeline`, `build_vector_index`,
`build_kg_extractor`, `merge_kg_extraction`) against live
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
from src.graph.index import (
    NoOpKGExtractor,
    build_kg_extractor,
    build_property_graph_index,
)
from src.graph.merge import merge_kg_extraction
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


# Neo4j accepts only primitive properties + arrays of primitives —
# nested maps / lists-of-maps cause `Neo.ClientError.Statement.TypeError`
# when PropertyGraphIndex writes a `:Chunk` node.  Our pipeline
# attaches `canonical_identifiers` as `list[dict]` for downstream
# retrievers (Milvus tolerates it via JSON serialisation), so we
# scrub the offending keys off the in-memory nodes right before the
# graph step.  Milvus has already received the full metadata in
# step 2; the strip is graph-store-only.
_NEO4J_UNSAFE_METADATA_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
})


def _is_neo4j_safe(value):
    """A value Neo4j will accept as a node property — primitives
    or flat arrays of primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(
            v is None or isinstance(v, (str, int, float, bool))
            for v in value
        )
    return False


def _strip_neo4j_unsafe_metadata(nodes) -> None:
    """In-place: drop metadata keys whose values Neo4j would reject."""
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not md:
            continue
        for key in list(md.keys()):
            if key in _NEO4J_UNSAFE_METADATA_KEYS or not _is_neo4j_safe(md[key]):
                md.pop(key, None)


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
                # LightRAG-style flow (see NoOpKGExtractor docstring):
                #   1. extractor: one LLM call/chunk → KG_NODES_KEY /
                #      KG_RELATIONS_KEY with entity descriptions inline
                #   2. cross-chunk merger: dedup by name, concat or
                #      LLM-summary descriptions, dedup relations
                #   3. PropertyGraphIndex with NoOp extractor: pops
                #      per-chunk metadata, creates Chunk(:MENTIONS)→
                #      Entity edges, embeds entities for retrieval
                #   4. upsert merged entities+relations: overwrites
                #      per-chunk descriptions with cross-chunk merged
                extractor = build_kg_extractor(llm, mode="lightrag")
                nodes = await extractor.acall(nodes)
                merged_entities, merged_relations = await merge_kg_extraction(
                    nodes, llm,
                )
                # PropertyGraphIndex writes every chunk's metadata
                # onto its `:Chunk` node in Neo4j.  Neo4j rejects
                # nested types ("Property values can only be of
                # primitive types or arrays thereof") so strip any
                # metadata value that isn't a Neo4j-friendly scalar
                # right before that call.  Milvus already received
                # the original metadata in step 2; this only affects
                # the graph store.
                _strip_neo4j_unsafe_metadata(nodes)
                await asyncio.to_thread(
                    build_property_graph_index,
                    graph_store=graph_store,
                    embed_model=embed_model,
                    extractor=NoOpKGExtractor(),
                    nodes=nodes,
                )
                if merged_entities:
                    graph_store.upsert_nodes(merged_entities)
                if merged_relations:
                    graph_store.upsert_relations(merged_relations)
                logger.info(
                    "graph done  doc_id={d}  entities={e}  relations={r}",
                    d=doc_id,
                    e=len(merged_entities),
                    r=len(merged_relations),
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

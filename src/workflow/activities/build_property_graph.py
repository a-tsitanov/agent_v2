"""`build_property_graph` — Chunk + MENTIONS + entity/relation upsert."""

from __future__ import annotations

import asyncio

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from loguru import logger
from temporalio import activity

from src.graph.index import NoOpKGExtractor, build_property_graph_index
from src.graph.store import build_neo4j_graph_store
from src.graph.write_retry import write_with_retry
from src.ingestion.embeddings import build_embedding_model
from src.workflow.contracts import GraphBuilt, Merged
from src.workflow.heartbeat import heartbeat_every
from src.workflow.staging import build_staging_store

# Pulse interval for the blocking Neo4j writes. Must stay well under the
# activity's heartbeat_timeout (5m in graph_build.py). At 37k nodes / 60k
# edges a single upsert can outrun that window; without a continuous pulse
# Temporal fires timeout_type_heartbeat and _FAST_RETRY re-runs the whole
# activity (up to 50×), amplifying offered load into a Neo4j-CPU retry storm.
_HEARTBEAT_INTERVAL_S = 60.0

_NEO4J_UNSAFE_METADATA_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
    "canonical_identifiers_augment",
})
_PRESERVE_METADATA_KEYS: frozenset[str] = frozenset({
    KG_NODES_KEY, KG_RELATIONS_KEY,
})


def _is_neo4j_safe(value) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(
            v is None or isinstance(v, (str, int, float, bool))
            for v in value
        )
    return False


def _strip_neo4j_unsafe_metadata(nodes) -> None:
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not md:
            continue
        for key in list(md.keys()):
            if key in _PRESERVE_METADATA_KEYS:
                continue
            if key in _NEO4J_UNSAFE_METADATA_KEYS or not _is_neo4j_safe(md[key]):
                md.pop(key, None)


@activity.defn
async def build_property_graph(merged: Merged) -> GraphBuilt:
    activity.logger.info(
        "build_property_graph start  doc=%s", merged.kg.parsed.ctx.doc_id,
    )
    activity.heartbeat({"stage": "init"})

    staging = build_staging_store()
    entities, relations, nodes = await asyncio.to_thread(
        staging.read_pickle, merged.merged_entities_uri,
    )
    activity.heartbeat({
        "stage": "loaded",
        "entities": len(entities),
        "relations": len(relations),
        "chunks": len(nodes),
    })

    # Pulse on a timer throughout the blocking Neo4j region: store
    # construction (driver I/O + schema refresh + index DDL, behind a
    # process-global lock), the index build (Chunk + embedding writes), the
    # entity/relation upserts, and the lookup-index DDL all run off the loop
    # via to_thread and can each be slow under hub-node lock contention. A
    # one-shot heartbeat between steps is not enough — a single step can
    # outrun heartbeat_timeout. heartbeat_every keeps a progressing-but-slow
    # build from being mistaken for a dead one (mirrors inject_canonical).
    from src.graph.index import (
        ensure_chunk_date_indexes,
        ensure_entity_fulltext_index,
        ensure_entity_lookup_indexes,
    )
    async with heartbeat_every(_HEARTBEAT_INTERVAL_S, {"stage": "writing"}):
        graph_store = await asyncio.to_thread(build_neo4j_graph_store)
        embed_model = await asyncio.to_thread(build_embedding_model)

        _strip_neo4j_unsafe_metadata(nodes)
        activity.heartbeat({"stage": "scrubbed"})

        activity.logger.info(
            "build_property_graph building index  chunks=%d", len(nodes),
        )
        await asyncio.to_thread(
            build_property_graph_index,
            graph_store=graph_store,
            embed_model=embed_model,
            extractor=NoOpKGExtractor(),
            nodes=nodes,
        )
        activity.heartbeat({"stage": "index_built"})

        # Wrap in write_with_retry: concurrent MERGE into shared hub nodes can
        # throw a retryable Neo.TransientError (deadlock / lock timeout) under
        # max_inflight>1 — re-run the write instead of failing the document.
        if entities:
            await asyncio.to_thread(
                write_with_retry, graph_store.upsert_nodes, entities
            )
            activity.heartbeat({"stage": "entities_upserted", "count": len(entities)})
        if relations:
            await asyncio.to_thread(
                write_with_retry, graph_store.upsert_relations, relations
            )
            activity.heartbeat(
                {"stage": "relations_upserted", "count": len(relations)}
            )

        await asyncio.to_thread(ensure_entity_fulltext_index, graph_store)
        await asyncio.to_thread(ensure_entity_lookup_indexes, graph_store)
        await asyncio.to_thread(ensure_chunk_date_indexes, graph_store)
        activity.heartbeat({"stage": "indexes_ensured"})

    logger.info(
        "build_property_graph done  doc={d}  e={e}  r={r}",
        d=merged.kg.parsed.ctx.doc_id, e=len(entities), r=len(relations),
    )
    return GraphBuilt(entities=len(entities), relations=len(relations))

"""`build_property_graph` — Chunk + MENTIONS + entity/relation upsert."""

from __future__ import annotations

import asyncio

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from loguru import logger
from temporalio import activity

from src.graph.index import NoOpKGExtractor, build_property_graph_index
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.workflow.contracts import GraphBuilt, Merged
from src.workflow.staging import build_staging_store

_NEO4J_UNSAFE_METADATA_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
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
    entities, relations, nodes = staging.read_pickle(merged.merged_entities_uri)
    activity.heartbeat({
        "stage": "loaded",
        "entities": len(entities),
        "relations": len(relations),
        "chunks": len(nodes),
    })

    graph_store = build_neo4j_graph_store()
    embed_model = build_embedding_model()

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

    if entities:
        graph_store.upsert_nodes(entities)
        activity.heartbeat({"stage": "entities_upserted", "count": len(entities)})
    if relations:
        graph_store.upsert_relations(relations)
        activity.heartbeat({"stage": "relations_upserted", "count": len(relations)})

    from src.graph.index import ensure_entity_fulltext_index
    ensure_entity_fulltext_index(graph_store)
    activity.heartbeat({"stage": "fulltext_index_ensured"})

    logger.info(
        "build_property_graph done  doc={d}  e={e}  r={r}",
        d=merged.kg.parsed.ctx.doc_id, e=len(entities), r=len(relations),
    )
    return GraphBuilt(entities=len(entities), relations=len(relations))

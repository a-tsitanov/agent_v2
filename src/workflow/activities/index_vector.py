"""`index_vector` — embed + Milvus insert.

Loads parsed nodes from staging, snapshot-strips Milvus-oversized
metadata around `index_nodes`, then writes the resulting node-id list
back to the workflow.  In-memory nodes keep their full metadata so
the same pickle (re-read by the next activity) is unaffected.
"""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio import activity

from src.ingestion.embeddings import build_embedding_model
from src.retrieval.vector_index import (
    build_vector_index,
    build_vector_store,
    index_nodes,
)
from src.workflow.contracts import Indexed, Parsed
from src.workflow.staging import build_staging_store

_MILVUS_DROP_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
    "translated_text",
})


def _snapshot_metadata(nodes, keys: frozenset[str]) -> list[dict]:
    snaps: list[dict] = []
    for n in nodes:
        md = getattr(n, "metadata", None)
        snap: dict = {}
        if md:
            for k in list(md.keys()):
                if k in keys:
                    snap[k] = md.pop(k)
        snaps.append(snap)
    return snaps


def _restore_metadata(nodes, snaps: list[dict]) -> None:
    for n, snap in zip(nodes, snaps):
        if not snap:
            continue
        md = getattr(n, "metadata", None)
        if md is None:
            n.metadata = snap
        else:
            md.update(snap)


@activity.defn
async def index_vector(parsed: Parsed) -> Indexed:
    activity.logger.info(
        "index_vector start  doc=%s  chunks=%d",
        parsed.ctx.doc_id, parsed.chunk_count,
    )
    activity.heartbeat({"stage": "init", "chunks": parsed.chunk_count})

    staging = build_staging_store()
    nodes = await asyncio.to_thread(staging.read_pickle, parsed.nodes_uri)
    activity.heartbeat({"stage": "loaded", "chunks": len(nodes)})

    embed_model = build_embedding_model()
    store = build_vector_store()
    index = build_vector_index(store, embed_model)
    activity.heartbeat({"stage": "embedding_init"})

    snaps = _snapshot_metadata(nodes, _MILVUS_DROP_KEYS)
    activity.logger.info("index_vector inserting  chunks=%d", len(nodes))
    try:
        # embed + Milvus insert are sync/blocking — off the loop.
        await asyncio.to_thread(index_nodes, index, nodes)
    finally:
        _restore_metadata(nodes, snaps)

    node_ids = [getattr(n, "node_id", "") for n in nodes]
    activity.heartbeat({"stage": "indexed", "count": len(node_ids)})
    logger.info(
        "index_vector done  doc={d}  count={n}",
        d=parsed.ctx.doc_id, n=len(node_ids),
    )
    return Indexed(node_ids=node_ids, count=len(node_ids))

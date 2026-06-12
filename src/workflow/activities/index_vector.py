"""`index_vector` — embed + Milvus insert.

Loads parsed nodes from staging, snapshot-strips Milvus-oversized
metadata (and truncates pathologically large chunk text) around
`index_nodes`, then writes the resulting node-id list back to the
workflow.  In-memory nodes are restored to their full content so the
same pickle (re-read by the next activity) is unaffected.

Milvus stores each row in two VARCHAR fields, both hard-capped at
65 535 chars by the server schema (not configurable — that's the
absolute Milvus VARCHAR maximum):

  * ``_node_content`` — JSON dump of the node *minus its text*; in
    practice dominated by ``node.metadata``.
  * ``text`` — the chunk text itself, a *separate* field.

A "большой документ" overflows whichever of these crosses the cap.
Rather than maintain a denylist of known-bulky metadata keys (it has
already missed keys before), `_snapshot_for_milvus` strips by *size*:
it always removes the known scaffolding keys, then peels off the
largest remaining metadata values until ``_node_content`` fits, and
logs every key it drops so the offender is visible in the worker log.
``_snapshot_oversized_text`` does the same for the ``text`` field.
Everything is restored after the insert.
"""

from __future__ import annotations

import asyncio
import json

from llama_index.core.vector_stores.utils import node_to_metadata_dict
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

# Known doc/chunk scaffolding that is bulky *and* needed downstream
# (identifiers → inject_canonical, translation → LightRAG).  Stripped
# for the insert, restored after.  The generic size pass below catches
# anything this list misses.
_MILVUS_DROP_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
    "translated_text",
})

# Milvus VARCHAR hard cap is 65 535; stay under it with headroom for
# the JSON wrapper `_node_content` adds around the metadata.
_MILVUS_HARD_LIMIT: int = 65_535
_MILVUS_FIELD_BUDGET: int = 64_000


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
    for n, snap in zip(nodes, snaps, strict=True):
        if not snap:
            continue
        md = getattr(n, "metadata", None)
        if md is None:
            n.metadata = snap
        else:
            md.update(snap)


def _node_content_len(node) -> int:
    """Length of the ``_node_content`` VARCHAR Milvus will actually
    store for this node — computed with the same helper Milvus uses, so
    the budget check matches the server-side limit exactly."""
    return len(node_to_metadata_dict(node, remove_text=True)["_node_content"])


def _value_size(value) -> int:
    try:
        return len(json.dumps(value, default=str, ensure_ascii=False))
    except (TypeError, ValueError):
        return len(str(value))


def _snapshot_for_milvus(nodes) -> list[dict]:
    """Strip metadata so every node's ``_node_content`` fits the Milvus
    VARCHAR cap.  Returns per-node snapshots for `_restore_metadata`.

    Two passes: (1) the known bulky keys, unconditionally; (2) a
    size-driven fallback that peels off the largest remaining metadata
    value, one at a time, until the node fits — logging each drop.
    """
    snaps = _snapshot_metadata(nodes, _MILVUS_DROP_KEYS)
    for node, snap in zip(nodes, snaps, strict=True):
        md = getattr(node, "metadata", None)
        if not md:
            continue
        while md and _node_content_len(node) > _MILVUS_FIELD_BUDGET:
            key = max(md, key=lambda k: _value_size(md[k]))
            snap[key] = md.pop(key)
            logger.warning(
                "index_vector: stripped oversized metadata key={k} "
                "size={s} node={n} — would overflow Milvus _node_content",
                k=key, s=_value_size(snap[key]),
                n=getattr(node, "node_id", "?"),
            )
    return snaps


def _snapshot_oversized_text(nodes) -> list:
    """Truncate any chunk whose ``text`` field exceeds the Milvus cap
    (a chunking pathology — e.g. the semantic splitter emitting one
    huge chunk).  Returns per-node original text (or ``None``) so
    `_restore_text` can put the full content back after the insert."""
    snaps: list = []
    for n in nodes:
        text = getattr(n, "text", "") or ""
        if len(text) > _MILVUS_FIELD_BUDGET:
            logger.warning(
                "index_vector: chunk text {l} chars > Milvus cap — "
                "truncating stored copy node={n}; fix chunking upstream",
                l=len(text), n=getattr(n, "node_id", "?"),
            )
            snaps.append(text)
            n.set_content(text[:_MILVUS_FIELD_BUDGET])
        else:
            snaps.append(None)
    return snaps


def _restore_text(nodes, snaps: list) -> None:
    for n, original in zip(nodes, snaps, strict=True):
        if original is not None:
            n.set_content(original)


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

    text_snaps = _snapshot_oversized_text(nodes)
    snaps = _snapshot_for_milvus(nodes)
    activity.logger.info("index_vector inserting  chunks=%d", len(nodes))
    try:
        # embed + Milvus insert are sync/blocking — off the loop.
        await asyncio.to_thread(index_nodes, index, nodes)
    finally:
        _restore_metadata(nodes, snaps)
        _restore_text(nodes, text_snaps)

    node_ids = [getattr(n, "node_id", "") for n in nodes]
    activity.heartbeat({"stage": "indexed", "count": len(node_ids)})
    logger.info(
        "index_vector done  doc={d}  count={n}",
        d=parsed.ctx.doc_id, n=len(node_ids),
    )
    return Indexed(node_ids=node_ids, count=len(node_ids))

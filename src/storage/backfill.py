"""Legacy `doc_id` backfill — pure planning core.

Chunks indexed before `index_vector` started tagging every node with
its `doc_id` (see `src/workflow/activities/index_vector.py`) have no
`doc_id` in their Milvus metadata, so `get_chunks_by_doc_id` can't fetch
them.  This module decides which chunks need the field and resolves the
value from their `file_path` via the Postgres `documents` map — the same
path the worker downloaded the source to, which the chunk metadata still
carries.

The Milvus paging + re-insert (which reuses the normal LlamaIndex write
path so the row stays schema-consistent) lives in
`scripts/backfill_doc_id.py`; this module is pure and unit-tested.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


@dataclass
class BackfillStats:
    """Counts for one backfill planning pass."""

    total: int = 0
    already: int = 0      # chunk already had a non-empty doc_id
    resolved: int = 0     # doc_id resolved from the path map + set
    unresolved: int = 0   # missing doc_id AND no path match (left untouched)


def row_to_node(
    row: dict[str, Any],
    *,
    embedding_field: str = "embedding",
    text_field: str = "text",
) -> Any:
    """Reconstruct a `TextNode` (with text + embedding) from a Milvus row.

    Milvus stores the node JSON in `_node_content` *without* its text
    (LlamaIndex writes with `remove_text=True`) and keeps the text +
    embedding in separate fields.  Reconstruction MUST restore both — a
    re-insert with empty text would wipe the chunk body, and a missing
    embedding would force a (lossy, model-dependent) re-embed.
    """
    from llama_index.core.vector_stores.utils import metadata_dict_to_node

    node = metadata_dict_to_node(row)
    text = row.get(text_field)
    if text:
        node.set_content(str(text))
    emb = row.get(embedding_field)
    if emb is not None:
        node.embedding = list(emb)
    return node


def build_path_index(rows: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Build `{file_path: doc_id}` from `(doc_id, path)` rows.

    Blank doc_ids or paths are skipped — a chunk can only be resolved to
    a registered document with a real path."""
    return {path: doc_id for doc_id, path in rows if path and doc_id}


def _node_meta_str(node: Any, key: str) -> str:
    return str((getattr(node, "metadata", None) or {}).get(key) or "")


def plan_doc_id_backfill(
    nodes: Iterable[Any],
    path_to_doc: dict[str, str],
) -> tuple[list[Any], BackfillStats]:
    """Decide which chunks need a `doc_id` and set it in-place.

    Returns `(to_update, stats)` where `to_update` is the nodes whose
    `doc_id` was just resolved + written (caller re-inserts only these).
    Nodes that already carry a `doc_id`, or whose `file_path` isn't in the
    map, are left untouched.
    """
    to_update: list[Any] = []
    stats = BackfillStats()
    for node in nodes:
        stats.total += 1
        if _node_meta_str(node, "doc_id"):
            stats.already += 1
            continue
        doc_id = path_to_doc.get(_node_meta_str(node, "file_path"))
        if not doc_id:
            stats.unresolved += 1
            continue
        node.metadata["doc_id"] = doc_id
        to_update.append(node)
        stats.resolved += 1
    return to_update, stats


__all__ = [
    "BackfillStats",
    "build_path_index",
    "plan_doc_id_backfill",
    "row_to_node",
]

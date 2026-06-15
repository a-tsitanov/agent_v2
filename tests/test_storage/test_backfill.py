"""Tests for `src/storage/backfill.py` — legacy `doc_id` backfill core.

The pure planning logic (decide which chunks need a `doc_id`, resolve it
from the file_path→doc_id map, set it) is unit-tested here.  The Milvus
I/O (paging + re-add) lives in the CLI script and is integration-only.
"""

from __future__ import annotations

from llama_index.core.schema import TextNode
from llama_index.core.vector_stores.utils import node_to_metadata_dict

from src.storage.backfill import (
    BackfillStats,
    build_path_index,
    plan_doc_id_backfill,
    row_to_node,
)


def _node(text: str, **metadata) -> TextNode:
    return TextNode(text=text, metadata=dict(metadata))


def test_build_path_index_maps_path_to_doc_id() -> None:
    rows = [("doc-1", "/data/a.pdf"), ("doc-2", "/data/b.pdf")]
    assert build_path_index(rows) == {"/data/a.pdf": "doc-1", "/data/b.pdf": "doc-2"}


def test_build_path_index_skips_blank_rows() -> None:
    rows = [("doc-1", "/data/a.pdf"), ("", "/data/x.pdf"), ("doc-3", "")]
    assert build_path_index(rows) == {"/data/a.pdf": "doc-1"}


def test_plan_backfills_missing_doc_id_from_path() -> None:
    nodes = [
        _node("chunk a", file_path="/data/a.pdf"),            # missing → resolve
        _node("chunk b", file_path="/data/a.pdf", doc_id=""),  # blank → resolve
    ]
    path_to_doc = {"/data/a.pdf": "doc-1"}
    to_update, stats = plan_doc_id_backfill(nodes, path_to_doc)

    assert len(to_update) == 2
    assert all(n.metadata["doc_id"] == "doc-1" for n in to_update)
    assert stats == BackfillStats(total=2, already=0, resolved=2, unresolved=0)


def test_plan_skips_nodes_that_already_have_doc_id() -> None:
    nodes = [_node("c", file_path="/data/a.pdf", doc_id="already")]
    to_update, stats = plan_doc_id_backfill(nodes, {"/data/a.pdf": "doc-x"})

    assert to_update == []
    assert nodes[0].metadata["doc_id"] == "already"  # untouched
    assert stats == BackfillStats(total=1, already=1, resolved=0, unresolved=0)


def test_row_to_node_restores_text_and_embedding() -> None:
    """Milvus stores text + embedding in SEPARATE fields (remove_text=True
    on write); reconstruction must put both back or a re-insert would wipe
    the chunk text / force a re-embed."""
    original = TextNode(
        text="the chunk body",
        metadata={"file_path": "/data/a.pdf", "position": 3},
    )
    # Simulate the Milvus row: _node_content (text stripped) + flat fields.
    meta = node_to_metadata_dict(original, remove_text=True)
    row = {
        **meta,                       # carries _node_content (no text)
        "text": "the chunk body",     # text field, stored separately
        "embedding": [0.1, 0.2, 0.3],
    }

    node = row_to_node(row)
    assert node.get_content() == "the chunk body"
    assert node.embedding == [0.1, 0.2, 0.3]
    assert node.metadata["file_path"] == "/data/a.pdf"
    assert node.metadata["position"] == 3


def test_plan_counts_unresolved_when_path_not_in_index() -> None:
    nodes = [
        _node("c1", file_path="/data/unknown.pdf"),  # no map entry
        _node("c2"),                                  # no file_path at all
    ]
    to_update, stats = plan_doc_id_backfill(nodes, {"/data/a.pdf": "doc-1"})

    assert to_update == []
    assert stats == BackfillStats(total=2, already=0, resolved=0, unresolved=2)
    # unresolved nodes are NOT given a doc_id key
    assert "doc_id" not in nodes[0].metadata

"""Snapshot/restore of bulky metadata around the Milvus write.

Milvus rejects rows whose `_node_content` dynamic field exceeds 65k
chars.  `canonical_identifiers` and `translated_text` are the two
keys that routinely push us over on identifier-dense / multilingual
documents.  They're still required by downstream steps
(`inject_canonical_entities` reads identifiers; LightRAG reads the
translation), so we strip them only for the duration of the Milvus
insert and restore immediately after.
"""

from __future__ import annotations

from llama_index.core.schema import TextNode

from src.workflow.activities.index_vector import (
    _MILVUS_DROP_KEYS,
    _restore_metadata,
    _snapshot_metadata,
)


def _node(meta: dict) -> TextNode:
    return TextNode(text="x", metadata=dict(meta))


def test_snapshot_pops_drop_keys_and_leaves_others() -> None:
    nodes = [
        _node({
            "doc_id": "d1",
            "canonical_identifiers": [{"canonical": "+74951234567"}],
            "translated_text": "русский перевод",
            "file_path": "/tmp/a.txt",
        }),
        _node({"doc_id": "d2"}),  # no drop keys
    ]

    snaps = _snapshot_metadata(nodes, _MILVUS_DROP_KEYS)

    assert "canonical_identifiers" not in nodes[0].metadata
    assert "translated_text" not in nodes[0].metadata
    assert nodes[0].metadata == {"doc_id": "d1", "file_path": "/tmp/a.txt"}
    assert nodes[1].metadata == {"doc_id": "d2"}

    assert snaps[0] == {
        "canonical_identifiers": [{"canonical": "+74951234567"}],
        "translated_text": "русский перевод",
    }
    assert snaps[1] == {}


def test_restore_round_trips_metadata() -> None:
    original = {
        "doc_id": "d1",
        "canonical_identifiers": [{"canonical": "+74951234567"}],
        "translated_text": "перевод",
    }
    nodes = [_node(original)]

    snaps = _snapshot_metadata(nodes, _MILVUS_DROP_KEYS)
    _restore_metadata(nodes, snaps)

    assert nodes[0].metadata == original


def test_restore_skips_empty_snapshots() -> None:
    """Nodes whose snapshot is `{}` shouldn't have their metadata
    mutated by `_restore_metadata` — empty restore is a no-op."""
    node = _node({"doc_id": "d1"})
    _restore_metadata([node], [{}])
    assert node.metadata == {"doc_id": "d1"}

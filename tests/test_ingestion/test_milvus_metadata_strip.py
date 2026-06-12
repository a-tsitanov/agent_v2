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
    _MILVUS_FIELD_BUDGET,
    _node_content_len,
    _restore_metadata,
    _restore_text,
    _snapshot_for_milvus,
    _snapshot_metadata,
    _snapshot_oversized_text,
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


# ── generic size guard: _node_content (metadata) overflow ────────────


def test_snapshot_for_milvus_strips_unknown_oversized_key() -> None:
    """An *unknown* bulky metadata key (not in the denylist) that would
    push `_node_content` past the Milvus VARCHAR cap is peeled off by
    the generic size pass and recorded for restore."""
    blob = "x" * 200_000
    node = _node({"doc_id": "d1", "kg_blob": blob})
    assert _node_content_len(node) > _MILVUS_FIELD_BUDGET  # precondition

    snaps = _snapshot_for_milvus([node])

    assert "kg_blob" not in node.metadata
    assert node.metadata == {"doc_id": "d1"}
    assert _node_content_len(node) <= _MILVUS_FIELD_BUDGET
    assert snaps[0]["kg_blob"] == blob

    _restore_metadata([node], snaps)
    assert node.metadata["kg_blob"] == blob


def test_snapshot_for_milvus_still_strips_known_keys() -> None:
    """The generic pass subsumes the old behaviour: known denylist keys
    are stripped+restored even when the node is under the size cap."""
    node = _node({
        "doc_id": "d1",
        "canonical_identifiers": [{"canonical": "+74951234567"}],
        "translated_text": "перевод",
    })

    snaps = _snapshot_for_milvus([node])

    assert "canonical_identifiers" not in node.metadata
    assert "translated_text" not in node.metadata
    assert node.metadata == {"doc_id": "d1"}

    _restore_metadata([node], snaps)
    assert node.metadata["canonical_identifiers"] == [
        {"canonical": "+74951234567"},
    ]
    assert node.metadata["translated_text"] == "перевод"


def test_snapshot_for_milvus_leaves_small_nodes_untouched() -> None:
    node = _node({"doc_id": "d1", "file_path": "/tmp/a.txt"})
    snaps = _snapshot_for_milvus([node])
    assert snaps[0] == {}
    assert node.metadata == {"doc_id": "d1", "file_path": "/tmp/a.txt"}


# ── separate VARCHAR field: chunk `text` overflow ────────────────────


def test_oversized_text_truncated_for_insert_then_restored() -> None:
    """A single chunk whose `text` exceeds the cap (a chunking
    pathology) is truncated for the Milvus write and restored after,
    so downstream stores still see the full content."""
    full = "y" * 200_000
    node = _node({"doc_id": "d1"})
    node.set_content(full)

    tsnaps = _snapshot_oversized_text([node])

    assert len(node.text) <= _MILVUS_FIELD_BUDGET
    assert tsnaps[0] == full

    _restore_text([node], tsnaps)
    assert node.text == full


def test_normal_text_not_snapshotted() -> None:
    node = _node({"doc_id": "d1"})
    node.set_content("short chunk")
    tsnaps = _snapshot_oversized_text([node])
    assert tsnaps[0] is None
    assert node.text == "short chunk"

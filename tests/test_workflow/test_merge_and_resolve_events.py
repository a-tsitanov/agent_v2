"""Tests: event-dedup branch in merge_and_resolve (Task 5).

When ``settings.events.extraction_enabled`` is True, EventOrAction nodes are
routed through ``merge_events`` before ER.  Two nodes sharing the same
event_key (event_type + participants + ts_bucket) must collapse to one.

When ``extraction_enabled`` is False the branch is a no-op: the entity list
is byte-identical to what phone consolidation produced.

Mirrors the harness in test_merge_and_resolve.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from llama_index.core.graph_stores.types import EntityNode, Relation

from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.contracts import Ctx, KGExtracted, Parsed

# ── shared helpers ────────────────────────────────────────────────────


def _ctx() -> KGExtracted:
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    return KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")


def _make_event(
    name: str,
    *,
    event_type: str = "meeting",
    participants: list[str] | None = None,
    event_ts: str | None = "2024-01-15",
) -> EntityNode:
    return EntityNode(
        name=name,
        label="EventOrAction",
        properties={
            "event_type": event_type,
            "participants": participants or ["Alice", "Bob"],
            "event_ts": event_ts,
        },
    )


def _make_entity(name: str, label: str = "Person") -> EntityNode:
    return EntityNode(name=name, label=label, properties={})


def _make_relation(source_id: str, target_id: str) -> Relation:
    return Relation(label="RELATES_TO", source_id=source_id, target_id=target_id, properties={})


# ── base patch context (mirrors test_merge_and_resolve.py) ────────────


def _base_patches(nodes, merged_entities, merged_relations, fake_settings):
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"
    return [
        patch(
            "src.workflow.activities.merge_and_resolve.build_staging_store", return_value=staging
        ),
        patch(
            "src.workflow.activities.merge_and_resolve.get_llm_pool",
            return_value=MagicMock(**{"get.return_value": MagicMock()}),
        ),
        patch(
            "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
            new=AsyncMock(return_value=(merged_entities, merged_relations)),
        ),
        patch(
            "src.workflow.activities.merge_and_resolve.consolidate_phone_entities",
            return_value=(merged_entities, merged_relations, {}),
        ),
        patch("src.workflow.activities.merge_and_resolve.settings", fake_settings),
        patch("src.workflow.activities.merge_and_resolve.activity"),
    ]


# ── tests ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_event_dedup_enabled_collapses_duplicate_events():
    """Two EventOrAction nodes with the same event_key → one survives."""
    nodes = [MagicMock(node_id="a")]
    ev1 = _make_event("EventMeeting_chunk1")
    ev2 = _make_event("EventMeeting_chunk2")  # same key: type+participants+ts_bucket
    regular = _make_entity("Alice")

    merged_entities = [ev1, ev2, regular]
    merged_relations = [
        _make_relation("EventMeeting_chunk1", "Alice"),
        _make_relation("EventMeeting_chunk2", "Alice"),
    ]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        out = await merge_and_resolve(_ctx())

    # Recover the written tuple from staging
    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    event_out = [e for e in out_entities if e.label == "EventOrAction"]
    # The two same-key events must collapse to ONE canonical node
    assert len(event_out) == 1, f"Expected 1 event node, got {len(event_out)}"
    # Regular entity must survive unchanged
    assert any(e.name == "Alice" for e in out_entities)
    # Entity names on the Merged contract reflect post-dedup state
    assert len([n for n in out.entity_names if n.startswith("EventMeeting")]) == 1


@pytest.mark.asyncio
async def test_event_dedup_disabled_is_noop():
    """When extraction_enabled=False the entity list is unchanged by event branch."""
    nodes = [MagicMock(node_id="a")]
    ev1 = _make_event("EventMeeting_chunk1")
    ev2 = _make_event("EventMeeting_chunk2")
    regular = _make_entity("Alice")

    merged_entities = [ev1, ev2, regular]
    merged_relations = [_make_relation("EventMeeting_chunk1", "Alice")]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = False

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        out = await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    # Both event nodes must be present (no dedup applied)
    event_out = [e for e in out_entities if e.label == "EventOrAction"]
    assert len(event_out) == 2, f"Expected 2 event nodes (no dedup), got {len(event_out)}"
    assert out.merged_entity_count == 3


@pytest.mark.asyncio
async def test_event_dedup_does_not_affect_non_event_entities():
    """Event-dedup branch must leave non-EventOrAction entities untouched."""
    nodes = [MagicMock(node_id="a")]
    ev = _make_event("EV1")
    p1 = _make_entity("Person1")
    p2 = _make_entity("Person2")

    merged_entities = [ev, p1, p2]
    merged_relations = [_make_relation("EV1", "Person1"), _make_relation("Person1", "Person2")]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    non_event = [e for e in out_entities if e.label != "EventOrAction"]
    assert len(non_event) == 2
    assert {e.name for e in non_event} == {"Person1", "Person2"}


@pytest.mark.asyncio
async def test_event_relations_rewritten_after_dedup():
    """Argument relations pointing at the non-canonical duplicate must be rewritten."""
    nodes = [MagicMock(node_id="a")]
    # chunk1 is canonical (first), chunk2 gets merged into it
    ev1 = _make_event("EV_chunk1")
    ev2 = _make_event("EV_chunk2")  # same key

    merged_entities = [ev1, ev2]
    # Relation that points at the non-canonical node
    rel_to_dup = _make_relation("EV_chunk2", "Participant")
    merged_relations = [rel_to_dup]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    _out_entities, out_relations, _ = written_tuple

    # The relation source must be rewritten to the canonical EV_chunk1
    assert all(r.source_id != "EV_chunk2" for r in out_relations), (
        "Non-canonical event name must be rewritten to canonical"
    )

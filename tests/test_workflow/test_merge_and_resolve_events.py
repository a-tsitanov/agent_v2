"""Tests: event-dedup branch in merge_and_resolve (Task 5).

When ``settings.events.extraction_enabled`` is True, EventOrAction nodes are
routed through ``merge_events`` before ER.  Two nodes sharing the same
event_key (event_type + participants + ts_bucket) must collapse to one.

When ``extraction_enabled`` is False the branch is a no-op: the entity list
is byte-identical to what phone consolidation produced.

Mirrors the harness in test_merge_and_resolve.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
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


def _epoch(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, tzinfo=UTC).timestamp())


# Default epoch used by _make_event: 2024-01-15 (ISO week 2024-W03). merge_events
# buckets by ISO year-week (bucket_days=7 default), so two default-epoch fixtures
# fall in the same bucket and are expected to dedup.
_DEFAULT_EPOCH = _epoch(2024, 1, 15)


def _make_event(
    name: str,
    *,
    event_type: str = "meeting",
    participants: list[str] | None = None,
    event_ts_raw: str | None = "2024-01-15",
    event_start_epoch: int | None = _DEFAULT_EPOCH,
    event_end_epoch: int | None = None,
    event_ts_precision: str | None = "day",
    trigger: str = "провели встречу",
) -> EntityNode:
    """A pipeline-produced event node -- always carries ``trigger`` (only
    ``events_to_graph`` writes it), which is what gates it into the
    event-dedup flow instead of the regular entity flow (Fix A)."""
    return EntityNode(
        name=name,
        label="EventOrAction",
        properties={
            "event_type": event_type,
            "trigger": trigger,
            "participants": participants or ["Alice", "Bob"],
            "event_ts_raw": event_ts_raw,
            "event_start_epoch": event_start_epoch,
            "event_end_epoch": event_end_epoch,
            "event_ts_precision": event_ts_precision,
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
    """Two EventOrAction nodes with the same event_key → one survives.

    Both use the default epoch (2024-01-15, ISO week 2024-W03), so they share
    the same key: type + participants + ts_bucket.
    """
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
async def test_er_stage_wrapped_in_heartbeat_every():
    """ER can run far past the 15-min heartbeat_timeout on a dense-entity doc
    (thousands of judge-pairs). It MUST pulse heartbeats during that call, else
    Temporal cancels + retries it forever (the 0b938ba5 wedge). Assert the
    resolve_entities call runs inside heartbeat_every(..., stage='resolving')."""
    hb_calls: list = []

    class _FakeHB:  # records the heartbeat_every(interval, detail) invocations
        def __init__(self, interval, detail=None):
            hb_calls.append(detail)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    nodes = [MagicMock(node_id="a")]
    regular = _make_entity("Alice")
    merged_entities = [regular]
    merged_relations = []

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = True
    fake_settings.events.extraction_enabled = False

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch(
        "src.workflow.activities.merge_and_resolve.heartbeat_every", _FakeHB
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_graph_store",
        return_value=MagicMock(),
    ), patch(
        "src.graph.entity_vector_store.build_entity_vector_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.resolve_entities",
        new=AsyncMock(return_value=(merged_entities, merged_relations, {})),
    ):
        await merge_and_resolve(_ctx())

    assert {"stage": "resolving"} in hb_calls, (
        f"ER not wrapped in heartbeat_every; pulses seen: {hb_calls}"
    )


@pytest.mark.asyncio
async def test_cross_channel_dedup_folds_entity_event_into_pipeline_event():
    """п.4: an entity-channel EventOrAction (no `trigger`) that paraphrases a
    same-chunk pipeline event folds into it; its relation is repointed."""
    nodes = [MagicMock(node_id="a")]
    pipe = _make_event("уничтожены три диверсанта")  # pipeline event (has trigger)
    pipe.properties["source_chunks"] = ["c1"]
    ent_ev = EntityNode(
        name="Уничтожение диверсантов",
        label="EventOrAction",
        properties={"source_chunk_id": "c1", "description": "d"},  # no `trigger`
    )
    regular = _make_entity("Alice")

    merged_entities = [pipe, ent_ev, regular]
    merged_relations = [_make_relation("Уничтожение диверсантов", "Alice")]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True
    fake_settings.events.cross_channel_dedup_enabled = True
    fake_settings.events.cross_channel_dedup_threshold = 0.88

    embed = MagicMock()

    async def _batch(names):  # near-identical vectors → cosine ~1 → fold
        return [[1.0, 0.02] for _ in names]

    embed.aget_text_embedding_batch = _batch

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch(
        "src.workflow.activities.merge_and_resolve.build_embedding_model",
        return_value=embed,
    ):
        await merge_and_resolve(_ctx())

    written = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, out_relations, _ = written
    names = {e.name for e in out_entities}
    assert "Уничтожение диверсантов" not in names  # entity-channel dup folded away
    assert "уничтожены три диверсанта" in names  # richer event node survives
    assert any(
        r.source_id == "уничтожены три диверсанта" and r.target_id == "Alice"
        for r in out_relations
    )  # relation repointed to the survivor


@pytest.mark.asyncio
async def test_cross_channel_dedup_keeps_low_similarity_entity_event():
    """Recall guard: an entity-channel event with NO similar same-chunk pipeline
    event survives (the event channel missed it)."""
    nodes = [MagicMock(node_id="a")]
    pipe = _make_event("состоялась встреча")
    pipe.properties["source_chunks"] = ["c1"]
    ent_ev = EntityNode(
        name="Новые санкции",
        label="EventOrAction",
        properties={"source_chunk_id": "c1", "description": "d"},
    )
    merged_entities = [pipe, ent_ev]
    merged_relations = []

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True
    fake_settings.events.cross_channel_dedup_enabled = True
    fake_settings.events.cross_channel_dedup_threshold = 0.88

    embed = MagicMock()

    async def _batch(names):  # orthogonal vectors per name → cosine ~0 → keep
        return [[1.0, 0.0] if "санкции" in n else [0.0, 1.0] for n in names]

    embed.aget_text_embedding_batch = _batch

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patch(
        "src.workflow.activities.merge_and_resolve.build_embedding_model",
        return_value=embed,
    ):
        await merge_and_resolve(_ctx())

    written = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written
    names = {e.name for e in out_entities}
    assert "Новые санкции" in names  # kept — recall preserved
    assert "состоялась встреча" in names


@pytest.mark.asyncio
async def test_event_dedup_different_ts_bucket_does_not_collapse():
    """Same type+participants but different ISO-week ts_bucket → both survive.

    This exercises the dated-dedup path itself (distinct ts_bucket keys),
    as opposed to the untimed ``∅`` bucket.
    """
    nodes = [MagicMock(node_id="a")]
    ev1 = _make_event(
        "EventMeeting_jan",
        event_ts_raw="2024-01-15",
        event_start_epoch=_epoch(2024, 1, 15),
    )
    ev2 = _make_event(
        "EventMeeting_jun",
        event_ts_raw="2024-06-15",
        event_start_epoch=_epoch(2024, 6, 15),
    )
    regular = _make_entity("Alice")

    merged_entities = [ev1, ev2, regular]
    merged_relations = [
        _make_relation("EventMeeting_jan", "Alice"),
        _make_relation("EventMeeting_jun", "Alice"),
    ]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        out = await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    event_out = [e for e in out_entities if e.label == "EventOrAction"]
    # Different ts_bucket ⇒ distinct event_key ⇒ no collapse
    assert len(event_out) == 2, f"Expected 2 event nodes (different ts-bucket), got {len(event_out)}"
    assert len([n for n in out.entity_names if n.startswith("EventMeeting")]) == 2


@pytest.mark.asyncio
async def test_event_dedup_untimed_events_still_collapse():
    """Events with no resolvable timestamp share the untimed ``∅`` bucket.

    Preserves coverage of the untimed path: a re-reported event with no
    extractable date must still dedup on type+participants alone.
    """
    nodes = [MagicMock(node_id="a")]
    ev1 = _make_event(
        "EventMeeting_untimed1",
        event_ts_raw=None,
        event_start_epoch=None,
        event_end_epoch=None,
        event_ts_precision=None,
    )
    ev2 = _make_event(
        "EventMeeting_untimed2",
        event_ts_raw=None,
        event_start_epoch=None,
        event_end_epoch=None,
        event_ts_precision=None,
    )
    regular = _make_entity("Alice")

    merged_entities = [ev1, ev2, regular]
    merged_relations = [
        _make_relation("EventMeeting_untimed1", "Alice"),
        _make_relation("EventMeeting_untimed2", "Alice"),
    ]

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    event_out = [e for e in out_entities if e.label == "EventOrAction"]
    # Untimed ⇒ shared ∅ bucket ⇒ still collapses to ONE canonical node
    assert len(event_out) == 1, f"Expected 1 event node (untimed ∅ bucket), got {len(event_out)}"


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


# ── Fix A: pipeline-signature gate ─────────────────────────────────────


@pytest.mark.asyncio
async def test_entity_kind_event_or_action_bypasses_event_pipeline():
    """EventOrAction nodes with NO event-pipeline signature (no ``trigger``)
    must stay in the regular entity flow -- untouched by ``merge_events``.

    Before Fix A, the EventOrAction/label-only split routed these into
    merge_events, where they all share the untimed dedup key
    ``("event", frozenset(), "∅")`` and mass-merge, and the type-vote
    default stamps a spurious ``event_type='event'`` property onto them.
    """
    nodes = [MagicMock(node_id="a")]
    # Entity-extractor nodes: label EventOrAction, but no pipeline props
    # (no `trigger`, no `participants`, no `event_start_epoch`).
    ent1 = EntityNode(name="Meeting Alpha", label="EventOrAction", properties={"description": "x"})
    ent2 = EntityNode(name="Meeting Beta", label="EventOrAction", properties={"description": "y"})

    merged_entities = [ent1, ent2]
    merged_relations = []

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False
    fake_settings.events.extraction_enabled = True

    patches = _base_patches(nodes, merged_entities, merged_relations, fake_settings)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        await merge_and_resolve(_ctx())

    written_tuple = patches[0].kwargs["return_value"].write_pickle.call_args.args[2]
    out_entities, _out_relations, _ = written_tuple

    event_out = [e for e in out_entities if e.label == "EventOrAction"]
    # Must NOT collapse -- each stays a distinct node, exactly like any
    # other entity that happens to share a dedup key by coincidence.
    assert len(event_out) == 2, f"Expected 2 distinct entity-kind nodes, got {len(event_out)}"
    assert {e.name for e in event_out} == {"Meeting Alpha", "Meeting Beta"}
    # Must NOT gain the merge_events type-vote stamp.
    for e in event_out:
        assert "event_type" not in (e.properties or {}), (
            f"{e.name} must not be stamped with event_type by merge_events"
        )

from datetime import UTC, datetime

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.event_merge import (
    _ts_bucket,
    dedup_cross_channel_events,
    event_key,
    merge_events,
)

# ── cross-channel event dedup (п.4) ─────────────────────────────────


def _ev_entity(name, chunk, desc="d"):
    """Entity-channel EventOrAction: has source_chunk_id, no `trigger`."""
    return EntityNode(name=name, label="EventOrAction",
                      properties={"source_chunk_id": chunk, "description": desc})


def _ev_pipeline(name, chunks, etype="incident"):
    """Event-channel EventOrAction: carries `trigger` + structured props."""
    return EntityNode(name=name, label="EventOrAction",
                      properties={"source_chunks": chunks, "trigger": name,
                                  "event_type": etype, "participants": []})


def test_cross_channel_merges_same_chunk_high_cosine():
    ent = _ev_entity("Уничтожение диверсантов", "c1")
    pipe = _ev_pipeline("уничтожены три диверсанта", ["c1"])
    emb = {"Уничтожение диверсантов": [1.0, 0.0],
           "уничтожены три диверсанта": [0.99, 0.14]}
    kept, alias = dedup_cross_channel_events([ent], [pipe], emb, threshold=0.86)
    assert kept == []  # entity-channel duplicate folded into the richer event node
    assert alias == {"Уничтожение диверсантов": "уничтожены три диверсанта"}


def test_cross_channel_keeps_low_cosine_recall_preserving():
    # A distinct action the event channel didn't capture must survive.
    ent = _ev_entity("Новые меры ЦБ", "c1")
    pipe = _ev_pipeline("состоялась встреча", ["c1"])
    emb = {"Новые меры ЦБ": [1.0, 0.0], "состоялась встреча": [0.0, 1.0]}
    kept, alias = dedup_cross_channel_events([ent], [pipe], emb, threshold=0.86)
    assert [e.name for e in kept] == ["Новые меры ЦБ"]
    assert alias == {}


def test_cross_channel_never_merges_across_chunks():
    # Same wording in a DIFFERENT chunk is a different context — never merge.
    ent = _ev_entity("Уничтожение диверсантов", "cA")
    pipe = _ev_pipeline("уничтожены три диверсанта", ["cB"])
    emb = {"Уничтожение диверсантов": [1.0, 0.0],
           "уничтожены три диверсанта": [1.0, 0.0]}
    kept, alias = dedup_cross_channel_events([ent], [pipe], emb, threshold=0.86)
    assert [e.name for e in kept] == ["Уничтожение диверсантов"]  # kept despite cos=1
    assert alias == {}


def test_cross_channel_picks_highest_cosine_candidate():
    ent = _ev_entity("Встреча глав Роскосмоса и NASA", "c1")
    p1 = _ev_pipeline("состоялась встреча", ["c1"])
    p2 = _ev_pipeline("подписан контракт", ["c1"])
    emb = {"Встреча глав Роскосмоса и NASA": [1.0, 0.0, 0.0],
           "состоялась встреча": [0.95, 0.31, 0.0],
           "подписан контракт": [0.0, 0.0, 1.0]}
    kept, alias = dedup_cross_channel_events([ent], [p1, p2], emb, threshold=0.86)
    assert alias == {"Встреча глав Роскосмоса и NASA": "состоялась встреча"}
    assert kept == []


def _epoch(y, m, d):
    return int(datetime(y, m, d, tzinfo=UTC).timestamp())


def test_ts_bucket_same_iso_week_collides():
    assert _ts_bucket(_epoch(2026, 7, 1), 7) == _ts_bucket(_epoch(2026, 7, 3), 7)
    assert _ts_bucket(_epoch(2026, 7, 1), 7) != _ts_bucket(_epoch(2026, 7, 8), 7)


def test_ts_bucket_none_is_sentinel():
    assert _ts_bucket(None, 7) == "∅"


def test_event_key_buckets_on_epoch():
    k1 = event_key("deal", ["Иванов"], _epoch(2026, 7, 1))
    k2 = event_key("deal", ["Иванов"], _epoch(2026, 7, 3))
    assert k1 == k2


def test_merge_events_keeps_earliest_interval():
    def node(start, end, ts_raw, precision):
        return EntityNode(name=f"deal: подписали {start}", label="EventOrAction", properties={
            "event_type": "deal", "participants": ["Иванов"],
            "event_ts_raw": ts_raw, "event_start_epoch": start, "event_end_epoch": end,
            "event_ts_precision": precision, "source_chunks": [f"c{start}"],
        })

    early, late = _epoch(2026, 7, 1), _epoch(2026, 7, 2)
    merged, _ = merge_events(
        [node(late, late + 86399, "raw-late", "datetime"),
         node(early, early + 86399, "raw-early", "day")],
        []
    )
    assert len(merged) == 1
    # Verify all timestamp fields are taken from earliest member
    assert merged[0].properties["event_start_epoch"] == early
    assert merged[0].properties["event_end_epoch"] == early + 86399
    assert merged[0].properties["event_ts_raw"] == "raw-early"
    assert merged[0].properties["event_ts_precision"] == "day"


def test_event_key_is_participant_order_insensitive_and_epoch_bucketed():
    k1 = event_key("Deal", ["Romashka", "Lutik"], _epoch(2024, 3, 1))
    k2 = event_key("deal", ["Lutik", "Romashka"], _epoch(2024, 3, 3))  # reordered + within bucket
    assert k1 == k2  # same event
    k3 = event_key("deal", ["Romashka", "Lutik"], _epoch(2024, 9, 1))  # far ts → different
    assert k1 != k3


def _ev(name, src_chunk, start_epoch):
    return EntityNode(
        name=name,
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "trigger": "signed",
            "event_ts_raw": "x",
            "event_start_epoch": start_epoch,
            "event_end_epoch": start_epoch + 86399,
            "event_ts_precision": "day",
            "participants": ["Romashka", "Lutik"],
            "source_chunks": [src_chunk],
        },
    )


def test_merge_collapses_same_event_from_two_docs():
    # same event re-reported in a second document → ONE node, source_chunks merged
    epoch = _epoch(2024, 3, 1)
    nodes, _ = merge_events([_ev("deal: signed", "c1", epoch), _ev("deal: signed", "c2", epoch)], [])
    assert len(nodes) == 1
    assert set(nodes[0].properties["source_chunks"]) == {"c1", "c2"}


def test_different_events_not_merged():
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts_raw": "x",
            "event_start_epoch": _epoch(2024, 3, 1),
            "event_end_epoch": _epoch(2024, 3, 1) + 86399,
            "event_ts_precision": "day",
            "participants": ["Alpha", "Beta"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts_raw": "x",
            "event_start_epoch": _epoch(2024, 9, 1),  # different bucket
            "event_end_epoch": _epoch(2024, 9, 1) + 86399,
            "event_ts_precision": "day",
            "participants": ["Alpha", "Beta"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 2


def test_argument_edges_rewritten_to_canonical():
    ev1 = EntityNode(
        name="ev-a",
        label="EventOrAction",
        properties={
            "event_type": "meeting",
            "event_ts_raw": "x",
            "event_start_epoch": _epoch(2024, 1, 8),  # ISO week 2
            "event_end_epoch": _epoch(2024, 1, 8) + 86399,
            "event_ts_precision": "day",
            "participants": ["Corp", "Bank"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev-b",
        label="EventOrAction",
        properties={
            "event_type": "meeting",
            "event_ts_raw": "x",
            "event_start_epoch": _epoch(2024, 1, 10),  # ISO week 2 (same)
            "event_end_epoch": _epoch(2024, 1, 10) + 86399,
            "event_ts_precision": "day",
            "participants": ["Bank", "Corp"],
            "source_chunks": ["c2"],
        },
    )
    arg_rel = Relation(label="PARTICIPANT", source_id="ev-b", target_id="some-entity")
    nodes, rels = merge_events([ev1, ev2], [arg_rel])
    assert len(nodes) == 1
    canonical = nodes[0].name
    assert len(rels) == 1
    assert rels[0].source_id == canonical


def test_earliest_event_epoch_selected():
    early = _epoch(2024, 3, 4)  # ISO week 10 (Monday) — earlier
    late = _epoch(2024, 3, 7)   # ISO week 10 (Thursday)
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts_raw": "raw-late",
            "event_start_epoch": late,
            "event_end_epoch": late + 86399,
            "event_ts_precision": "datetime",
            "participants": ["X", "Y"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts_raw": "raw-early",
            "event_start_epoch": early,
            "event_end_epoch": early + 86399,
            "event_ts_precision": "day",
            "participants": ["Y", "X"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 1
    # Verify all timestamp fields are taken from earliest member (ev2)
    assert nodes[0].properties["event_start_epoch"] == early
    assert nodes[0].properties["event_end_epoch"] == early + 86399
    assert nodes[0].properties["event_ts_raw"] == "raw-early"
    assert nodes[0].properties["event_ts_precision"] == "day"


def test_no_ts_sentinel_groups_correctly():
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "acquisition",
            "event_ts_raw": "x",
            "event_start_epoch": None,
            "event_end_epoch": None,
            "event_ts_precision": None,
            "participants": ["Firm"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "acquisition",
            "event_ts_raw": "x",
            "event_start_epoch": None,
            "event_end_epoch": None,
            "event_ts_precision": None,
            "participants": ["Firm"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 1
    assert set(nodes[0].properties["source_chunks"]) == {"c1", "c2"}

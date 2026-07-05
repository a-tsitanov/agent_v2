from datetime import UTC, datetime

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.event_merge import _ts_bucket, event_key, merge_events


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
    def node(start, end):
        return EntityNode(name=f"deal: подписали {start}", label="EventOrAction", properties={
            "event_type": "deal", "participants": ["Иванов"],
            "event_ts_raw": "x", "event_start_epoch": start, "event_end_epoch": end,
            "event_ts_precision": "day", "source_chunks": [f"c{start}"],
        })

    early, late = _epoch(2026, 7, 1), _epoch(2026, 7, 2)
    merged, _ = merge_events([node(late, late + 86399), node(early, early + 86399)], [])
    assert len(merged) == 1
    assert merged[0].properties["event_start_epoch"] == early


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
            "event_ts_raw": "x",
            "event_start_epoch": late,
            "event_end_epoch": late + 86399,
            "event_ts_precision": "day",
            "participants": ["X", "Y"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts_raw": "x",
            "event_start_epoch": early,
            "event_end_epoch": early + 86399,
            "event_ts_precision": "day",
            "participants": ["Y", "X"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 1
    assert nodes[0].properties["event_start_epoch"] == early


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

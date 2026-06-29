from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.event_merge import event_key, merge_events


def test_event_key_is_participant_order_insensitive_and_ts_bucketed():
    k1 = event_key("Deal", ["Romashka", "Lutik"], "2024-03-01")
    k2 = event_key("deal", ["Lutik", "Romashka"], "2024-03-03")  # reordered + within bucket
    assert k1 == k2  # same event
    k3 = event_key("deal", ["Romashka", "Lutik"], "2024-09-01")  # far ts → different
    assert k1 != k3


def _ev(name, src_chunk):
    return EntityNode(
        name=name,
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "trigger": "signed",
            "event_ts": "2024-03-01",
            "participants": ["Romashka", "Lutik"],
            "source_chunks": [src_chunk],
        },
    )


def test_merge_collapses_same_event_from_two_docs():
    # same event re-reported in a second document → ONE node, source_chunks merged
    nodes, _ = merge_events([_ev("deal: signed", "c1"), _ev("deal: signed", "c2")], [])
    assert len(nodes) == 1
    assert set(nodes[0].properties["source_chunks"]) == {"c1", "c2"}


def test_different_events_not_merged():
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts": "2024-03-01",
            "participants": ["Alpha", "Beta"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts": "2024-09-01",  # different bucket
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
            "event_ts": "2024-01-08",  # ISO week 2
            "participants": ["Corp", "Bank"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev-b",
        label="EventOrAction",
        properties={
            "event_type": "meeting",
            "event_ts": "2024-01-10",  # ISO week 2 (same)
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


def test_earliest_event_ts_selected():
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts": "2024-03-07",  # ISO week 10 (Thursday)
            "participants": ["X", "Y"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "deal",
            "event_ts": "2024-03-04",  # ISO week 10 (Monday) — earlier
            "participants": ["Y", "X"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 1
    assert nodes[0].properties["event_ts"] == "2024-03-04"


def test_no_ts_sentinel_groups_correctly():
    ev1 = EntityNode(
        name="ev1",
        label="EventOrAction",
        properties={
            "event_type": "acquisition",
            "event_ts": None,
            "participants": ["Firm"],
            "source_chunks": ["c1"],
        },
    )
    ev2 = EntityNode(
        name="ev2",
        label="EventOrAction",
        properties={
            "event_type": "acquisition",
            "event_ts": None,
            "participants": ["Firm"],
            "source_chunks": ["c2"],
        },
    )
    nodes, _ = merge_events([ev1, ev2], [])
    assert len(nodes) == 1
    assert set(nodes[0].properties["source_chunks"]) == {"c1", "c2"}

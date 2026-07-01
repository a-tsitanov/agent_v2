"""Tests for E1 — ON-CREATE-emulated first_seen stamping."""

from src.graph.first_seen import stamp_first_seen


class _Rec:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        return []


def test_stamp_only_sets_where_created_at_is_null():
    store = _Rec()
    stamp_first_seen(
        store,
        entity_names=["A", "B"],
        relations=[("A", "OWNS", "B")],
        ingest_epoch=19797,
        doc_id="d1",
    )
    joined = " ".join(c[0] for c in store.calls)
    assert "created_at IS NULL" in joined  # ON CREATE semantics
    assert "SET e.created_at" in joined or "SET r.created_at" in joined
    # entity names + ts + doc passed
    ent_call = next(c for c in store.calls if "e.created_at" in c[0])
    assert ent_call[1]["names"] == ["A", "B"]
    assert ent_call[1]["ts"] == 19797 and ent_call[1]["doc_id"] == "d1"


def test_stamp_noop_on_empty():
    store = _Rec()
    stamp_first_seen(store, entity_names=[], relations=[], ingest_epoch=1, doc_id="d")
    assert store.calls == []


def test_stamp_relations_packed_as_src_label_tgt_dicts():
    store = _Rec()
    stamp_first_seen(
        store,
        entity_names=["X", "Y"],
        relations=[("X", "KNOWS", "Y"), ("Y", "LIKES", "X")],
        ingest_epoch=100,
        doc_id="doc2",
    )
    rel_call = next((c for c in store.calls if "r.created_at" in c[0]), None)
    assert rel_call is not None, "expected a relation stamp call"
    rels = rel_call[1]["rels"]
    assert len(rels) == 2
    assert rels[0] == {"src": "X", "label": "KNOWS", "tgt": "Y"}
    assert rel_call[1]["ts"] == 100
    assert rel_call[1]["doc_id"] == "doc2"


def test_stamp_entities_only_no_relations():
    """When relations list is empty, only the entity pass fires."""
    store = _Rec()
    stamp_first_seen(store, entity_names=["Z"], relations=[], ingest_epoch=5, doc_id="d3")
    assert len(store.calls) == 1
    assert "e.created_at" in store.calls[0][0]


def test_stamp_fail_soft_on_store_error():
    """A broken store must not propagate exceptions."""

    class _Bad:
        def structured_query(self, cypher, param_map=None):
            raise RuntimeError("boom")

    # must not raise
    stamp_first_seen(
        _Bad(),
        entity_names=["A"],
        relations=[("A", "X", "B")],
        ingest_epoch=1,
        doc_id="d",
    )

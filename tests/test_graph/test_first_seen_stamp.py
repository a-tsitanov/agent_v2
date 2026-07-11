"""Tests for E1 — ON-CREATE-emulated first_seen stamping.

Covers the backend dispatch added for the nebula-first-seen slice:
neo4j keeps the original Cypher (byte-for-byte guard, monkeypatched
explicitly so these tests don't silently depend on the settings default),
nebula issues per-entity ``UPDATE VERTEX ... WHEN created_at == 0`` and
no-ops relations.
"""

from src.graph import first_seen as first_seen_mod
from src.graph.first_seen import stamp_first_seen
from src.graph.nebula_store import entity_vid


class _Rec:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        return []


def test_stamp_only_sets_where_created_at_is_null(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "neo4j", raising=False)
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


def test_stamp_noop_on_empty(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "neo4j", raising=False)
    store = _Rec()
    stamp_first_seen(store, entity_names=[], relations=[], ingest_epoch=1, doc_id="d")
    assert store.calls == []


def test_stamp_relations_packed_as_src_label_tgt_dicts(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "neo4j", raising=False)
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


def test_stamp_entities_only_no_relations(monkeypatch):
    """When relations list is empty, only the entity pass fires."""
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "neo4j", raising=False)
    store = _Rec()
    stamp_first_seen(store, entity_names=["Z"], relations=[], ingest_epoch=5, doc_id="d3")
    assert len(store.calls) == 1
    assert "e.created_at" in store.calls[0][0]


def test_stamp_fail_soft_on_store_error(monkeypatch):
    """A broken store must not propagate exceptions."""
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "neo4j", raising=False)

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


# ── nebula backend dispatch ──────────────────────────────────────────────────


def test_stamp_nebula_issues_update_vertex_when_created_at_zero(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "nebula", raising=False)
    store = _Rec()
    stamp_first_seen(
        store,
        entity_names=["A", "B"],
        relations=[("x", "L", "y")],
        ingest_epoch=42,
        doc_id="doc9",
    )

    update_calls = [c for c in store.calls if "UPDATE VERTEX ON `Entity`" in c[0]]
    assert len(update_calls) == 2

    vid_a, vid_b = entity_vid("A"), entity_vid("B")
    stmt_by_vid = {vid_a: None, vid_b: None}
    for cypher, _ in update_calls:
        for vid in stmt_by_vid:
            if f'"{vid}"' in cypher:
                stmt_by_vid[vid] = cypher

    for vid, cypher in stmt_by_vid.items():
        assert cypher is not None, f"missing UPDATE VERTEX for vid {vid}"
        assert 'SET created_at = 42, first_doc_id = "doc9" WHEN created_at == 0' in cypher

    # rel no-op: no UPDATE EDGE / rel statement issued
    assert not any("UPDATE EDGE" in c[0] for c in store.calls)
    assert not any("RELATED" in c[0] for c in store.calls)

    # nebula path uses inline nGQL, not param_map
    assert all(param_map == {} for _, param_map in store.calls)


def test_stamp_nebula_relations_only_is_noop(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "nebula", raising=False)
    store = _Rec()
    stamp_first_seen(
        store,
        entity_names=[],
        relations=[("x", "L", "y")],
        ingest_epoch=42,
        doc_id="doc9",
    )
    assert store.calls == []


def test_stamp_nebula_fail_soft_on_store_error(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "nebula", raising=False)

    class _Bad:
        def structured_query(self, cypher, param_map=None):
            raise RuntimeError("boom")

    # must not raise
    stamp_first_seen(
        _Bad(),
        entity_names=["A"],
        relations=[],
        ingest_epoch=1,
        doc_id="d",
    )


def test_stamp_noop_on_empty_nebula(monkeypatch):
    monkeypatch.setattr(first_seen_mod.settings.graph, "backend", "nebula", raising=False)
    store = _Rec()
    stamp_first_seen(store, entity_names=[], relations=[], ingest_epoch=1, doc_id="d")
    assert store.calls == []

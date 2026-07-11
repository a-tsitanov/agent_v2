from __future__ import annotations

from src.graph import er_graph_ops as ego


class _RecStore:
    """Records (cypher, param_map) calls; returns canned rows per call,
    popped in call order."""

    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


# --- verdict_vid -----------------------------------------------------


def test_verdict_vid_is_stable_32_hex():
    v = ego.verdict_vid("some-key")
    assert v == ego.verdict_vid("some-key")
    assert len(v) == 32
    assert all(c in "0123456789abcdef" for c in v)


def test_verdict_vid_distinct_per_key():
    assert ego.verdict_vid("key-a") != ego.verdict_vid("key-b")


# --- Neo4j: byte-for-byte guard ---------------------------------------


def test_neo4j_load_verdicts_issues_read_cypher_and_maps_rows():
    rows = [[{"key": "k1", "same": True}, {"key": "k2", "same": False}]]
    store = _RecStore(rows=rows)
    ops = ego.Neo4jERGraphOps(store)

    result = ops.load_verdicts(["k1", "k2"])

    assert result == {"k1": True, "k2": False}
    assert store.calls == [
        (ego._LOAD_VERDICTS_CYPHER, {"keys": ["k1", "k2"]}),
    ]
    assert ego._LOAD_VERDICTS_CYPHER == (
        "MATCH (v:ERVerdict) WHERE v.key IN $keys "
        "RETURN v.key AS key, v.same AS same"
    )


def test_neo4j_store_verdicts_issues_constraint_then_unwind_merge():
    store = _RecStore()
    ops = ego.Neo4jERGraphOps(store)

    ops.store_verdicts({"k": True})

    assert store.calls == [
        (ego._ENSURE_VERDICT_CONSTRAINT_CYPHER, None),
        (ego._STORE_VERDICTS_CYPHER, {"rows": [{"key": "k", "same": True}]}),
    ]
    assert ego._ENSURE_VERDICT_CONSTRAINT_CYPHER == (
        "CREATE CONSTRAINT er_verdict_key IF NOT EXISTS "
        "FOR (v:ERVerdict) REQUIRE v.key IS UNIQUE"
    )
    assert ego._STORE_VERDICTS_CYPHER == (
        "UNWIND $rows AS row MERGE (v:ERVerdict {key: row.key}) "
        "SET v.same = row.same, v.updated = datetime()"
    )


def test_neo4j_ensure_verdict_schema_issues_constraint():
    store = _RecStore()
    ego.Neo4jERGraphOps(store).ensure_verdict_schema()
    assert store.calls == [(ego._ENSURE_VERDICT_CONSTRAINT_CYPHER, None)]


def test_neo4j_merge_loser_into_canonical_issues_apoc_merge_cypher():
    store = _RecStore()
    ops = ego.Neo4jERGraphOps(store)

    ops.merge_loser_into_canonical(loser="L", canon="C")

    assert len(store.calls) == 1
    cypher, param_map = store.calls[0]
    assert param_map == {"loser": "L", "canon": "C"}
    assert "apoc.merge.relationship" in cypher
    assert "DETACH DELETE loser" in cypher
    assert cypher == ego._MERGE_LOSER_CYPHER


# --- Dispatch ----------------------------------------------------------


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(ego.settings.graph, "backend", "neo4j")
    assert isinstance(ego.build_er_graph_ops(_RecStore()), ego.Neo4jERGraphOps)


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(ego.settings.graph, "backend", "nebula")
    assert isinstance(ego.build_er_graph_ops(_RecStore()), ego.NebulaERGraphOps)


# --- Nebula stub: ensure_verdict_schema no-op, others raise -----------


def test_nebula_ensure_verdict_schema_is_noop():
    store = _RecStore()
    ego.NebulaERGraphOps(store).ensure_verdict_schema()
    assert store.calls == []


def test_nebula_stub_methods_raise_not_implemented():
    ops = ego.NebulaERGraphOps(_RecStore())
    try:
        ops.load_verdicts(["k"])
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass
    try:
        ops.store_verdicts({"k": True})
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass
    try:
        ops.merge_loser_into_canonical(loser="L", canon="C")
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass

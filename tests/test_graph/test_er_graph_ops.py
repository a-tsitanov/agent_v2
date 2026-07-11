from __future__ import annotations

import re

from src.graph import er_graph_ops as ego
from src.graph.nebula_store import _q, entity_vid


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


# --- Nebula: ensure_verdict_schema no-op; verdict cache + merge --------


def test_nebula_ensure_verdict_schema_is_noop():
    store = _RecStore()
    ego.NebulaERGraphOps(store).ensure_verdict_schema()
    assert store.calls == []


class _RecNebula:
    """Fake nebula store: records nGQL statements (asserts no param_map —
    nebula binds no params inline); returns canned rows keyed by a
    substring match against the statement. ``raise_on``, if given, makes
    ``structured_query`` raise once a matching statement is issued (the
    statement is still recorded first — mirrors the real store attempting
    the write before nGQL reports failure)."""

    def __init__(self, canned: dict[str, list[dict]] | None = None, raise_on: str | None = None):
        self.stmts: list[str] = []
        self._canned = dict(canned or {})
        self._raise_on = raise_on

    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula structured_query must not receive param_map"
        self.stmts.append(query)
        if self._raise_on and self._raise_on in query:
            raise RuntimeError(f"nGQL failed: {query}")
        for needle, rows in self._canned.items():
            if needle in query:
                return rows
        return []


def test_nebula_load_verdicts_fetches_by_vid_and_maps_rows():
    k1, k2 = ego.verdict_vid("k1"), ego.verdict_vid("k2")
    store = _RecNebula(
        canned={
            "FETCH PROP ON `ERVerdict`": [
                {"key": "k1", "same": True},
                {"key": "k2", "same": False},
            ],
        }
    )
    ops = ego.NebulaERGraphOps(store)

    result = ops.load_verdicts(["k1", "k2"])

    assert result == {"k1": True, "k2": False}
    assert len(store.stmts) == 1
    stmt = store.stmts[0]
    assert stmt.startswith("FETCH PROP ON `ERVerdict`")
    assert _q(k1) in stmt
    assert _q(k2) in stmt
    assert "`ERVerdict`.er_key AS key" in stmt
    assert "`ERVerdict`.same AS same" in stmt


def test_nebula_load_verdicts_empty_keys_issues_no_query():
    store = _RecNebula()
    assert ego.NebulaERGraphOps(store).load_verdicts([]) == {}
    assert store.stmts == []


def test_nebula_store_verdicts_inserts_vertex_by_vid_batched():
    store = _RecNebula()
    ego.NebulaERGraphOps(store).store_verdicts({"k": True})

    assert len(store.stmts) == 1
    stmt = store.stmts[0]
    assert stmt.startswith("INSERT VERTEX `ERVerdict` (er_key, same, updated) VALUES")
    vid = ego.verdict_vid("k")
    m = re.search(rf'{_q(vid)}:\({_q("k")}, true, (\d+)\)', stmt)
    assert m, stmt
    assert int(m.group(1)) > 0  # now_ms


def test_nebula_store_verdicts_empty_entries_issues_no_query():
    store = _RecNebula()
    ego.NebulaERGraphOps(store).store_verdicts({})
    assert store.stmts == []


def test_nebula_merge_loser_into_canonical_redirects_edges_then_deletes_last():
    lv, cv = entity_vid("L"), entity_vid("C")
    tv, sv = entity_vid("T"), entity_vid("S")
    store = _RecNebula(
        canned={
            "dst(edge)": [
                {"t": tv, "rt": "MENTIONS", "pol": "pos", "vf": 1, "vt": 2, "w": 0.5},
            ],
            "src(edge)": [
                {"s": sv, "rt": "MENTIONS", "pol": "neg", "vf": 3, "vt": 4, "w": 1.5},
            ],
        }
    )
    ops = ego.NebulaERGraphOps(store)

    ops.merge_loser_into_canonical(loser="L", canon="C")

    insert_stmts = [s for s in store.stmts if s.startswith("INSERT EDGE `RELATED`")]
    assert len(insert_stmts) == 2
    out_stmt, in_stmt = insert_stmts
    assert f'"{cv}" -> "{tv}"' in out_stmt
    assert f'"{sv}" -> "{cv}"' in in_stmt

    delete_stmts = [s for s in store.stmts if s.startswith("DELETE VERTEX")]
    assert delete_stmts == [f'DELETE VERTEX {_q(lv)} WITH EDGE;']

    # Both edge INSERTs happen strictly before the DELETE.
    delete_idx = store.stmts.index(delete_stmts[0])
    for s in insert_stmts:
        assert store.stmts.index(s) < delete_idx


def test_nebula_merge_loser_into_canonical_skips_edge_to_canon_itself():
    lv, cv = entity_vid("L"), entity_vid("C")
    store = _RecNebula(
        canned={
            "dst(edge)": [{"t": cv, "rt": "X", "pol": "", "vf": 0, "vt": 0, "w": 1.0}],
            "src(edge)": [{"s": cv, "rt": "X", "pol": "", "vf": 0, "vt": 0, "w": 1.0}],
        }
    )
    ops = ego.NebulaERGraphOps(store)

    ops.merge_loser_into_canonical(loser="L", canon="C")

    assert not any(s.startswith("INSERT EDGE") for s in store.stmts)
    assert store.stmts[-1] == f'DELETE VERTEX {_q(lv)} WITH EDGE;'


def test_nebula_merge_loser_into_canonical_noop_when_loser_equals_canon():
    store = _RecNebula()
    ego.NebulaERGraphOps(store).merge_loser_into_canonical(loser="X", canon="X")
    assert store.stmts == []


def test_nebula_merge_loser_into_canonical_leaves_loser_intact_on_reinsert_failure():
    """Safety guarantee: if a redirected-edge re-insert fails, the loser
    must NOT be deleted (no DELETE VERTEX issued) — the exception must
    propagate so the caller's try/except leaves the loser (with its
    original edges) intact."""
    tv = entity_vid("T")
    store = _RecNebula(
        canned={"dst(edge)": [{"t": tv, "rt": "X", "pol": "", "vf": 0, "vt": 0, "w": 1.0}]},
        raise_on="INSERT EDGE",
    )
    ops = ego.NebulaERGraphOps(store)

    try:
        ops.merge_loser_into_canonical(loser="L", canon="C")
        raise AssertionError("expected the nGQL failure to propagate")
    except AssertionError:
        raise
    except RuntimeError:
        pass

    assert not any(s.startswith("DELETE VERTEX") for s in store.stmts)

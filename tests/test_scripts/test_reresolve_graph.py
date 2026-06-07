"""Unit tests for the pure / stubbed helpers in scripts/reresolve_graph.py.

The whole-graph consolidation job needs a live Neo4j + LLM + embed model
to actually run; those paths are exercised manually via
``python -m scripts.reresolve_graph``.  Here we cover only the pure
building blocks that make the dry-run safe and the merge decision correct:

  * ``_is_write_cypher`` — the read/write classifier behind the proxy.
  * ``_ReadOnlyGraphStore`` — passes reads through, no-ops writes,
    delegates arbitrary attributes to the inner store.
  * ``_resolve_chains`` — collapses transitive merge chains and survives
    cycles without infinite-looping.
"""
from __future__ import annotations

import importlib

rrg = importlib.import_module("scripts.reresolve_graph")


# ── _is_write_cypher ────────────────────────────────────────────────


def test_is_write_cypher_detects_cleanup_detach_delete() -> None:
    # The _cleanup_stored_losers primitive: DETACH DELETE + apoc.merge.
    cypher = """
    MATCH (loser:__Entity__ {name: $loser})
    MATCH (canon:__Entity__ {name: $canon})
    CALL apoc.merge.relationship(canon, rt, {}, rp, t, {}) YIELD rel
    DETACH DELETE loser
    """
    assert rrg._is_write_cypher(cypher) is True


def test_is_write_cypher_detects_create_vector_index() -> None:
    assert rrg._is_write_cypher(
        "CREATE VECTOR INDEX er_embedding_vec IF NOT EXISTS "
        "FOR (n:__Entity__) ON (n.er_vec)"
    ) is True


def test_is_write_cypher_detects_set() -> None:
    assert rrg._is_write_cypher("MATCH (n) SET n.x = 1 RETURN n") is True


def test_is_write_cypher_detects_merge_and_create_and_delete() -> None:
    assert rrg._is_write_cypher("MATCH (a),(b) MERGE (a)-[:R]->(b)") is True
    assert rrg._is_write_cypher("CREATE (n:Foo {x: 1})") is True
    assert rrg._is_write_cypher("MATCH (n) DELETE n") is True


def test_is_write_cypher_false_for_plain_read() -> None:
    assert rrg._is_write_cypher("MATCH (n) RETURN n") is False


def test_is_write_cypher_false_for_native_knn_read() -> None:
    # The native-kNN candidate load is a read and MUST pass through.
    cypher = (
        "CALL db.index.vector.queryNodes('er_embedding_vec', $k, $vec) "
        "YIELD node WHERE node.er_canonical_name IS NOT NULL "
        "RETURN node.name AS name"
    )
    assert rrg._is_write_cypher(cypher) is False


def test_is_write_cypher_case_insensitive() -> None:
    # Lower-cased input still classifies as a write.
    assert rrg._is_write_cypher("match (n) detach delete n") is True


# ── _ReadOnlyGraphStore ─────────────────────────────────────────────


class _RecordingStore:
    """Inner stub that records every structured_query call and exposes
    an arbitrary attribute to prove __getattr__ delegation."""

    arbitrary_attr = "delegated-value"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return [{"name": "from-inner"}]

    def some_method(self, x):
        return x * 2


def test_proxy_passes_read_through_to_inner() -> None:
    inner = _RecordingStore()
    proxy = rrg._ReadOnlyGraphStore(inner)
    result = proxy.structured_query("MATCH (n) RETURN n", {"a": 1})
    assert result == [{"name": "from-inner"}]
    assert inner.calls == [("MATCH (n) RETURN n", {"a": 1})]


def test_proxy_noops_write_without_touching_inner() -> None:
    inner = _RecordingStore()
    proxy = rrg._ReadOnlyGraphStore(inner)
    result = proxy.structured_query("MATCH (n) DETACH DELETE n")
    # No-op: returns empty list, inner store NEVER called.
    assert result == []
    assert inner.calls == []


def test_proxy_delegates_arbitrary_attribute() -> None:
    inner = _RecordingStore()
    proxy = rrg._ReadOnlyGraphStore(inner)
    assert proxy.arbitrary_attr == "delegated-value"
    assert proxy.some_method(21) == 42


# ── _resolve_chains ─────────────────────────────────────────────────


def test_resolve_chains_collapses_transitive() -> None:
    # A->B, B->C  =>  A->C, B->C  (everyone points at the final canon).
    out = rrg._resolve_chains({"A": "B", "B": "C"})
    assert out == {"A": "C", "B": "C"}


def test_resolve_chains_handles_cycle_without_hanging() -> None:
    # A->B, B->A is a degenerate cycle with no terminal canonical — must
    # terminate (not loop forever) and produce a finite, safe result.
    # Following either side loops back to itself, so neither merge is
    # emitted: a pure cycle has no winner, and dropping both is safer
    # than deleting both nodes.
    out = rrg._resolve_chains({"A": "B", "B": "A"})
    assert out == {}
    # Result keys are always a subset of the input losers, and no loser
    # ever maps to itself.
    assert set(out.keys()) <= {"A", "B"}
    for loser, canon in out.items():
        assert loser != canon


def test_resolve_chains_longer_chain() -> None:
    out = rrg._resolve_chains({"A": "B", "B": "C", "C": "D"})
    assert out == {"A": "D", "B": "D", "C": "D"}


def test_resolve_chains_empty() -> None:
    assert rrg._resolve_chains({}) == {}


def test_resolve_chains_drops_self_map() -> None:
    # A loser equal to its canon is a no-op pair — should not appear.
    out = rrg._resolve_chains({"A": "A", "B": "C"})
    assert out == {"B": "C"}

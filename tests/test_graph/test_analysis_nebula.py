"""Nebula backend for graph analysis (TDD). Under nebula there is no GDS:
pagerank reads the igraph-materialized property; stats/components use nGQL /
in-worker compute. Fake store returns canned rows per query substring."""
from __future__ import annotations

import pytest

from src.graph import analysis


class _FakeNebulaStore:
    def __init__(self, canned: list[tuple[str, list[dict]]] | None = None):
        self._canned = list(canned or [])
        self.calls: list[str] = []

    def structured_query(self, stmt, param_map=None):
        self.calls.append(stmt)
        assert param_map is None, "nebula path must not use param_map"
        for sub, rows in self._canned:
            if sub in stmt:
                return rows
        return []


@pytest.mark.asyncio
async def test_pagerank_nebula_reads_materialized_property(monkeypatch):
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    store = _FakeNebulaStore([("`Entity`.pagerank", [
        {"name": "A", "score": 0.9}, {"name": "B", "score": 0.5}])])
    out = await analysis.pagerank(store, top_n=5)
    assert out == [{"name": "A", "score": 0.9}, {"name": "B", "score": 0.5}]
    stmt = store.calls[0]
    assert "e.`Entity`.pagerank" in stmt
    assert "ORDER BY score DESC" in stmt
    assert "LIMIT 5" in stmt
    assert "gds." not in stmt.lower()  # no GDS under nebula


@pytest.mark.asyncio
async def test_pagerank_nebula_fail_soft(monkeypatch):
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)

    class _Boom:
        def structured_query(self, *a, **k):
            raise RuntimeError("nebula down")

    assert await analysis.pagerank(_Boom(), top_n=5) == []


@pytest.mark.asyncio
async def test_graph_stats_nebula_counts_degree_dup(monkeypatch):
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    store = _FakeNebulaStore([
        ("AS ent_n", [{"ent_n": 5}]),
        ("AS rel_n", [{"rel_n": 8}]),
        ("AS comm_n", [{"comm_n": 3}]),
        ("AS deg", [{"deg": 2}, {"deg": 4}, {"deg": 0}, {"deg": 4}]),
        ("AS dup_name", [{"dup_name": "Foo"}, {"dup_name": "foo"}, {"dup_name": "Bar"}]),
    ])
    out = await analysis.graph_stats(store)
    assert out["entities"] == 5
    assert out["relationships"] == 8
    assert out["communities"] == 3
    assert out["degree"]["max"] == 4
    assert out["degree"]["avg"] == pytest.approx((2 + 4 + 0 + 4) / 4)
    assert out["duplicate_name_groups"] == 1   # {foo} (Foo + foo)
    assert out["duplicate_entities"] == 2
    assert all("gds." not in c.lower() for c in store.calls)  # no GDS under nebula


@pytest.mark.asyncio
async def test_graph_stats_nebula_empty_graph(monkeypatch):
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    out = await analysis.graph_stats(_FakeNebulaStore([]))
    assert out["entities"] == 0 and out["degree"]["max"] == 0


def test_components_from_edges_counts_weak_components():
    # {A,B,C} + {D,E} + isolated F  ->  3 components, sizes [3,2,1]
    edges = [("A", "B", 1.0), ("B", "C", 1.0), ("D", "E", 1.0)]
    names = ["A", "B", "C", "D", "E", "F"]
    out = analysis._components_from_edges(edges, names)
    assert out["component_count"] == 3
    assert out["distribution"]["max"] == 3
    assert out["distribution"]["min"] == 1
    assert out["distribution"]["count"] == 3


def test_components_from_edges_empty():
    out = analysis._components_from_edges([], [])
    assert out == {"component_count": 0, "distribution": {}}


_LINE = [("A", "B", 1.0), ("B", "C", 1.0), ("C", "D", 1.0)]
_LINE_NAMES = ["A", "B", "C", "D", "F"]  # F isolated


def test_shortest_path_from_edges_finds_path():
    out = analysis._shortest_path_from_edges(_LINE, _LINE_NAMES, "A", "D")
    assert out["path"] == ["A", "B", "C", "D"]
    assert out["hops"] == 3


def test_shortest_path_from_edges_no_path_to_isolated():
    out = analysis._shortest_path_from_edges(_LINE, _LINE_NAMES, "A", "F")
    assert out == {"path": [], "hops": -1}


def test_shortest_path_from_edges_missing_endpoint():
    out = analysis._shortest_path_from_edges(_LINE, _LINE_NAMES, "A", "ZZZ")
    assert out == {"path": [], "hops": -1}


def test_personalized_pagerank_biases_toward_seed_component():
    # two disconnected components; seed on A -> A's component outranks the other
    edges = [("A", "B", 1.0), ("B", "C", 1.0), ("X", "Y", 1.0)]
    names = ["A", "B", "C", "X", "Y"]
    out = analysis._personalized_pagerank_from_edges(edges, names, ["A"], 5)
    d = {r["name"]: r["score"] for r in out}
    assert d["A"] > d["X"]
    assert d["B"] > d["Y"]


def test_personalized_pagerank_empty_seeds():
    assert analysis._personalized_pagerank_from_edges(
        [("A", "B", 1.0)], ["A", "B"], [], 5) == []

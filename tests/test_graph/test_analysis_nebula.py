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
async def test_graph_stats_nebula_no_rows_is_not_zero(monkeypatch):
    """A count aggregate ALWAYS yields a row — verified on the live store,
    `MATCH (a:Alert) RETURN count(a)` returns [{'n': 0}] on an empty tag.
    So a query answering with nothing at all did not measure zero, and
    must not be reported as zero."""
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    out = await analysis.graph_stats(_FakeNebulaStore([]))
    assert out["entities"] is None
    assert out["relationships"] is None
    assert out["communities"] is None
    assert out["errors"]["entities"]


@pytest.mark.asyncio
async def test_graph_stats_nebula_reports_a_measured_zero_as_zero(monkeypatch):
    """The other side of it: an empty graph that ANSWERS zero is zero,
    with no error recorded."""
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    store = _FakeNebulaStore([
        ("AS ent_n", [{"ent_n": 0}]),
        ("AS rel_n", [{"rel_n": 0}]),
        ("AS comm_n", [{"comm_n": 0}]),
        ("AS deg", []),
        ("AS dup_name", []),
    ])
    out = await analysis.graph_stats(store)
    assert out["entities"] == 0
    assert out["relationships"] == 0
    assert "entities" not in out["errors"]
    assert out["degree"] == {"avg": 0.0, "p50": 0, "p99": 0, "max": 0}


@pytest.mark.asyncio
async def test_graph_stats_nebula_failure_is_none_not_zero(monkeypatch):
    """The 2026-08-16 defect: the counting scans were refused for memory
    and the handler turned that into 0, so the tool reported "no
    relationships, no communities" on a space holding 310 989 and 2 302."""
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)

    class _Refusing:
        def structured_query(self, stmt, param_map=None):
            if "SHOW STATS" in stmt or "SHOW JOBS" in stmt:
                raise RuntimeError("no stats info")
            raise RuntimeError("Used memory hits the high watermark(0.800000)")

    out = await analysis.graph_stats(_Refusing())
    assert out["relationships"] is None
    assert out["communities"] is None
    assert out["degree"] is None
    assert "high watermark" in out["errors"]["relationships"]


@pytest.mark.asyncio
async def test_graph_stats_nebula_prefers_show_stats(monkeypatch):
    """`SHOW STATS` serves Nebula's own job results: exact, milliseconds,
    and no scan — the scans it replaces fail outright on the production
    space. The counting queries must not even be issued."""
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    store = _FakeNebulaStore([
        ("SHOW STATS", [
            {"Type": "Tag", "Name": "Entity", "Count": 161367},
            {"Type": "Tag", "Name": "Community", "Count": 2302},
            {"Type": "Edge", "Name": "RELATED", "Count": 311387},
            {"Type": "Space", "Name": "vertices", "Count": 163669},
        ]),
        ("SHOW JOBS", [
            {"Command": "STATS", "Status": "FINISHED",
             "Stop Time": "utc datetime: 2026-08-16T21:44:28.000000, timezone_offset: 0"},
        ]),
        ("AS deg", [{"deg": 3}]),
        ("AS dup_name", []),
    ])
    out = await analysis.graph_stats(store)
    assert out["entities"] == 161367
    assert out["relationships"] == 311387
    assert out["communities"] == 2302
    assert out["source"] == "show_stats"
    assert out["stats_computed_at"] == "2026-08-16T21:44:28.000000"
    # The three counting scans it replaces are not issued at all. (The
    # degree query still is — SHOW STATS reports totals, not a
    # distribution — so match on their result aliases, not on `count(`.)
    assert not any(
        alias in c for c in store.calls for alias in ("AS ent_n", "AS rel_n", "AS comm_n")
    )


@pytest.mark.asyncio
async def test_graph_stats_nebula_falls_back_to_scanning(monkeypatch):
    """No stats job has ever run — Nebula answers "please execute `submit
    job stats' firstly". Fall back to the scans rather than report
    nothing."""
    monkeypatch.setattr("src.graph.analysis.settings.graph.backend", "nebula", raising=False)
    store = _FakeNebulaStore([
        ("AS ent_n", [{"ent_n": 5}]),
        ("AS rel_n", [{"rel_n": 8}]),
        ("AS comm_n", [{"comm_n": 3}]),
        ("AS deg", []),
        ("AS dup_name", []),
    ])
    out = await analysis.graph_stats(store)
    assert out["source"] == "scan"
    assert out["entities"] == 5 and out["relationships"] == 8


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

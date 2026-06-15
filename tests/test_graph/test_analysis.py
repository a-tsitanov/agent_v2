"""Unit tests for read-only GDS graph analysis (Track 7b).

No live Neo4j/GDS: a fake store returns canned rows keyed by which GDS
call the Cypher contains.  Asserts result shaping + fail-soft behaviour.
"""

from __future__ import annotations

import pytest

from src.graph.analysis import (
    _pagerank_cypher,
    components,
    graph_stats,
    pagerank,
    shortest_path,
)


class _FakeStore:
    """Returns canned rows depending on the GDS/Cypher fragment seen.
    Project/drop calls return []."""

    def __init__(self, *, rows_by_fragment=None, raise_on=None):
        self._rows = rows_by_fragment or {}
        self._raise_on = raise_on
        self.calls: list[str] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append(cypher)
        if self._raise_on and self._raise_on in cypher:
            raise RuntimeError("boom")
        for fragment, rows in self._rows.items():
            if fragment in cypher:
                return rows
        return []


# ── pagerank ─────────────────────────────────────────────────────────


def test_pagerank_cypher_is_weighted_and_limited():
    cy = _pagerank_cypher("g", 5)
    assert "gds.pageRank.stream" in cy
    assert "relationshipWeightProperty" in cy
    assert "LIMIT 5" in cy


@pytest.mark.asyncio
async def test_pagerank_returns_scored_names():
    store = _FakeStore(rows_by_fragment={
        "gds.pageRank.stream": [
            {"name": "Иванов", "score": 9.1},
            {"name": "СтройИнвест", "score": 4.2},
        ],
    })
    out = await pagerank(store, top_n=2)
    assert out == [
        {"name": "Иванов", "score": 9.1},
        {"name": "СтройИнвест", "score": 4.2},
    ]


# ── components ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_components_parses_wcc():
    store = _FakeStore(rows_by_fragment={
        "gds.wcc.stats": [
            {"componentCount": 42, "componentDistribution": {"p99": 7, "max": 1200}},
        ],
    })
    out = await components(store)
    assert out["component_count"] == 42
    assert out["distribution"]["max"] == 1200


# ── shortest path ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_shortest_path_returns_path_and_hops():
    store = _FakeStore(rows_by_fragment={
        "shortestPath": [{"path": ["A", "X", "B"], "hops": 2}],
    })
    out = await shortest_path(store, "A", "B")
    assert out["path"] == ["A", "X", "B"]
    assert out["hops"] == 2


@pytest.mark.asyncio
async def test_shortest_path_none_when_disconnected():
    store = _FakeStore(rows_by_fragment={})  # no path row
    out = await shortest_path(store, "A", "B")
    assert out["path"] == []
    assert out["hops"] == -1


# ── graph stats ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_graph_stats_assembles_counts():
    store = _FakeStore(rows_by_fragment={
        "count(e)": [{"n": 50000}],
        "(:__Entity__)-[r]->(:__Entity__)": [{"n": 120000}],
        "percentileCont": [{"avg": 4.8, "p50": 3, "p99": 41, "max": 5000}],
        "c > 1": [{"dup_groups": 130, "dup_entities": 410}],
        "c:Community": [{"n": 191}],
    })
    out = await graph_stats(store)
    assert out["entities"] == 50000
    assert out["relationships"] == 120000
    assert out["degree"]["p99"] == 41
    assert out["duplicate_name_groups"] == 130
    assert out["communities"] == 191


# ── fail-soft ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_none_store_is_failsafe():
    assert await pagerank(None) == []
    assert (await components(None))["component_count"] == 0
    assert (await shortest_path(None, "A", "B"))["hops"] == -1
    assert (await graph_stats(None))["entities"] == 0


@pytest.mark.asyncio
async def test_store_error_is_failsafe():
    store = _FakeStore(raise_on="gds.pageRank.stream")
    assert await pagerank(store) == []

import pytest

from src.analytics.primitives import centrality as c
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_top_central_reads_metric_property():
    # _FakeStore drives the default Neo4jCentralityGraphOps path.
    store = _FakeStore(rows=[{"name": "A", "score": 0.9}])
    res = await c.top_central_entities(store, metric="betweenness", top_n=5)
    assert res.params["top_n"] == 5 and res.params["metric"] == "betweenness"
    assert res.rows[0]["name"] == "A"
    assert "e.betweenness" in store.last_cypher  # metric inlined into the seam's Cypher


@pytest.mark.asyncio
async def test_top_central_rejects_unknown_metric():
    store = _FakeStore(rows=[{"x": 1}])
    res = await c.top_central_entities(store, metric="bogus")
    assert res.rows == []  # allowlist guard → empty, no injection


@pytest.mark.asyncio
async def test_link_prediction_reads_edges():
    store = _FakeStore(rows=[{"name": "B", "score": 0.8}])
    res = await c.link_prediction(store, name="A")
    assert res.params["name"] == "A"
    assert res.rows[0]["name"] == "B"
    assert ":LIKELY_LINK" in store.last_cypher  # neo4j seam still issues the edge Cypher

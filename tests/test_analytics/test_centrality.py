import pytest

from src.analytics.primitives import centrality as c
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_top_central_reads_metric_property():
    store = _FakeStore(rows=[{"name": "A", "score": 0.9}])
    res = await c.top_central_entities(store, metric="betweenness", top_n=5)
    assert "e.betweenness" in res.cypher and res.params["top_n"] == 5


@pytest.mark.asyncio
async def test_top_central_rejects_unknown_metric():
    store = _FakeStore(rows=[{"x": 1}])
    res = await c.top_central_entities(store, metric="bogus")
    assert res.rows == []  # allowlist guard → empty, no injection


@pytest.mark.asyncio
async def test_link_prediction_reads_edges():
    store = _FakeStore(rows=[{"name": "B", "score": 0.8}])
    res = await c.link_prediction(store, name="A")
    assert ":LIKELY_LINK" in res.cypher and res.params["name"] == "A"

import pytest

from src.analytics.primitives import aggregations as agg
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_count_entities_excludes_identifiers_by_default():
    store = _FakeStore(rows=[{"n": 7}])
    res = await agg.count_entities(store, type="Organization")
    assert res.rows == [{"n": 7}]
    assert "count(e)" in res.cypher
    assert res.params["type"] == "Organization"
    # identifier exclusion present
    assert "ID_TYPES" in res.cypher or "$id_types" in res.cypher
    assert res.params["id_types"]  # passed in


@pytest.mark.asyncio
async def test_count_entities_failsoft():
    res = await agg.count_entities(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_count_relationships_filters_rel_type_and_polarity():
    store = _FakeStore(rows=[{"n": 3}])
    res = await agg.count_relationships(store, rel_type="OWNS", polarity="negated")
    assert res.params["rel_type"] == "OWNS" and res.params["polarity"] == "negated"


@pytest.mark.asyncio
async def test_top_entities_by_mentions_clamps_and_orders():
    store = _FakeStore(rows=[{"name": "X", "mentions": 9}])
    res = await agg.top_entities_by_mentions(store, top_n=99999)
    assert res.params["top_n"] == 200  # clamp
    assert "mention_count" in res.cypher and "ORDER BY" in res.cypher


@pytest.mark.asyncio
async def test_distribution_by_type_shape():
    store = _FakeStore(rows=[{"type": "Person", "n": 5}])
    res = await agg.distribution_by_type(store)
    assert res.rows[0]["type"] == "Person"


@pytest.mark.asyncio
async def test_top_entities_by_degree_excludes_negated():
    store = _FakeStore(rows=[{"name": "X", "degree": 4}])
    res = await agg.top_entities_by_degree(store)
    assert "negated" in res.cypher

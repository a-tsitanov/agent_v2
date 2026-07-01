# tests/test_analytics/test_quality.py
import pytest

from src.analytics.primitives import quality as q
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_contradictions_requires_overlap_caveat_in_cypher():
    store = _FakeStore(rows=[{"a": "A", "rel": "OWNS", "b": "B"}])
    res = await q.contradictions(store)
    assert "affirmed" in res.cypher and "negated" in res.cypher
    # temporal-overlap guard present (don't flag a fact that changed over time)
    assert "valid_from" in res.cypher and "valid_to" in res.cypher


@pytest.mark.asyncio
async def test_orphans_uses_min_degree(monkeypatch):
    store = _FakeStore(rows=[{"name": "Lonely", "degree": 0}])
    res = await q.orphans(store, min_degree=1)
    assert res.params["min_degree"] == 1


@pytest.mark.asyncio
async def test_failsoft():
    assert (await q.contradictions(None)).rows == []


@pytest.mark.asyncio
async def test_incomplete_entities_uses_expected_attrs():
    store = _FakeStore(rows=[{"name": "Орг1", "missing": ["INN"], "have": ["OGRN"]}])
    res = await q.incomplete_entities(store, type="Organization")
    assert res.params["type"] == "Organization"
    assert "INN" in res.params["expected"]  # from settings.signals.expected_attrs


@pytest.mark.asyncio
async def test_merge_candidates_groups_duplicate_names():
    store = _FakeStore(rows=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}])
    res = await q.merge_candidates(store)
    assert "toLower" in res.cypher and "count(" in res.cypher.lower()

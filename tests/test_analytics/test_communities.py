# tests/test_analytics/test_communities.py
import pytest

from src.analytics.primitives import communities as com
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_community_overview_reads_level():
    store = _FakeStore(rows=[{"title": "Поставки", "summary": "...", "member_count": 12}])
    res = await com.community_overview(store, level=0)
    assert res.params["level"] == 0
    assert "c:Community" in res.cypher and "member_count" in res.cypher


@pytest.mark.asyncio
async def test_entity_communities_by_name():
    store = _FakeStore(rows=[{"level": 0, "title": "Поставки", "summary": "s"}])
    res = await com.entity_communities(store, name="Ромашка")
    assert ":IN_COMMUNITY" in res.cypher


@pytest.mark.asyncio
async def test_personalized_pagerank_wraps_analysis(monkeypatch):
    async def _fake_ppr(store, seeds, *, top_n=20):
        return [{"name": "X", "score": 0.4}]

    monkeypatch.setattr(com, "_analysis_ppr", _fake_ppr)
    res = await com.personalized_pagerank(object(), seeds=["A"], top_n=5)
    assert res.rows == [{"name": "X", "score": 0.4}]
    assert res.params["seeds"] == ["A"]


@pytest.mark.asyncio
async def test_personalized_pagerank_failsoft_no_seeds():
    res = await com.personalized_pagerank(None, seeds=[])
    assert res.rows == []

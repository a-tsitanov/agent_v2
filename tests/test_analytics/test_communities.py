# tests/test_analytics/test_communities.py
import pytest

from src.analytics.primitives import communities as com
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_community_overview_reads_level():
    # _FakeStore drives the default Neo4jCommunitiesGraphOps path.
    store = _FakeStore(rows=[{"title": "Поставки", "summary": "...", "member_count": 12}])
    res = await com.community_overview(store, level=0)
    assert res.params["level"] == 0
    assert res.rows[0]["member_count"] == 12


@pytest.mark.asyncio
async def test_community_overview_fail_soft_none_store():
    assert (await com.community_overview(None)).rows == []


@pytest.mark.asyncio
async def test_entity_communities_by_name():
    store = _FakeStore(rows=[{"level": 0, "title": "Поставки", "summary": "s"}])
    res = await com.entity_communities(store, name="Ромашка")
    assert res.params["name"] == "Ромашка"
    assert res.rows[0]["title"] == "Поставки"


@pytest.mark.asyncio
async def test_entity_communities_fail_soft_none_store():
    assert (await com.entity_communities(None, name="X")).rows == []


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

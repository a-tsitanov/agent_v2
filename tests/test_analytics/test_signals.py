import pytest

from src.analytics.primitives import signals as sig
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_risk_score_reads_materialized():
    store = _FakeStore(rows=[{"name": "Shell", "score": 0.8, "band": "high", "components": "{}"}])
    res = await sig.risk_score(store, band="high")
    assert "e.risk_score" in res.cypher and res.params["band"] == "high"


@pytest.mark.asyncio
async def test_risk_score_by_name():
    store = _FakeStore(rows=[{"name": "A", "score": 0.4, "band": "medium", "components": "{}"}])
    res = await sig.risk_score(store, name="A")
    assert res.params["name"] == "A"


@pytest.mark.asyncio
async def test_investigate_next_ranks_high_risk_low_completeness():
    store = _FakeStore(rows=[{"name": "X", "risk": 0.9, "completeness": 0.2}])
    res = await sig.investigate_next(store)
    assert "risk_score" in res.cypher and res.params["top_n"] == 20
    assert "ORDER BY e.risk_score DESC" in res.cypher


@pytest.mark.asyncio
async def test_recommended_merges_groups_dup_names():
    store = _FakeStore(rows=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}])
    res = await sig.recommended_merges(store)
    assert "toLower" in res.cypher
    assert "id_types" in res.params


@pytest.mark.asyncio
async def test_review_queue_shell_signal():
    store = _FakeStore(rows=[{"name": "Org", "degree": 2, "flag": "shell_signal"}])
    res = await sig.review_queue(store)
    assert "Organization" in res.cypher and res.params["top_n"] == 50


@pytest.mark.asyncio
async def test_circular_ownership_cypher():
    store = _FakeStore(rows=[{"cycle": ["A", "B", "A"]}])
    res = await sig.circular_ownership(store)
    assert ":OWNS*2..6" in res.cypher or "OWNS*2..6" in res.cypher

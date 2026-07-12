import pytest

from src.analytics.primitives import signals as sig
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_risk_score_reads_materialized():
    # _FakeStore drives the default Neo4jSignalsGraphOps path.
    store = _FakeStore(rows=[{"name": "Shell", "score": 0.8, "band": "high", "components": "{}"}])
    res = await sig.risk_score(store, band="high")
    assert res.params["band"] == "high"
    assert res.rows[0]["name"] == "Shell"


@pytest.mark.asyncio
async def test_risk_score_by_name():
    store = _FakeStore(rows=[{"name": "A", "score": 0.4, "band": "medium", "components": "{}"}])
    res = await sig.risk_score(store, name="A")
    assert res.params["name"] == "A"


@pytest.mark.asyncio
async def test_investigate_next_ranks_high_risk_low_completeness():
    store = _FakeStore(rows=[{"name": "X", "risk": 0.9, "completeness": 0.2}])
    res = await sig.investigate_next(store)
    assert res.params["top_n"] == 20
    assert res.rows[0]["name"] == "X"


@pytest.mark.asyncio
async def test_recommended_merges_groups_dup_names():
    store = _FakeStore(rows=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}])
    res = await sig.recommended_merges(store)
    assert res.rows[0]["count"] == 2


@pytest.mark.asyncio
async def test_review_queue_shell_signal():
    store = _FakeStore(rows=[{"name": "Org", "degree": 2, "flag": "shell_signal"}])
    res = await sig.review_queue(store)
    assert res.params["top_n"] == 50
    assert res.rows[0]["flag"] == "shell_signal"


@pytest.mark.asyncio
async def test_circular_ownership_routes_through_seam():
    store = _FakeStore(rows=[{"cycle": ["A", "B", "A"]}])
    res = await sig.circular_ownership(store)
    assert res.rows[0]["cycle"] == ["A", "B", "A"]


@pytest.mark.asyncio
async def test_signals_fail_soft_none_store():
    assert (await sig.risk_score(None)).rows == []
    assert (await sig.investigate_next(None)).rows == []
    assert (await sig.recommended_merges(None)).rows == []
    assert (await sig.review_queue(None)).rows == []
    assert (await sig.circular_ownership(None)).rows == []

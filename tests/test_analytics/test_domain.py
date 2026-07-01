import pytest

from src.analytics.primitives import domain as dm
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_issue_resolution_stats_computes_rate():
    store = _FakeStore(rows=[{"total": 10, "unresolved": 4}])
    res = await dm.issue_resolution_stats(store)
    assert "RESOLVED_BY" in res.cypher
    assert ":Issue" in res.cypher and ":Resolution" in res.cypher
    # negated RESOLVED_BY edges must not count as resolutions
    assert "rr.polarity IS NULL OR rr.polarity <> 'negated'" in res.cypher
    row = res.rows[0]
    assert row["total_issues"] == 10
    assert row["unresolved"] == 4
    assert row["resolved"] == 6
    assert row["resolution_rate"] == 0.6


@pytest.mark.asyncio
async def test_issue_resolution_stats_empty_no_div_by_zero():
    res = await dm.issue_resolution_stats(_FakeStore(rows=[]))
    assert res.rows[0]["total_issues"] == 0
    assert res.rows[0]["resolution_rate"] == 0.0


@pytest.mark.asyncio
async def test_issue_resolution_stats_fail_soft_none_store():
    res = await dm.issue_resolution_stats(None)
    assert res.rows[0]["total_issues"] == 0


@pytest.mark.asyncio
async def test_communication_stats_counts_pairs():
    store = _FakeStore(rows=[{"a": "Alice", "b": "Bob", "rel": "RESPONDED_TO", "interactions": 5}])
    res = await dm.communication_stats(store)
    assert "RESPONDED_TO" in res.cypher and "CONTACT" in res.cypher
    assert res.rows[0]["interactions"] == 5


@pytest.mark.asyncio
async def test_communication_stats_name_filter_param():
    store = _FakeStore(rows=[])
    res = await dm.communication_stats(store, name="Alice", top_n=5)
    assert res.params["name"] == "Alice"
    assert res.params["top_n"] == 5
    assert "$name IS NULL OR a.name = $name" in res.cypher


def test_domain_primitives_registered():
    from src.analytics.catalog import CATALOG

    assert "issue_resolution_stats" in CATALOG
    assert "communication_stats" in CATALOG

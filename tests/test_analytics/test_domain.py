import pytest

from src.analytics.primitives import domain as dm
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_issue_resolution_stats_computes_rate():
    # _FakeStore drives the default Neo4jDomainGraphOps path (returns the raw
    # {total, unresolved}); the primitive computes resolved/rate.
    store = _FakeStore(rows=[{"total": 10, "unresolved": 4}])
    res = await dm.issue_resolution_stats(store)
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
    assert res.rows[0]["interactions"] == 5


@pytest.mark.asyncio
async def test_communication_stats_threads_name_and_top_n(monkeypatch):
    calls = {}

    class _Ops:
        def communication_stats(self, name, top_n):
            calls["args"] = (name, top_n)
            return [{"a": "Alice", "b": "Bob", "rel": "CONTACT", "interactions": 1}]

    monkeypatch.setattr(dm, "build_domain_graph_ops", lambda store: _Ops())
    res = await dm.communication_stats(object(), name="Alice", top_n=5)
    assert res.params["name"] == "Alice" and res.params["top_n"] == 5
    assert calls["args"] == ("Alice", 5)
    assert res.rows[0]["interactions"] == 1


@pytest.mark.asyncio
async def test_communication_stats_fail_soft_none_store():
    assert (await dm.communication_stats(None)).rows == []


def test_domain_primitives_registered():
    from src.analytics.catalog import CATALOG

    assert "issue_resolution_stats" in CATALOG
    assert "communication_stats" in CATALOG

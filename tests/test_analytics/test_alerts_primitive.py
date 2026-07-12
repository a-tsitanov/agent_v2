import pytest

from src.analytics.ids import clamp_top_n
from src.analytics.primitives import alerts as al
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_alerts_reads_alert_nodes_newest_first():
    store = _FakeStore(
        rows=[
            {
                "key": "risk_rise:Shell:0.8",
                "kind": "risk_rise",
                "entity": "Shell",
                "detail": "0.8",
                "created_at": 19900,
            }
        ]
    )
    res = await al.alerts(store, kind="risk_rise")
    # _FakeStore drives the default Neo4jAlertStore path (read_alerts_cypher).
    assert res.params["kind"] == "risk_rise"
    assert res.rows[0]["entity"] == "Shell"


@pytest.mark.asyncio
async def test_alerts_filters_passed_as_params():
    store = _FakeStore(rows=[])
    res = await al.alerts(store, kind="new_connection", entity="Shell")
    assert res.params["kind"] == "new_connection"
    assert res.params["entity"] == "Shell"


@pytest.mark.asyncio
async def test_alerts_window_days_sets_since(monkeypatch):
    monkeypatch.setattr(al, "today_epoch_days", lambda: 19900)
    store = _FakeStore(rows=[])
    res = await al.alerts(store, window_days=7)
    assert res.params["since"] == 19900 - 7


@pytest.mark.asyncio
async def test_alerts_no_window_means_since_null():
    store = _FakeStore(rows=[])
    res = await al.alerts(store)
    assert res.params["since"] is None


@pytest.mark.asyncio
async def test_alerts_fail_soft_on_none_store():
    res = await al.alerts(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_alerts_clamps_top_n():
    store = _FakeStore(rows=[])
    res = await al.alerts(store, top_n=100000)
    assert res.params["top_n"] == clamp_top_n(100000, default=50)


def test_alerts_registered_in_catalog():
    from src.analytics.catalog import CATALOG

    assert "alerts" in CATALOG

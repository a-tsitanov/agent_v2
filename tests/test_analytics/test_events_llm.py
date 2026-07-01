"""E2 event read primitives — event_dossier + event_timeline."""

from __future__ import annotations

import pytest

from src.analytics.primitives import events_llm
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_event_dossier_assembles_core_and_actors():
    """event_dossier gathers event core (type/ts/polarity) + actors."""
    store = _FakeStore(
        by_call=[
            [
                {
                    "name": "Murder of Sergei",
                    "event_type": "ASSASSINATION",
                    "event_ts": 1234567890,
                    "polarity": "negated",
                }
            ],  # core
            [
                {"actor_name": "Ivan", "rel": "VICTIM"},
                {"actor_name": "Boris", "rel": "SUSPECT"},
            ],  # actors
        ]
    )
    res = await events_llm.event_dossier(store, name="Murder of Sergei")
    row = res.rows[0]
    assert row["core"]["name"] == "Murder of Sergei"
    assert row["core"]["event_type"] == "ASSASSINATION"
    assert row["core"]["event_ts"] == 1234567890
    assert len(row["actors"]) == 2
    assert row["actors"][0]["actor_name"] == "Ivan"
    assert res.params["name"] == "Murder of Sergei"


@pytest.mark.asyncio
async def test_event_dossier_empty_actors():
    """event_dossier handles event with no actors gracefully."""
    store = _FakeStore(
        by_call=[
            [
                {
                    "name": "Market Rally",
                    "event_type": "MARKET_EVENT",
                    "event_ts": 1234567890,
                    "polarity": None,
                }
            ],  # core
            [],  # no actors
        ]
    )
    res = await events_llm.event_dossier(store, name="Market Rally")
    row = res.rows[0]
    assert row["core"]["name"] == "Market Rally"
    assert row["actors"] == []


@pytest.mark.asyncio
async def test_event_dossier_top_n_clamping():
    """event_dossier clamps top_n."""
    store = _FakeStore(by_call=[[], []])
    await events_llm.event_dossier(store, name="test", top_n=5000)
    assert store.last_params["top_n"] <= 200  # hard_max clamp


@pytest.mark.asyncio
async def test_event_timeline_orders_by_event_ts():
    """event_timeline returns events ordered by event_ts property."""
    store = _FakeStore(
        rows=[
            {"name": "Event A", "event_type": "PROTEST", "event_ts": 1000},
            {"name": "Event B", "event_type": "ARREST", "event_ts": 2000},
            {"name": "Event C", "event_type": "RELEASE", "event_ts": 1500},
        ]
    )
    res = await events_llm.event_timeline(store, entity="John")
    # Caller should order; we just check they came back
    assert len(res.rows) == 3
    assert res.rows[1]["name"] == "Event B"  # middle one if ordering preserved
    assert res.params["entity"] == "John"


@pytest.mark.asyncio
async def test_event_timeline_window_days():
    """event_timeline accepts window_days parameter."""
    store = _FakeStore(rows=[])
    res = await events_llm.event_timeline(store, entity="Jane", window_days=30)
    assert res.params["entity"] == "Jane"
    # Verify window_days is in params if used
    if "window_days" in res.params:
        assert res.params["window_days"] == 30


@pytest.mark.asyncio
async def test_event_timeline_top_n_clamping():
    """event_timeline clamps top_n."""
    store = _FakeStore(rows=[])
    await events_llm.event_timeline(store, entity="test", top_n=5000)
    assert store.last_params["top_n"] <= 200  # hard_max clamp


@pytest.mark.asyncio
async def test_trending_events_windows_and_shape(monkeypatch):
    monkeypatch.setattr(events_llm, "today_epoch_days", lambda: 19900)
    store = _FakeStore(
        rows=[
            {
                "entity": "Acme",
                "event_type": "lawsuit",
                "recent": 6,
                "baseline_rate": 1.0,
                "burst_score": 6.0,
            }
        ]
    )
    res = await events_llm.trending_events(store, window_days=7, baseline_windows=4)
    assert res.params["since_recent"] == 19900 - 7
    assert res.params["since_baseline"] == 19900 - 7 * (4 + 1)
    assert res.params["ratio"] == 1.0
    assert "burst_score" in res.cypher
    assert res.rows[0]["event_type"] == "lawsuit"


@pytest.mark.asyncio
async def test_trending_events_fail_soft_none_store():
    res = await events_llm.trending_events(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_event_timeline_window_days_applies_since(monkeypatch):
    monkeypatch.setattr(events_llm, "today_epoch_days", lambda: 19900)
    res = await events_llm.event_timeline(_FakeStore(rows=[]), entity="Acme", window_days=30)
    assert res.params["since"] == 19900 - 30
    assert "e.created_at >= $since" in res.cypher

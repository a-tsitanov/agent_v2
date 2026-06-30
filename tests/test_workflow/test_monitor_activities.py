"""Tests for detect_alerts activity (Arc 2 monitoring sweep)."""

from __future__ import annotations

import pytest

from src.analytics.contracts import MonitorIn

# Import the module under test — will fail until activities.py exists.
from src.workflow.monitor import activities as ma

_EDGE_HINT = "a.watched = true OR b.watched = true"
_RISK_HINT = "e.risk_score IS NOT NULL"
_MERGE_HINT = "MERGE"


class _FakeStore:
    """Records all structured_query calls and returns scripted rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def structured_query(self, cypher: str, param_map: dict | None = None) -> list[dict]:
        self.calls.append((cypher, param_map or {}))
        if _RISK_HINT in cypher:
            return [{"name": "Alice", "score": 0.8}]
        if _EDGE_HINT in cypher:
            return [
                {
                    "src": "Alice",
                    "rel": "KNOWS",
                    "tgt": "Bob",
                    "created_at": 12345,
                    "a_watched": True,
                    "b_watched": False,
                }
            ]
        return []


@pytest.mark.asyncio
async def test_detect_alerts_writes_both_kinds(monkeypatch: pytest.MonkeyPatch) -> None:
    """One edge row + one risk row → one alert of each kind."""
    store = _FakeStore()
    monkeypatch.setattr(ma, "_get_store", lambda: store)

    result = await ma.detect_alerts(MonitorIn(window_days=7, risk_rise_delta=0.1))

    assert result.new_connection_alerts == 1
    assert result.risk_rise_alerts == 1
    assert result.error == ""

    # Verify two MERGE (Alert) calls were recorded on the fake store.
    merge_calls = [cypher for cypher, _ in store.calls if _MERGE_HINT in cypher]
    assert len(merge_calls) == 2, f"Expected 2 MERGE calls, got {len(merge_calls)}"


@pytest.mark.asyncio
async def test_detect_alerts_both_watched_emits_two_connection_alerts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When both endpoints are watched, two new_connection alerts are emitted."""

    class _BothWatched(_FakeStore):
        def structured_query(self, cypher: str, param_map: dict | None = None) -> list[dict]:
            self.calls.append((cypher, param_map or {}))
            if _RISK_HINT in cypher:
                return []
            if _EDGE_HINT in cypher:
                return [
                    {
                        "src": "Alice",
                        "rel": "KNOWS",
                        "tgt": "Bob",
                        "created_at": 12345,
                        "a_watched": True,
                        "b_watched": True,
                    }
                ]
            return []

    store = _BothWatched()
    monkeypatch.setattr(ma, "_get_store", lambda: store)
    result = await ma.detect_alerts(MonitorIn(window_days=7, risk_rise_delta=0.1))
    assert result.new_connection_alerts == 2
    assert result.risk_rise_alerts == 0


@pytest.mark.asyncio
async def test_detect_alerts_failsoft(monkeypatch: pytest.MonkeyPatch) -> None:
    """If _get_store raises, detect_alerts must return error and not propagate."""

    def _boom() -> None:
        raise RuntimeError("neo4j gone")

    monkeypatch.setattr(ma, "_get_store", _boom)
    result = await ma.detect_alerts(MonitorIn())
    assert result.error != ""
    assert "neo4j gone" in result.error
    assert result.new_connection_alerts == 0
    assert result.risk_rise_alerts == 0

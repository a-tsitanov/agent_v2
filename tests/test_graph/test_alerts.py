"""Tests for src/graph/alerts.py — Alert store + watchlist Cypher helpers."""

from src.graph.alerts import alert_key, mark_watched, upsert_alert


class _Rec:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        return []


def test_alert_key_stable_and_dedup_friendly():
    assert alert_key("risk_rise", "Shell", "0.8") == alert_key("risk_rise", "Shell", "0.8")


def test_upsert_alert_merges_on_key():
    s = _Rec()
    upsert_alert(s, kind="risk_rise", entity="Shell", detail="0.8", created_at=19900)
    joined = " ".join(c for c, _ in s.calls)
    assert "MERGE (a:Alert" in joined and "ON CREATE" in joined


def test_upsert_alert_param_map_carries_key():
    s = _Rec()
    upsert_alert(s, kind="risk_rise", entity="Shell", detail="0.8", created_at=19900)
    assert s.calls, "expected at least one store call"
    _, params = s.calls[0]
    assert "key" in params
    assert params["key"] == alert_key("risk_rise", "Shell", "0.8")


def test_mark_watched():
    s = _Rec()
    mark_watched(s, ["A", "B"])
    assert "e.watched" in s.calls[0][0] and s.calls[0][1]["names"] == ["A", "B"]


def test_mark_watched_passes_watched_flag():
    s = _Rec()
    mark_watched(s, ["X"], watched=False)
    _, params = s.calls[0]
    assert params["watched"] is False


def test_mark_watched_empty_is_noop():
    s = _Rec()
    mark_watched(s, [])
    assert s.calls == []

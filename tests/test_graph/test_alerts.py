"""Tests for src/graph/alerts.py — Alert store + watchlist Cypher helpers."""

from src.graph.alerts import alert_key, mark_watched, read_alerts_cypher, upsert_alert


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


# ── Fail-soft (never-raise) contract ─────────────────────────────────────────


class _Boom:
    def structured_query(self, *a, **kw):
        raise RuntimeError("neo4j down")


def test_upsert_alert_is_fail_soft():
    # must not raise — fail-soft per the off-loop contract
    assert upsert_alert(_Boom(), kind="x", entity="y", detail="z", created_at=0) is None


def test_mark_watched_is_fail_soft():
    # must not raise — fail-soft per the off-loop contract
    assert mark_watched(_Boom(), ["A", "B"]) is None


# ── read_alerts_cypher shape ──────────────────────────────────────────────────


def test_read_alerts_cypher_shape():
    assert "ORDER BY a.created_at DESC" in read_alerts_cypher
    for col in ("a.key", "a.kind", "a.entity", "a.detail", "a.created_at", "a.score"):
        assert col in read_alerts_cypher, f"missing column reference: {col}"


# ── scored alerts (one :Alert per entity, score updated in place) ─────────────


def test_upsert_alert_scored_updates_on_match():
    s = _Rec()
    upsert_alert(s, kind="burst", entity="Acme", detail="lawsuit", created_at=100, score=6.0)
    cypher, params = s.calls[0]
    assert "ON MATCH SET" in cypher and "a.score" in cypher
    assert params["score"] == 6.0
    # the volatile score is NOT part of the dedup key → no churn on score drift
    assert params["key"] == "burst:Acme:lawsuit"


def test_upsert_alert_unscored_is_oncreate_only():
    s = _Rec()
    upsert_alert(s, kind="new_connection", entity="Acme", detail="KNOWS:Bob", created_at=100)
    cypher, _ = s.calls[0]
    assert "ON CREATE" in cypher and "ON MATCH" not in cypher

from __future__ import annotations

from src.mcp.tools_server import _polarity_evolution, _topic_trend


async def test_topic_trend_rejects_empty_topic():
    out = await _topic_trend(None, "  ", "month", None, None)
    assert "error" in out


async def test_topic_trend_rejects_bad_granularity():
    out = await _topic_trend(None, "инфляция", "fortnight", None, None)
    assert "error" in out


async def test_topic_trend_rejects_bad_dates():
    out = await _topic_trend(None, "инфляция", "month", "2026/01/01", None)
    assert "error" in out
    assert "YYYY-MM-DD" in out["error"]


async def test_topic_trend_post_filters_by_date_window():
    """`topic_trend` returns the full history; the window is applied to
    the buckets it returns rather than by changing the primitive."""
    async def fake(store, *, topic, granularity):
        from src.analytics.catalog import PrimitiveResult
        return PrimitiveResult(cypher="", params={}, rows=[
            {"period": "2026-01", "mentions": 3},
            {"period": "2026-05", "mentions": 9},
            {"period": "2026-09", "mentions": 1},
        ])

    out = await _topic_trend(
        object(), "инфляция", "month", "2026-02-01", "2026-06-30", _fn=fake,
    )
    assert out["rows"] == [{"period": "2026-05", "mentions": 9}]


async def test_polarity_evolution_requires_a_name_or_rel_type():
    out = await _polarity_evolution(None, None, None)
    assert "error" in out

"""The end-to-end composition MCP-2 `topic_trend` → MCP-3 `stat_align`.

No task owned this seam, and it was broken in both directions: the two
sides disagreed on key names AND on the period format.  Both functions
are pure and callable without FastMCP, so the whole intended agent path
— fetch the channel series, fetch the indicator series, hand both to
the arithmetic — fits in one process with no I/O.

The assertion that matters is negative: no `"error"` key.  A composition
that errors is the honest failure; one that quietly aligns nothing is
not.
"""

from __future__ import annotations

from datetime import date, timedelta

from src.analytics.catalog import PrimitiveResult
from src.mcp.stats_server import _align_tool
from src.mcp.tools_server import _topic_trend


def _fake_primitive(rows: list[dict]):
    async def fake(store, *, topic, granularity):
        return PrimitiveResult(cypher="", params={}, rows=rows)

    return fake


def _indicator_series(starts: list[str], values: list[float]) -> list[dict]:
    """The MCP-3 side: what `stat_series` returns per row."""
    return [
        {"period_start": s, "value": v} for s, v in zip(starts, values, strict=True)
    ]


async def test_topic_trend_rows_feed_stat_align_at_month_granularity():
    """`month` is `topic_trend`'s default, and its `"2026-03"` labels are
    exactly what `date.fromisoformat` used to choke on."""
    periods = [f"2026-{m:02d}" for m in range(1, 11)]
    mentions = [3, 7, 5, 11, 9, 14, 8, 16, 12, 20]
    trend = await _topic_trend(
        object(), "инфляция", "month", None, None,
        _fn=_fake_primitive(
            [{"period": p, "mentions": n} for p, n in zip(periods, mentions, strict=True)]
        ),
    )
    assert "error" not in trend

    indicator = _indicator_series(
        [f"2026-{m:02d}-01" for m in range(1, 11)],
        [50.0, 52.0, 51.0, 55.0, 54.0, 58.0, 53.0, 60.0, 56.0, 63.0],
    )

    out = _align_tool(trend["rows"], indicator, "month", "share", "share", 0)
    assert "error" not in out
    assert isinstance(out["divergence"], float)
    assert len(out["grid"]) == 10
    assert out["gap"].count(None) == 0


async def test_topic_trend_rows_feed_stat_align_at_week_granularity():
    monday = date(2026, 1, 5)
    weeks = [(monday + timedelta(days=7 * i)).isoformat() for i in range(10)]
    mentions = [4, 9, 6, 12, 10, 15, 7, 18, 13, 21]
    trend = await _topic_trend(
        object(), "инфляция", "week", None, None,
        _fn=_fake_primitive(
            [{"period": p, "mentions": n} for p, n in zip(weeks, mentions, strict=True)]
        ),
    )
    assert "error" not in trend

    indicator = _indicator_series(
        weeks, [40.0, 44.0, 41.0, 47.0, 45.0, 50.0, 42.0, 53.0, 48.0, 57.0],
    )

    out = _align_tool(trend["rows"], indicator, "week", "share", "share", 2)
    assert "error" not in out
    assert isinstance(out["divergence"], float)
    assert isinstance(out["correlation"], float)
    assert len(out["grid"]) == 10


async def test_composition_survives_the_windowed_subset():
    """`since`/`until` drop buckets; the rows that survive must still
    carry the keys `stat_align` needs."""
    trend = await _topic_trend(
        object(), "инфляция", "month", "2026-02-01", "2026-04-30",
        _fn=_fake_primitive([
            {"period": "2026-01", "mentions": 3},
            {"period": "2026-02", "mentions": 9},
            {"period": "2026-03", "mentions": 4},
            {"period": "2026-09", "mentions": 1},
        ]),
    )
    assert [r["period_start"] for r in trend["rows"]] == ["2026-02-01", "2026-03-01"]

    out = _align_tool(
        trend["rows"],
        _indicator_series(["2026-02-01", "2026-03-01"], [10.0, 20.0]),
        "month", "share", "share", 0,
    )
    assert "error" not in out
    assert isinstance(out["divergence"], float)

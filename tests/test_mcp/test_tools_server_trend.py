from __future__ import annotations

from datetime import date

from src.mcp.tools_server import (
    _period_bounds,
    _period_in_window,
    _polarity_evolution,
    _topic_trend,
)


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
    assert out["rows"] == [
        {"period": "2026-05", "mentions": 9,
         "period_start": "2026-05-01", "value": 9},
    ]


async def test_polarity_evolution_requires_a_name_or_rel_type():
    out = await _polarity_evolution(None, None, None)
    assert "error" in out


def test_period_bounds_per_granularity():
    assert _period_bounds("2026", "year") == (date(2026, 1, 1), date(2026, 12, 31))
    assert _period_bounds("2026-Q2", "quarter") == (
        date(2026, 4, 1), date(2026, 6, 30),
    )
    assert _period_bounds("2026-02", "month") == (
        date(2026, 2, 1), date(2026, 2, 28),
    )
    assert _period_bounds("2024-02", "month") == (
        date(2024, 2, 1), date(2024, 2, 29),
    )
    assert _period_bounds("2026-05-04", "week") == (
        date(2026, 5, 4), date(2026, 5, 10),
    )
    assert _period_bounds("2026-05-04", "day") == (
        date(2026, 5, 4), date(2026, 5, 4),
    )


def test_period_in_window_keeps_quarters_that_overlap():
    """The bug this replaces: 'Q' sorts after every digit, so a prefix
    compare admitted every quarter under `since` and dropped every
    quarter under `until`."""
    keep = [
        q for q in ("2026-Q1", "2026-Q2", "2026-Q3", "2026-Q4")
        if _period_in_window(q, "quarter", "2026-04-01", "2026-09-30")
    ]
    assert keep == ["2026-Q2", "2026-Q3"]


def test_period_in_window_overlap_not_containment():
    """A bucket straddling the boundary still carries mentions from
    inside the window; dropping it would silently lose them."""
    assert _period_in_window("2026-Q2", "quarter", "2026-06-25", None)
    assert _period_in_window("2026", "year", None, "2026-01-02")


def test_period_in_window_open_bounds():
    assert _period_in_window("2026-05", "month", None, None)
    assert not _period_in_window("2026-05", "month", "2026-07-01", None)
    assert not _period_in_window("2026-05", "month", None, "2026-04-30")


async def test_topic_trend_filters_quarter_buckets():
    async def fake(store, *, topic, granularity):
        from src.analytics.catalog import PrimitiveResult
        return PrimitiveResult(cypher="", params={}, rows=[
            {"period": "2026-Q1", "mentions": 3},
            {"period": "2026-Q2", "mentions": 9},
            {"period": "2026-Q4", "mentions": 1},
        ])

    out = await _topic_trend(
        object(), "инфляция", "quarter", "2026-04-01", "2026-09-30", _fn=fake,
    )
    assert out["rows"] == [
        {"period": "2026-Q2", "mentions": 9,
         "period_start": "2026-04-01", "value": 9},
    ]


async def test_topic_trend_rows_carry_period_start_and_value():
    """The two keys `stat_align` requires.  `period` / `mentions` stay —
    this is additive, so nothing that already read the rows breaks."""
    async def fake(store, *, topic, granularity):
        from src.analytics.catalog import PrimitiveResult
        return PrimitiveResult(cypher="", params={}, rows=[
            {"period": "2024-10", "mentions": 3},
            {"period": "2024-11", "mentions": 9},
        ])

    out = await _topic_trend(object(), "инфляция", "month", None, None, _fn=fake)
    assert out["rows"] == [
        {"period": "2024-10", "mentions": 3,
         "period_start": "2024-10-01", "value": 3},
        {"period": "2024-11", "mentions": 9,
         "period_start": "2024-11-01", "value": 9},
    ]


async def test_topic_trend_period_start_is_the_bucket_first_day_per_granularity():
    def _fake(period: str):
        async def fake(store, *, topic, granularity):
            from src.analytics.catalog import PrimitiveResult
            return PrimitiveResult(
                cypher="", params={}, rows=[{"period": period, "mentions": 1}],
            )

        return fake

    for granularity, period, expected in (
        ("year", "2026", "2026-01-01"),
        ("quarter", "2026-Q3", "2026-07-01"),
        ("month", "2026-02", "2026-02-01"),
        ("week", "2026-05-04", "2026-05-04"),
        ("day", "2026-05-06", "2026-05-06"),
    ):
        out = await _topic_trend(
            object(), "инфляция", granularity, None, None, _fn=_fake(period),
        )
        assert out["rows"][0]["period_start"] == expected, granularity
        # Whatever the label format, the result parses as a plain ISO date —
        # that is the contract `stat_align` actually depends on.
        assert date.fromisoformat(out["rows"][0]["period_start"])


async def test_topic_trend_errors_instead_of_returning_a_silent_zero_on_nebula(
    monkeypatch,
):
    """Under the production backend the primitive's parameterised query
    fail-softs to `[]`, which reads as "never mentioned".  A missing
    answer is honest; a silent zero is not."""
    from src.config import settings

    monkeypatch.setattr(settings.graph, "backend", "nebula")
    out = await _topic_trend(object(), "инфляция", "month", None, None)
    assert "error" in out
    assert "nebula" in out["error"]
    assert "rows" not in out


async def test_topic_trend_is_not_guarded_on_neo4j(monkeypatch):
    from src.config import settings

    monkeypatch.setattr(settings.graph, "backend", "neo4j")

    async def fake(store, *, topic, granularity):
        from src.analytics.catalog import PrimitiveResult
        return PrimitiveResult(cypher="", params={}, rows=[
            {"period": "2026-01", "mentions": 3},
        ])

    out = await _topic_trend(object(), "инфляция", "month", None, None, _fn=fake)
    assert "error" not in out
    assert out["rows"][0]["mentions"] == 3


async def test_polarity_evolution_is_not_guarded_on_nebula(monkeypatch):
    """It does NOT share topic_trend's fault: it goes through
    `build_dynamics_graph_ops`, whose nebula implementation inlines its
    literals into nGQL and never passes a `param_map`.  Guarding it too
    would suppress a working tool."""
    from src.config import settings

    monkeypatch.setattr(settings.graph, "backend", "nebula")

    async def fake(store, *, name, rel_type):
        from src.analytics.catalog import PrimitiveResult
        return PrimitiveResult(cypher="", params={}, rows=[
            {"period": "2026-01", "polarity": "affirmed", "n": 4},
        ])

    out = await _polarity_evolution(object(), "инфляция", None, _fn=fake)
    assert "error" not in out
    assert out["rows"] == [{"period": "2026-01", "polarity": "affirmed", "n": 4}]


async def test_topic_trend_argument_checks_run_before_the_store_check():
    """With a real store present, a bad argument must still be reported
    as a bad argument — the previous tests passed a None store, so they
    could not tell the two branches apart."""
    out = await _topic_trend(object(), "  ", "month", None, None)
    assert "topic" in out["error"]
    out = await _topic_trend(object(), "инфляция", "fortnight", None, None)
    assert "granularity" in out["error"]
    out = await _polarity_evolution(object(), None, None)
    assert "rel_type" in out["error"]

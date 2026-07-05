"""Table-driven tests for the deterministic event-time resolver.

Anchor below = 2026-07-05 (epoch day 20639): date(2026,7,5) - date(1970,1,1) = 20639 days.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.graph.event_ts_resolver import resolve

ANCHOR_DAYS = 20639  # 2026-07-05


def _utc(y, m, d, hh=0, mm=0, ss=0) -> int:
    return int(datetime(y, m, d, hh, mm, ss, tzinfo=UTC).timestamp())


# ── resolvable: explicit dates ───────────────────────────────────────

def test_explicit_dmy_date():
    assert resolve("1 марта 2024", ANCHOR_DAYS) == (_utc(2024, 3, 1), _utc(2024, 3, 1, 23, 59, 59), "day")


def test_iso_day():
    assert resolve("2024-07-06", ANCHOR_DAYS) == (_utc(2024, 7, 6), _utc(2024, 7, 6, 23, 59, 59), "day")


def test_iso_month():
    assert resolve("2024-03", ANCHOR_DAYS) == (_utc(2024, 3, 1), _utc(2024, 3, 31, 23, 59, 59), "month")


def test_iso_range():
    assert resolve("2026-01-01..2026-04-30", ANCHOR_DAYS) == (
        _utc(2026, 1, 1), _utc(2026, 4, 30, 23, 59, 59), "day")


# ── resolvable: relative to document date ────────────────────────────

def test_yesterday():
    assert resolve("вчера", ANCHOR_DAYS) == (_utc(2026, 7, 4), _utc(2026, 7, 4, 23, 59, 59), "day")


def test_yearless_day_month_resolves_nearest_to_anchor():
    # 2026-07-06 is 1 day from the anchor; 2025-07-06 is 364 days away.
    assert resolve("6 июля", ANCHOR_DAYS) == (_utc(2026, 7, 6), _utc(2026, 7, 6, 23, 59, 59), "day")


def test_bare_month_uses_anchor_year():
    assert resolve("в марте", ANCHOR_DAYS) == (_utc(2026, 3, 1), _utc(2026, 3, 31, 23, 59, 59), "month")


# ── resolvable: intervals ────────────────────────────────────────────

def test_bare_year():
    assert resolve("2023", ANCHOR_DAYS) == (_utc(2023, 1, 1), _utc(2023, 12, 31, 23, 59, 59), "year")


def test_bare_year_implausible_clamped_to_none():
    # Bare 4-digit numbers outside a plausible calendar-year range must not be
    # treated as years (avoids e.g. "1200" → year 1200 with a negative epoch).
    assert resolve("1200", ANCHOR_DAYS) is None
    assert resolve("3000", ANCHOR_DAYS) is None
    assert resolve("2023", ANCHOR_DAYS) is not None


def test_year_range_with_word():
    assert resolve("2026–2027 годы", ANCHOR_DAYS) == (_utc(2026, 1, 1), _utc(2027, 12, 31, 23, 59, 59), "year")


def test_day_span_in_month():
    assert resolve("1-5 июля", ANCHOR_DAYS) == (_utc(2026, 7, 1), _utc(2026, 7, 5, 23, 59, 59), "day")


def test_first_half_year():
    assert resolve("первое полугодие", ANCHOR_DAYS) == (_utc(2026, 1, 1), _utc(2026, 6, 30, 23, 59, 59), "month")


def test_intraday_span_with_day():
    assert resolve("6 июля с 12:00 до 18:00 мск", ANCHOR_DAYS) == (
        _utc(2026, 7, 6, 12, 0), _utc(2026, 7, 6, 18, 0), "datetime")


# ── Fix D: day-of-month with unknown month («N числа») ───────────────


def test_n_chisla_resolves_nearest_month():
    # Anchor is 2026-07-05; "7 числа" is 2 days away in July vs. ~28/33
    # days away in June/August -- nearest wins.
    assert resolve("7 числа", ANCHOR_DAYS) == (
        _utc(2026, 7, 7), _utc(2026, 7, 7, 23, 59, 59), "day")


def test_n_chisla_with_go_suffix_resolves_same():
    assert resolve("7-го числа", ANCHOR_DAYS) == (
        _utc(2026, 7, 7), _utc(2026, 7, 7, 23, 59, 59), "day")


def test_n_chisla_none_without_anchor():
    assert resolve("7 числа", None) is None
    assert resolve("7-го числа", None) is None


# ── Fix D: «в течение дня» — anchor day itself ────────────────────────


def test_v_techenie_dnya_resolves_to_anchor_day():
    assert resolve("в течение дня", ANCHOR_DAYS) == (
        _utc(2026, 7, 5), _utc(2026, 7, 5, 23, 59, 59), "day")


def test_v_techenie_dnya_none_without_anchor():
    assert resolve("в течение дня", None) is None


# ── unresolvable ⇒ None, never an invention ──────────────────────────

@pytest.mark.parametrize("garbage", [
    None, "", "2024-XX", "2024-XX..2025-XX", "20XX-MM-DD", "..2024",
    "после праздников", "листья", "Константиновка",
    "Упоминается роль Норвегии как крупнейшего донора в программе НАТО PURL.",
])
def test_unresolvable_returns_none(garbage):
    assert resolve(garbage, ANCHOR_DAYS) is None


def test_no_anchor_still_resolves_absolute_dates():
    assert resolve("2024-07-06", None) == (_utc(2024, 7, 6), _utc(2024, 7, 6, 23, 59, 59), "day")


def test_no_anchor_relative_returns_none():
    assert resolve("вчера", None) is None


def test_never_raises_on_weird_input():
    assert resolve("6 " * 30, ANCHOR_DAYS) is None  # gibberish — rejected by dateparser fallback (59 chars)
    assert resolve("6 " * 40, ANCHOR_DAYS) is None  # 79 chars — length fast-path
    assert resolve("99 микабря 20260", ANCHOR_DAYS) is None

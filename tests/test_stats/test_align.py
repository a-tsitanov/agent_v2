"""Tests for `src.stats.align` bucketing / resampling / normalisation.

Everything here is deterministic, so assertions are exact equality —
that is the whole point of keeping exact statistics out of the
semantic contour.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.stats.align import MIN_OVERLAP, align, pearson, period_key, resample, zscore


def test_period_key_week_starts_monday():
    # 2026-08-07 is a Friday; its week bucket is Monday 2026-08-03.
    assert period_key(date(2026, 8, 7), "week") == date(2026, 8, 3)
    assert period_key(date(2026, 8, 3), "week") == date(2026, 8, 3)


def test_period_key_month_quarter_year():
    d = date(2026, 8, 7)
    assert period_key(d, "day") == d
    assert period_key(d, "month") == date(2026, 8, 1)
    assert period_key(d, "quarter") == date(2026, 7, 1)
    assert period_key(d, "year") == date(2026, 1, 1)


def test_period_key_rejects_unknown_granularity():
    with pytest.raises(ValueError, match="granularity"):
        period_key(date(2026, 8, 7), "fortnight")


def test_resample_share_averages_within_bucket():
    points = [
        (date(2026, 8, 3), 10.0),
        (date(2026, 8, 5), 20.0),
        (date(2026, 8, 10), 7.0),
    ]
    assert resample(points, granularity="week", value_kind="share") == [
        (date(2026, 8, 3), 15.0),
        (date(2026, 8, 10), 7.0),
    ]


def test_resample_level_takes_last_point_in_bucket():
    points = [
        (date(2026, 8, 5), 20.0),
        (date(2026, 8, 3), 10.0),
    ]
    # Sorted by date inside the bucket, so 08-05 wins regardless of input order.
    assert resample(points, granularity="week", value_kind="level") == [
        (date(2026, 8, 3), 20.0),
    ]


def test_resample_never_invents_buckets():
    """Monthly points asked for at weekly granularity must NOT be
    interpolated upward — only the weeks that actually contain a point
    appear."""
    points = [(date(2026, 6, 1), 1.0), (date(2026, 7, 1), 2.0)]
    out = resample(points, granularity="week", value_kind="level")
    assert out == [(date(2026, 6, 1), 1.0), (date(2026, 6, 29), 2.0)]
    assert len(out) == 2


def test_resample_rejects_unknown_granularity():
    with pytest.raises(ValueError, match="granularity"):
        resample([(date(2026, 8, 3), 1.0)], granularity="fortnight", value_kind="share")


def test_resample_rejects_unknown_granularity_with_empty_points():
    """Empty points must not mask granularity validation (Finding 1).

    Without eager validation, this silently returns [] instead of raising.
    """
    with pytest.raises(ValueError, match="granularity"):
        resample([], granularity="fortnight", value_kind="share")


def test_resample_rejects_unknown_value_kind():
    with pytest.raises(ValueError, match="value_kind"):
        resample([(date(2026, 8, 3), 1.0)], granularity="week", value_kind="ratio")


def test_zscore_centres_and_scales():
    out = zscore([1.0, 2.0, 3.0])
    assert out[0] == pytest.approx(-1.2247448713915889)
    assert out[1] == pytest.approx(0.0)
    assert out[2] == pytest.approx(1.2247448713915889)


def test_zscore_zero_variance_returns_zeros_not_nan():
    assert zscore([5.0, 5.0, 5.0]) == [0.0, 0.0, 0.0]


def test_zscore_empty():
    assert zscore([]) == []


def _weekly(start: date, values: list[float]) -> list[tuple[date, float]]:
    """Consecutive Monday-anchored weekly points starting at `start`."""
    return [
        (date.fromordinal(start.toordinal() + 7 * i), v)
        for i, v in enumerate(values)
    ]


def test_pearson_perfect_positive():
    assert pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)


def test_pearson_constant_series_is_none():
    assert pearson([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) is None


def test_align_identical_series_have_zero_divergence():
    start = date(2026, 1, 5)
    vals = [1.0, 3.0, 2.0, 6.0, 4.0, 9.0, 5.0, 11.0]
    res = align(
        _weekly(start, vals), _weekly(start, vals),
        granularity="week", value_kind_a="share", value_kind_b="share",
    )
    assert res.divergence == pytest.approx(0.0)
    assert res.correlation == pytest.approx(1.0)
    assert res.warnings == []
    assert len(res.grid) == 8


def test_align_different_units_still_comparable_after_normalisation():
    """A percentage and a ruble amount moving together must normalise
    to the same shape."""
    start = date(2026, 1, 5)
    pct = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    rub = [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0, 7000.0, 8000.0]
    res = align(
        _weekly(start, pct), _weekly(start, rub),
        granularity="week", value_kind_a="share", value_kind_b="level",
    )
    assert res.divergence == pytest.approx(0.0)
    assert res.correlation == pytest.approx(1.0)


def test_align_marks_sparsity_when_one_side_is_coarser():
    start = date(2026, 1, 5)
    dense = _weekly(start, [1.0, 2.0, 3.0, 4.0])
    sparse = [(start, 1.0), (date.fromordinal(start.toordinal() + 21), 4.0)]
    res = align(
        dense, sparse,
        granularity="week", value_kind_a="share", value_kind_b="share",
    )
    assert "sparse:b" in res.warnings
    assert res.b[1] is None
    assert res.gap[1] is None


def test_align_low_overlap_suppresses_correlation():
    start = date(2026, 1, 5)
    short = _weekly(start, [1.0, 2.0, 3.0])
    res = align(
        short, short,
        granularity="week", value_kind_a="share", value_kind_b="share",
    )
    assert res.correlation is None
    assert f"low_overlap:3<{MIN_OVERLAP}" in res.warnings


def test_align_finds_the_lag_that_best_fits():
    """b repeats a one week later; searching lags must recover +1
    rather than reporting a spurious divergence."""
    start = date(2026, 1, 5)
    a_vals = [1.0, 5.0, 2.0, 8.0, 3.0, 9.0, 4.0, 7.0, 6.0]
    a = _weekly(start, a_vals)
    b = _weekly(date.fromordinal(start.toordinal() + 7), a_vals)
    res = align(
        a, b, granularity="week",
        value_kind_a="share", value_kind_b="share", max_lag=2,
    )
    assert res.best_lag == 1
    assert res.correlation == pytest.approx(1.0)


def test_align_no_overlap_returns_none_divergence():
    a = _weekly(date(2026, 1, 5), [1.0, 2.0])
    b = _weekly(date(2026, 6, 1), [1.0, 2.0])
    res = align(
        a, b, granularity="week",
        value_kind_a="share", value_kind_b="share",
    )
    assert res.divergence is None
    assert res.correlation is None
    assert "no_overlap" in res.warnings


def test_align_rejects_unknown_granularity():
    with pytest.raises(ValueError, match="granularity"):
        align([], [], granularity="fortnight",
              value_kind_a="share", value_kind_b="share")

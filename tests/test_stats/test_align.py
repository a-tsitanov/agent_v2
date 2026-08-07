"""Tests for `src.stats.align` bucketing / resampling / normalisation.

Everything here is deterministic, so assertions are exact equality —
that is the whole point of keeping exact statistics out of the
semantic contour.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.stats.align import period_key, resample, zscore


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

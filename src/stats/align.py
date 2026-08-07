"""Deterministic series alignment for cross-source comparison.

Pure functions only — no I/O, no LLM, no clock.  Exact statistics get
their own contour precisely so this code can be tested by exact
equality, and so an agent is never asked to do the arithmetic.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date

GRANULARITIES = frozenset({"day", "week", "month", "quarter", "year"})
VALUE_KINDS = frozenset({"share", "level", "rate", "index"})

# `share`/`rate`/`index` are averaged within a bucket; a `level` is a
# stock, so the last observation in the bucket represents it.
_MEAN_KINDS = frozenset({"share", "rate", "index"})
_LAST_KINDS = frozenset({"level"})


def period_key(d: date, granularity: str) -> date:
    """Start of the bucket containing ``d``.  Weeks start Monday."""
    if granularity == "day":
        return d
    if granularity == "week":
        return date.fromordinal(d.toordinal() - d.weekday())
    if granularity == "month":
        return date(d.year, d.month, 1)
    if granularity == "quarter":
        return date(d.year, 3 * ((d.month - 1) // 3) + 1, 1)
    if granularity == "year":
        return date(d.year, 1, 1)
    raise ValueError(f"unknown granularity {granularity!r}")


def resample(
    points: Iterable[tuple[date, float]],
    *,
    granularity: str,
    value_kind: str,
) -> list[tuple[date, float]]:
    """Down-aggregate ``points`` onto ``granularity``.

    NEVER interpolates upward: a bucket with no observation is absent
    from the result rather than filled.  Callers detect that as
    sparsity and warn — silently inventing values would fabricate the
    very numbers this subsystem exists to keep exact.
    """
    if value_kind not in VALUE_KINDS:
        raise ValueError(f"unknown value_kind {value_kind!r}")
    buckets: dict[date, list[tuple[date, float]]] = {}
    for d, v in points:
        buckets.setdefault(period_key(d, granularity), []).append((d, float(v)))
    out: list[tuple[date, float]] = []
    for key in sorted(buckets):
        items = sorted(buckets[key])
        if value_kind in _LAST_KINDS:
            out.append((key, items[-1][1]))
        else:
            out.append((key, sum(v for _, v in items) / len(items)))
    return out


def zscore(values: Sequence[float]) -> list[float]:
    """Z-score ``values`` (population sd).

    Zero variance returns zeros rather than NaN: a flat series is
    perfectly ordinary input, and NaN would poison every downstream
    correlation.
    """
    n = len(values)
    if n == 0:
        return []
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    if var == 0.0:
        return [0.0] * n
    sd = var**0.5
    return [(v - mean) / sd for v in values]

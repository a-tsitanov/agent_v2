"""Deterministic series alignment for cross-source comparison.

Pure functions only — no I/O, no LLM, no clock.  Exact statistics get
their own contour precisely so this code can be tested by exact
equality, and so an agent is never asked to do the arithmetic.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date

GRANULARITIES = frozenset({"day", "week", "month", "quarter", "year"})
VALUE_KINDS = frozenset({"share", "level", "rate", "index"})

# `share`/`rate`/`index` are averaged within a bucket; a `level` is a
# stock, so the last observation in the bucket represents it.
_MEAN_KINDS = frozenset({"share", "rate", "index"})
_LAST_KINDS = frozenset({"level"})


def period_key(d: date, granularity: str) -> date:
    """Start of the bucket containing ``d``.  Weeks start Monday."""
    if granularity not in GRANULARITIES:
        raise ValueError(f"unknown granularity {granularity!r}")
    if granularity == "day":
        return d
    if granularity == "week":
        return date.fromordinal(d.toordinal() - d.weekday())
    if granularity == "month":
        return date(d.year, d.month, 1)
    if granularity == "quarter":
        return date(d.year, 3 * ((d.month - 1) // 3) + 1, 1)
    return date(d.year, 1, 1)


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
    if granularity not in GRANULARITIES:
        raise ValueError(f"unknown granularity {granularity!r}")
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


# Below eight overlapping periods a correlation is noise dressed as a
# finding, so it is withheld rather than reported with a caveat.
MIN_OVERLAP = 8


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Pearson correlation, or None when undefined (n < 2, or either
    side constant)."""
    n = len(xs)
    if n < 2 or n != len(ys):
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0.0 or dy == 0.0:
        return None
    return num / (dx * dy)


@dataclass(frozen=True)
class AlignedResult:
    grid: list[date]
    a: list[float | None]
    b: list[float | None]
    a_norm: list[float | None]
    b_norm: list[float | None]
    gap: list[float | None]
    divergence: float | None
    best_lag: int
    correlation: float | None
    warnings: list[str] = field(default_factory=list)


def _project(
    series: list[tuple[date, float]], grid: list[date],
) -> list[float | None]:
    lookup = dict(series)
    return [lookup.get(k) for k in grid]


def _normalise(values: list[float | None]) -> list[float | None]:
    present = [v for v in values if v is not None]
    scaled = zscore(present)
    it = iter(scaled)
    return [None if v is None else next(it) for v in values]


def align(
    series_a: Iterable[tuple[date, float]],
    series_b: Iterable[tuple[date, float]],
    *,
    granularity: str,
    value_kind_a: str,
    value_kind_b: str,
    max_lag: int = 0,
    min_overlap: int = MIN_OVERLAP,
) -> AlignedResult:
    """Put two series on a common grid and describe how far apart they run.

    ``max_lag`` searches shifts of ``series_b`` in
    ``[-max_lag, +max_lag]`` grid steps and keeps the one with the
    highest signed correlation: polls describe what already happened
    while channels react earlier, so without a lag search a pure timing
    offset reads as divergence.
    """
    if granularity not in GRANULARITIES:
        raise ValueError(f"unknown granularity {granularity!r}")
    if max_lag < 0:
        raise ValueError("max_lag must be >= 0")

    ra = resample(series_a, granularity=granularity, value_kind=value_kind_a)
    rb = resample(series_b, granularity=granularity, value_kind=value_kind_b)

    grid = sorted({k for k, _ in ra} | {k for k, _ in rb})
    a_vals = _project(ra, grid)
    b_vals = _project(rb, grid)
    a_norm = _normalise(a_vals)
    b_norm = _normalise(b_vals)

    warnings: list[str] = []
    if any(v is None for v in a_vals):
        warnings.append("sparse:a")
    if any(v is None for v in b_vals):
        warnings.append("sparse:b")
    if value_kind_a != value_kind_b:
        warnings.append(f"value_kind_mismatch:{value_kind_a}/{value_kind_b}")

    best_lag = 0
    best_corr: float | None = None
    for lag in range(-max_lag, max_lag + 1):
        xs: list[float] = []
        ys: list[float] = []
        for i in range(len(grid)):
            j = i + lag
            if 0 <= j < len(grid) and a_norm[i] is not None and b_norm[j] is not None:
                xs.append(a_norm[i])
                ys.append(b_norm[j])
        corr = pearson(xs, ys) if len(xs) >= min_overlap else None
        if corr is not None and (best_corr is None or corr > best_corr):
            best_corr, best_lag = corr, lag

    gap: list[float | None] = []
    for i in range(len(grid)):
        j = i + best_lag
        if 0 <= j < len(grid) and a_norm[i] is not None and b_norm[j] is not None:
            gap.append(a_norm[i] - b_norm[j])
        else:
            gap.append(None)

    present_gap = [g for g in gap if g is not None]
    if not present_gap:
        warnings.append("no_overlap")
        divergence = None
    else:
        divergence = sum(abs(g) for g in present_gap) / len(present_gap)
        if len(present_gap) < min_overlap:
            warnings.append(f"low_overlap:{len(present_gap)}<{min_overlap}")

    return AlignedResult(
        grid=grid, a=a_vals, b=b_vals, a_norm=a_norm, b_norm=b_norm,
        gap=gap, divergence=divergence, best_lag=best_lag,
        correlation=best_corr, warnings=warnings,
    )

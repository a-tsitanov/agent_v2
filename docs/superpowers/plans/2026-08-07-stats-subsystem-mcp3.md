# Statistics Subsystem (MCP-3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an exact-statistics subsystem — its own Postgres schema, its own MCP server, and deterministic alignment math — so an agent can compare Telegram-channel attention against external poll/statistical series.

**Architecture:** Responsibility is split by *guarantee*, not by feature. Exact numbers live in `stat_indicator` / `stat_observation`, are served by a new MCP-3 server that never calls an LLM, and are joined to the semantic side by the **agent** — which is possible because MCP-2 already establishes the atomic-tool model. The comparison itself is a pure function (`stat_align`) that takes both series as arguments and reads nothing, so the boundary stays clean and the arithmetic is never done by a model.

**Tech Stack:** Python 3.12, psycopg 3 + `psycopg_pool`, FastMCP, pydantic-settings, pytest (`asyncio_mode = "auto"`).

**Spec:** `docs/superpowers/specs/2026-08-07-stats-subsystem-design.md`

## Global Constraints

- Nothing in `src/stats/` or `src/mcp/stats_server.py` may call an LLM, read the graph, or read Milvus.
- All external links are weak: `stat_indicator.entity_vid` and `stat_observation.source_doc_id` are plain columns, **never** foreign keys to graph or `documents`.
- One query layer: every surface reads through `StatsRepository`, mirroring the rule `_stats_by` follows for `/api/v1/stats` (`src/mcp/tools_server.py:402`).
- MCP-3 tool timeout is **120 s** (`@mcp.tool(timeout=120)`), matching `channel_message_stats`, not the 1800 s used by retrieval tools.
- MCP tools return `{"error": "..."}` on bad input; they never raise.
- Ruff: `line-length = 100`, target `py312`, `from __future__ import annotations` at the top of every new module.
- Raw values are stored as loaded. Alignment and normalisation are computed on read — never persisted.
- Do not modify `/api/v1/search/*`, `SearchOrchestratorWorkflow`, or the synthesis step.
- Do not register anything in the analytics `CATALOG`; the `prim.fn(store, …)` contract (`src/workflow/analytics/activities.py:61`) stays untouched.

---

## File Structure

**Create:**
- `src/stats/__init__.py` — empty package marker.
- `src/stats/align.py` — pure alignment math. No imports from `src.storage`, `src.graph`, `src.retrieval`.
- `src/storage/stats.py` — `StatsRepository` + pure SQL builders.
- `src/mcp/stats_server.py` — MCP-3 server: thin validating helpers + tool wrappers.
- `src/api/routes/stats_data.py` — the write path: `POST /api/v1/statistics/load`.
- `tests/test_stats/test_align.py`, `tests/test_stats/test_import.py`
- `tests/test_storage/test_stats_repository.py`
- `tests/test_mcp/test_stats_server.py`

**Modify:**
- `scripts/setup_db.py` — `pg_trgm` extension, two tables, four indexes.
- `src/config.py` — `StatsSettings` + `Settings.stats` cached_property.
- `src/mcp/tools_server.py` — expose `topic_trend` / `polarity_evolution`.
- `docs/runbook/mcp.md`, `README.md`, `docs/FEATURES.md` — three servers, not two.

---

### Task 1: Alignment primitives (bucketing, resampling, z-score)

The hardest logic, and it has zero dependencies — build it first so everything downstream can assume it works.

**Files:**
- Create: `src/stats/__init__.py`, `src/stats/align.py`
- Test: `tests/test_stats/test_align.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `period_key(d: date, granularity: str) -> date`
  - `resample(points: Iterable[tuple[date, float]], *, granularity: str, value_kind: str) -> list[tuple[date, float]]`
  - `zscore(values: Sequence[float]) -> list[float]`
  - Module constants `GRANULARITIES: frozenset[str]`, `VALUE_KINDS: frozenset[str]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_stats/__init__.py` (empty) and `tests/test_stats/test_align.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_stats/test_align.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.stats'`

- [ ] **Step 3: Write the implementation**

Create `src/stats/__init__.py` as an empty file, then `src/stats/align.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_stats/test_align.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/stats tests/test_stats`
Expected: no findings.

- [ ] **Step 6: Commit**

```bash
git add src/stats tests/test_stats
git commit -m "feat(stats): add pure bucketing, resampling and z-score primitives"
```

---

### Task 2: `align()` — gap, lag search, divergence, warnings

**Files:**
- Modify: `src/stats/align.py`
- Test: `tests/test_stats/test_align.py`

**Interfaces:**
- Consumes: `period_key`, `resample`, `zscore` from Task 1.
- Produces:
  - `pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None`
  - `AlignedResult` frozen dataclass with fields `grid: list[date]`, `a: list[float | None]`, `b: list[float | None]`, `a_norm: list[float | None]`, `b_norm: list[float | None]`, `gap: list[float | None]`, `divergence: float | None`, `best_lag: int`, `correlation: float | None`, `warnings: list[str]`
  - `align(series_a, series_b, *, granularity, value_kind_a, value_kind_b, max_lag=0, min_overlap=MIN_OVERLAP) -> AlignedResult`
  - `MIN_OVERLAP: int = 8`

- [ ] **Step 1: Write the failing tests**

First extend the existing import at the top of `tests/test_stats/test_align.py`
— do not add a second import block lower down, ruff `E402` rejects it:

```python
from src.stats.align import MIN_OVERLAP, align, pearson, period_key, resample, zscore
```

Then append the tests:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_stats/test_align.py -v`
Expected: FAIL — `ImportError: cannot import name 'align' from 'src.stats.align'`

- [ ] **Step 3: Write the implementation**

First extend the top-level import block of `src/stats/align.py` — a mid-file
import trips ruff `E402`:

```python
from dataclasses import dataclass, field
```

Then append:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_stats/test_align.py -v`
Expected: PASS (18 tests)

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/stats tests/test_stats`

- [ ] **Step 6: Commit**

```bash
git add src/stats/align.py tests/test_stats/test_align.py
git commit -m "feat(stats): add align() with lag search, gap and divergence"
```

---

### Task 3: Schema and settings

**Files:**
- Modify: `scripts/setup_db.py`, `src/config.py`
- Test: `tests/test_config/test_settings.py` (append), `tests/test_scripts/test_setup_db_stats.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `scripts/setup_db.py` module constants `_STAT_INDICATOR_DDL: str`, `_STAT_OBSERVATION_DDL: str`, `_STAT_INDEXES_DDL: str`, `_PG_TRGM_DDL: str`
  - `src/config.py` class `StatsSettings` (env prefix `STATS_`) with `default_granularity: str = "week"`, `default_max_lag: int = 4`, `search_limit: int = 20`, `min_overlap: int = 8`; reachable as `settings.stats`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_scripts/test_setup_db_stats.py`:

```python
"""The stats DDL is asserted as text, not by hitting a live Postgres —
the point is that the constraints which make the schema correct are
actually present."""

from __future__ import annotations

from scripts.setup_db import (
    _PG_TRGM_DDL,
    _STAT_INDEXES_DDL,
    _STAT_INDICATOR_DDL,
    _STAT_OBSERVATION_DDL,
)


def test_tables_are_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS stat_indicator" in _STAT_INDICATOR_DDL
    assert "CREATE TABLE IF NOT EXISTS stat_observation" in _STAT_OBSERVATION_DDL


def test_indicator_is_unique_per_source_and_code():
    assert "UNIQUE (source, code)" in _STAT_INDICATOR_DDL


def test_indicator_constrains_value_kind_and_granularity():
    assert "value_kind IN ('share','level','rate','index')" in _STAT_INDICATOR_DDL
    assert (
        "granularity IN ('day','week','month','quarter','year')"
        in _STAT_INDICATOR_DDL
    )


def test_dims_is_not_null_with_empty_default():
    """A nullable `dims` would silently break the UNIQUE constraint,
    because NULL never compares equal to NULL — duplicate undimensioned
    observations would slip in."""
    assert "dims           JSONB   NOT NULL DEFAULT '{}'::jsonb" in _STAT_OBSERVATION_DDL


def test_observation_unique_key_includes_revision():
    assert (
        "UNIQUE (indicator_id, period_start, dims, revision)"
        in _STAT_OBSERVATION_DDL
    )


def test_trigram_extension_and_indexes_present():
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in _PG_TRGM_DDL
    assert "gin_trgm_ops" in _STAT_INDEXES_DDL
    assert "USING GIN (dims)" in _STAT_INDEXES_DDL
```

Append to `tests/test_config/test_settings.py`:

```python
def test_stats_settings_defaults():
    from src.config import settings

    assert settings.stats.default_granularity == "week"
    assert settings.stats.default_max_lag == 4
    assert settings.stats.search_limit == 20
    assert settings.stats.min_overlap == 8
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scripts/test_setup_db_stats.py tests/test_config/test_settings.py -v`
Expected: FAIL — `ImportError: cannot import name '_STAT_INDICATOR_DDL'`

- [ ] **Step 3: Add the DDL**

In `scripts/setup_db.py`, after the `ingest_metrics` DDL block, add:

```python
_PG_TRGM_DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

_STAT_INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS stat_indicator (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         TEXT    NOT NULL,
    code           TEXT    NOT NULL,
    title          TEXT    NOT NULL,
    question_text  TEXT    NOT NULL DEFAULT '',
    unit           TEXT    NOT NULL,
    value_kind     TEXT    NOT NULL,
    granularity    TEXT    NOT NULL,
    dims_schema    JSONB   NOT NULL DEFAULT '{}'::jsonb,
    entity_vid     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, code),
    CONSTRAINT stat_indicator_value_kind_check
        CHECK (value_kind IN ('share','level','rate','index')),
    CONSTRAINT stat_indicator_granularity_check
        CHECK (granularity IN ('day','week','month','quarter','year'))
);
"""

# `entity_vid` and `source_doc_id` are WEAK links on purpose — no
# foreign keys — so a graph rebuild or a document re-ingest can never
# invalidate a stored number.
_STAT_OBSERVATION_DDL = """
CREATE TABLE IF NOT EXISTS stat_observation (
    indicator_id   BIGINT  NOT NULL REFERENCES stat_indicator(id) ON DELETE CASCADE,
    period_start   DATE    NOT NULL,
    period_end     DATE    NOT NULL,
    dims           JSONB   NOT NULL DEFAULT '{}'::jsonb,
    value          NUMERIC NOT NULL,
    sample_n       INTEGER,
    revision       INTEGER NOT NULL DEFAULT 0,
    source_doc_id  UUID,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (indicator_id, period_start, dims, revision)
);
"""

_STAT_INDEXES_DDL = """
CREATE INDEX IF NOT EXISTS stat_observation_series_idx
    ON stat_observation (indicator_id, period_start);
CREATE INDEX IF NOT EXISTS stat_observation_dims_idx
    ON stat_observation USING GIN (dims);
CREATE INDEX IF NOT EXISTS stat_indicator_title_trgm_idx
    ON stat_indicator USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS stat_indicator_question_trgm_idx
    ON stat_indicator USING GIN (question_text gin_trgm_ops);
"""
```

Then execute them where the existing DDL is executed — find the function in `scripts/setup_db.py` that runs `_DOCUMENTS_DDL` and add the four new statements in this order (extension first, indicator before observation because of the `REFERENCES`, indexes last).

- [ ] **Step 4: Add the settings block**

In `src/config.py`, next to the other `BaseSettings` subclasses:

```python
class StatsSettings(BaseSettings):
    """External-statistics subsystem (MCP-3).

    Defaults are read-side only — nothing here changes what is stored.
    """

    model_config = SettingsConfigDict(
        env_prefix="STATS_", env_file=".env", extra="ignore",
    )

    default_granularity: str = "week"
    default_max_lag: int = 4
    search_limit: int = 20
    min_overlap: int = 8
```

And in `Settings` (`src/config.py:1163`), alongside the other cached properties:

```python
    @cached_property
    def stats(self) -> StatsSettings:
        return StatsSettings()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_scripts/test_setup_db_stats.py tests/test_config/test_settings.py -v`
Expected: PASS

- [ ] **Step 6: Apply the schema against a live database**

Run: `uv run python -m scripts.setup_db`
Expected: exits 0; re-running it a second time also exits 0 (idempotency is the requirement, not a nice-to-have).

- [ ] **Step 7: Commit**

```bash
git add scripts/setup_db.py src/config.py tests/test_scripts/test_setup_db_stats.py tests/test_config/test_settings.py
git commit -m "feat(stats): add stat_indicator/stat_observation schema and StatsSettings"
```

---

### Task 4: `StatsRepository`

**Files:**
- Create: `src/storage/stats.py`
- Test: `tests/test_storage/test_stats_repository.py`

**Interfaces:**
- Consumes: `get_pg_pool()` from `src/storage/pg_pool.py`; `GRANULARITIES`/`VALUE_KINDS` from `src/stats/align.py`.
- Produces:
  - `build_series_query(indicator_id: int, since: date | None, until: date | None, dims: dict | None, revision: int | None) -> tuple[str, list]`
  - `build_search_query(query: str, source: str | None, limit: int) -> tuple[str, list]`
  - `class StatsRepository` with async methods `search_indicators`, `get_indicator`, `series`, `upsert_indicator`, `upsert_observations`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_storage/test_stats_repository.py`:

```python
"""Query-builder tests are exact-string; row mapping is tested against a
stub connection.  Same split as `_stats_by` in MCP-2: the validation
lives in thin functions so it stays testable without a live Postgres."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import date

import pytest

from src.storage.stats import (
    StatsRepository,
    build_search_query,
    build_series_query,
)


# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    rows: list[dict]
    executed: list[tuple] = field(default_factory=list)

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    async def executemany(self, sql, params_seq):
        self.executed.append((sql, list(params_seq)))

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@dataclass
class _StubConn:
    cur: _StubCursor
    committed: int = 0

    def cursor(self, *a, **kw):
        return self.cur

    async def commit(self):
        self.committed += 1


def _repo_with(rows: list[dict]) -> tuple[StatsRepository, _StubConn]:
    conn = _StubConn(cur=_StubCursor(rows=rows))
    repo = StatsRepository()

    @asynccontextmanager
    async def _conn():
        yield conn

    repo._conn = _conn  # type: ignore[method-assign]
    return repo, conn


# ── query builders ───────────────────────────────────────────────────


def test_series_query_defaults_to_latest_revision():
    sql, params = build_series_query(7, None, None, None, None)
    assert "DISTINCT ON (period_start, dims)" in sql
    assert "ORDER BY period_start, dims, revision DESC" in sql
    assert params == [7]


def test_series_query_pins_an_explicit_revision():
    sql, params = build_series_query(7, None, None, None, 2)
    assert "revision = %s" in sql
    assert params == [7, 2]


def test_series_query_applies_date_bounds_and_dims():
    sql, params = build_series_query(
        7, date(2026, 1, 1), date(2026, 6, 1), {"region": "Москва"}, None,
    )
    assert "period_start >= %s" in sql
    assert "period_start <= %s" in sql
    assert "dims @> %s" in sql
    assert params[0] == 7
    assert date(2026, 1, 1) in params
    assert date(2026, 6, 1) in params


def test_search_query_uses_trigram_similarity_and_caps_limit():
    sql, params = build_search_query("тревожность", None, 20)
    assert "similarity(" in sql
    assert "ORDER BY score DESC" in sql
    assert params[-1] == 20


def test_search_query_filters_by_source():
    sql, params = build_search_query("тревожность", "fom", 5)
    assert "source = %s" in sql
    assert "fom" in params


# ── repository behaviour ─────────────────────────────────────────────


async def test_series_normalises_rows():
    repo, _ = _repo_with([
        {"period_start": date(2026, 1, 5), "period_end": date(2026, 1, 11),
         "dims": {}, "value": 57.5, "sample_n": 1500, "revision": 0,
         "source_doc_id": None},
    ])
    rows = await repo.series(7)
    assert rows == [{
        "period_start": "2026-01-05", "period_end": "2026-01-11",
        "dims": {}, "value": 57.5, "sample_n": 1500, "revision": 0,
        "source_doc_id": None,
    }]


async def test_series_rejects_bad_dims_type():
    repo, _ = _repo_with([])
    with pytest.raises(ValueError, match="dims"):
        await repo.series(7, dims=["region"])  # type: ignore[arg-type]


async def test_upsert_observations_commits_once():
    repo, conn = _repo_with([])
    await repo.upsert_observations([
        {"indicator_id": 7, "period_start": date(2026, 1, 5),
         "period_end": date(2026, 1, 11), "dims": {}, "value": 57.5,
         "sample_n": 1500, "revision": 0, "source_doc_id": None},
    ])
    assert conn.committed == 1
    sql, _ = conn.cur.executed[0]
    assert "ON CONFLICT (indicator_id, period_start, dims, revision)" in sql
    assert "DO UPDATE" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_storage/test_stats_repository.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.storage.stats'`

- [ ] **Step 3: Write the implementation**

Create `src/storage/stats.py`:

```python
"""Postgres access for the statistics subsystem.

The single query layer for `stat_indicator` / `stat_observation`, so
every surface reports identical numbers — the same discipline
`_stats_by` follows for `/api/v1/stats`.  Query construction is split
into pure builders so it can be asserted exactly without a live
database.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.stats.align import GRANULARITIES, VALUE_KINDS
from src.storage.pg_pool import get_pg_pool

_SERIES_COLUMNS = (
    "period_start, period_end, dims, value, sample_n, revision, source_doc_id"
)


def build_series_query(
    indicator_id: int,
    since: date | None,
    until: date | None,
    dims: dict[str, Any] | None,
    revision: int | None,
) -> tuple[str, list[Any]]:
    """Rows for one indicator, newest revision per period unless pinned."""
    params: list[Any] = [indicator_id]
    where = ["indicator_id = %s"]
    if since is not None:
        where.append("period_start >= %s")
        params.append(since)
    if until is not None:
        where.append("period_start <= %s")
        params.append(until)
    if dims:
        where.append("dims @> %s")
        params.append(json.dumps(dims))
    if revision is not None:
        where.append("revision = %s")
        params.append(revision)
        sql = (
            f"SELECT {_SERIES_COLUMNS} FROM stat_observation "
            f"WHERE {' AND '.join(where)} ORDER BY period_start, dims"
        )
        return sql, params
    sql = (
        f"SELECT DISTINCT ON (period_start, dims) {_SERIES_COLUMNS} "
        f"FROM stat_observation WHERE {' AND '.join(where)} "
        "ORDER BY period_start, dims, revision DESC"
    )
    return sql, params


def build_search_query(
    query: str, source: str | None, limit: int,
) -> tuple[str, list[Any]]:
    """Trigram search over the registry — the only searchable surface
    the subsystem has; values themselves carry no semantics."""
    # Placeholder order follows the SQL below: two in the SELECT
    # (similarity scoring), then two in the WHERE (the `%` trigram
    # operator, escaped as `%%`), then the optional source, then LIMIT.
    params: list[Any] = [query, query]
    where = ["(title %% %s OR question_text %% %s)"]
    params.extend([query, query])
    if source is not None:
        where.append("source = %s")
        params.append(source)
    params.append(limit)
    sql = (
        "SELECT id, source, code, title, question_text, unit, value_kind, "
        "granularity, dims_schema, entity_vid, "
        "GREATEST(similarity(title, %s), similarity(question_text, %s)) AS score "
        "FROM stat_indicator "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY score DESC, title LIMIT %s"
    )
    return sql, params


def _row_out(row: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe projection: dates to ISO, NUMERIC to float, UUID to str."""
    return {
        "period_start": row["period_start"].isoformat(),
        "period_end": row["period_end"].isoformat(),
        "dims": row["dims"],
        "value": float(row["value"]),
        "sample_n": row["sample_n"],
        "revision": row["revision"],
        "source_doc_id": (
            str(row["source_doc_id"]) if row["source_doc_id"] else None
        ),
    }


class StatsRepository:
    """Async wrapper over the two `stat_*` tables."""

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    @asynccontextmanager
    async def _conn(self) -> AsyncIterator[psycopg.AsyncConnection]:
        if self._dsn is None:
            pool = await get_pg_pool()
            async with pool.connection() as conn:
                yield conn
        else:
            async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
                yield conn

    async def search_indicators(
        self, query: str, *, source: str | None = None, limit: int = 20,
    ) -> list[dict[str, Any]]:
        sql, params = build_search_query(query, source, limit)
        async with self._conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
        return [{**r, "score": float(r["score"])} for r in rows]

    async def get_indicator(self, indicator_id: int) -> dict[str, Any] | None:
        async with self._conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT id, source, code, title, question_text, unit, "
                    "value_kind, granularity, dims_schema, entity_vid "
                    "FROM stat_indicator WHERE id = %s",
                    [indicator_id],
                )
                return await cur.fetchone()

    async def series(
        self,
        indicator_id: int,
        *,
        since: date | None = None,
        until: date | None = None,
        dims: dict[str, Any] | None = None,
        revision: int | None = None,
    ) -> list[dict[str, Any]]:
        if dims is not None and not isinstance(dims, dict):
            raise ValueError("dims must be an object mapping name → value")
        sql, params = build_series_query(
            indicator_id, since, until, dims, revision,
        )
        async with self._conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
        return [_row_out(r) for r in rows]

    async def upsert_indicator(
        self,
        *,
        source: str,
        code: str,
        title: str,
        unit: str,
        value_kind: str,
        granularity: str,
        question_text: str = "",
        dims_schema: dict[str, Any] | None = None,
        entity_vid: str | None = None,
    ) -> int:
        if value_kind not in VALUE_KINDS:
            raise ValueError(f"unknown value_kind {value_kind!r}")
        if granularity not in GRANULARITIES:
            raise ValueError(f"unknown granularity {granularity!r}")
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO stat_indicator
                        (source, code, title, question_text, unit,
                         value_kind, granularity, dims_schema, entity_vid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, code) DO UPDATE SET
                        title = EXCLUDED.title,
                        question_text = EXCLUDED.question_text,
                        unit = EXCLUDED.unit,
                        value_kind = EXCLUDED.value_kind,
                        granularity = EXCLUDED.granularity,
                        dims_schema = EXCLUDED.dims_schema,
                        entity_vid = EXCLUDED.entity_vid
                    RETURNING id
                    """,
                    (source, code, title, question_text, unit, value_kind,
                     granularity, json.dumps(dims_schema or {}), entity_vid),
                )
                row = await cur.fetchone()
            await conn.commit()
        return int(row[0]) if not isinstance(row, dict) else int(row["id"])

    async def upsert_observations(
        self, rows: Sequence[dict[str, Any]],
    ) -> int:
        """Insert or restate observations.  Idempotent by
        (indicator_id, period_start, dims, revision)."""
        if not rows:
            return 0
        payload = [
            (
                r["indicator_id"], r["period_start"], r["period_end"],
                json.dumps(r.get("dims") or {}), r["value"],
                r.get("sample_n"), int(r.get("revision", 0)),
                str(r["source_doc_id"]) if r.get("source_doc_id") else None,
            )
            for r in rows
        ]
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO stat_observation
                        (indicator_id, period_start, period_end, dims,
                         value, sample_n, revision, source_doc_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (indicator_id, period_start, dims, revision)
                    DO UPDATE SET
                        period_end = EXCLUDED.period_end,
                        value = EXCLUDED.value,
                        sample_n = EXCLUDED.sample_n,
                        source_doc_id = EXCLUDED.source_doc_id,
                        loaded_at = now()
                    """,
                    payload,
                )
            await conn.commit()
        return len(payload)
```

`upsert_observations` uses `executemany`, which the test stub records into the
same `executed` list as `execute` — that is why the test can assert on
`conn.cur.executed[0]`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_storage/test_stats_repository.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/storage/stats.py tests/test_storage/test_stats_repository.py`

- [ ] **Step 6: Commit**

```bash
git add src/storage/stats.py tests/test_storage/test_stats_repository.py
git commit -m "feat(stats): add StatsRepository with pure query builders"
```

---

### Task 4 — Amendment (approved 2026-08-09, after the Task 4 review)

Two things, both landing in the same two files.

**(a) Registry discovery.** `search_indicators` requires the caller to already
know a search term, and trigram matching does not find synonyms — so a caller
that guesses wrong cannot distinguish "no match" from "no such data" and will
confidently report the statistic does not exist. The registry needs a way to
answer "what is in here at all".

**(b) Validation coverage.** The Task 4 review found — correctly, and it is the
plan's fault, not the implementer's — that `upsert_indicator`'s `value_kind` /
`granularity` validation ships with no test at all, and that 3 of 5 repository
methods are never exercised. That validation is the gate keeping a malformed
source out of the registry, and more sources are expected.

**Interfaces added:**
- `build_sources_query() -> tuple[str, list]`
- `build_indicators_query(source: str | None, limit: int) -> tuple[str, list]`
- `StatsRepository.list_sources() -> list[dict]`
- `StatsRepository.list_indicators(*, source=None, limit=100) -> list[dict]`

- [ ] **Step A1: Write the failing tests**

Append to `tests/test_storage/test_stats_repository.py`, extending the existing
top-level import to include `build_indicators_query` and `build_sources_query`:

```python
def test_sources_query_rolls_up_indicator_count_and_period_bounds():
    sql, params = build_sources_query()
    assert "LEFT JOIN stat_observation" in sql
    assert "count(DISTINCT i.id) AS indicators" in sql
    assert "min(o.period_start) AS earliest" in sql
    assert "max(o.period_start) AS latest" in sql
    assert "GROUP BY i.source" in sql
    assert params == []


def test_indicators_query_without_source_has_no_where_clause():
    sql, params = build_indicators_query(None, 100)
    assert "WHERE" not in sql
    assert params == [100]


def test_indicators_query_filters_by_source():
    sql, params = build_indicators_query("fom", 50)
    assert "WHERE source = %s" in sql
    assert params == ["fom", 50]


async def test_list_sources_isoformats_period_bounds():
    repo, _ = _repo_with([
        {"source": "fom", "indicators": 3,
         "earliest": date(2026, 1, 5), "latest": date(2026, 6, 1)},
    ])
    assert await repo.list_sources() == [
        {"source": "fom", "indicators": 3,
         "earliest": "2026-01-05", "latest": "2026-06-01"},
    ]


async def test_list_sources_survives_a_source_with_no_observations():
    """A registered indicator with no rows yet must still be listed —
    otherwise a freshly seeded source looks like it does not exist."""
    repo, _ = _repo_with([
        {"source": "rosstat", "indicators": 1, "earliest": None, "latest": None},
    ])
    assert await repo.list_sources() == [
        {"source": "rosstat", "indicators": 1, "earliest": None, "latest": None},
    ]


async def test_upsert_indicator_rejects_unknown_value_kind():
    repo, conn = _repo_with([])
    with pytest.raises(ValueError, match="value_kind"):
        await repo.upsert_indicator(
            source="fom", code="x", title="T", unit="%",
            value_kind="ratio", granularity="week",
        )
    assert conn.cur.executed == []


async def test_upsert_indicator_rejects_unknown_granularity():
    repo, conn = _repo_with([])
    with pytest.raises(ValueError, match="granularity"):
        await repo.upsert_indicator(
            source="fom", code="x", title="T", unit="%",
            value_kind="share", granularity="fortnight",
        )
    assert conn.cur.executed == []


async def test_search_indicators_casts_score_to_float():
    repo, _ = _repo_with([
        {"id": 1, "source": "fom", "code": "anxiety", "title": "Тревожность",
         "question_text": "", "unit": "%", "value_kind": "share",
         "granularity": "week", "dims_schema": {}, "entity_vid": None,
         "score": 1},
    ])
    rows = await repo.search_indicators("тревожность")
    assert rows[0]["score"] == 1.0
    assert isinstance(rows[0]["score"], float)


async def test_get_indicator_returns_none_when_absent():
    repo, _ = _repo_with([])
    assert await repo.get_indicator(999) is None
```

- [ ] **Step A2: Run tests to verify they fail**

Run: `uv run pytest tests/test_storage/test_stats_repository.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_sources_query'`

- [ ] **Step A3: Implement**

Add to `src/storage/stats.py`, beside the existing builders:

```python
def build_sources_query() -> tuple[str, list[Any]]:
    """One row per source: how many indicators it has and the span its
    observations cover.  The entry point for a caller that does not yet
    know what the subsystem holds."""
    sql = (
        "SELECT i.source, count(DISTINCT i.id) AS indicators, "
        "min(o.period_start) AS earliest, max(o.period_start) AS latest "
        "FROM stat_indicator i "
        "LEFT JOIN stat_observation o ON o.indicator_id = i.id "
        "GROUP BY i.source ORDER BY i.source"
    )
    return sql, []


def build_indicators_query(
    source: str | None, limit: int,
) -> tuple[str, list[Any]]:
    """The registry itself, optionally scoped to one source."""
    params: list[Any] = []
    where = ""
    if source is not None:
        where = "WHERE source = %s "
        params.append(source)
    params.append(limit)
    sql = (
        "SELECT id, source, code, title, question_text, unit, value_kind, "
        "granularity, dims_schema, entity_vid FROM stat_indicator "
        f"{where}ORDER BY source, title LIMIT %s"
    )
    return sql, params
```

And the two repository methods:

```python
    async def list_sources(self) -> list[dict[str, Any]]:
        sql, params = build_sources_query()
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        # LEFT JOIN: a registered indicator with no observations yet yields
        # NULL bounds rather than dropping the source from the catalogue.
        return [
            {
                "source": r["source"],
                "indicators": int(r["indicators"]),
                "earliest": r["earliest"].isoformat() if r["earliest"] else None,
                "latest": r["latest"].isoformat() if r["latest"] else None,
            }
            for r in rows
        ]

    async def list_indicators(
        self, *, source: str | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        sql, params = build_indicators_query(source, limit)
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())
```

- [ ] **Step A4: Run tests to verify they pass**

Run: `uv run pytest tests/test_storage/test_stats_repository.py -v`
Expected: PASS — the 8 original tests plus 9 new ones.

- [ ] **Step A5: Lint and commit**

```bash
uv run ruff check src/storage/stats.py tests/test_storage/test_stats_repository.py
git add src/storage/stats.py tests/test_storage/test_stats_repository.py
git commit -m "feat(stats): add registry discovery queries and validation coverage"
```

---

### Task 5: MCP-3 server

**Files:**
- Create: `src/mcp/stats_server.py`
- Test: `tests/test_mcp/test_stats_server.py`
- Modify: `docs/runbook/mcp.md`, `README.md`, `docs/FEATURES.md`

**Interfaces:**
- Consumes: `StatsRepository` (Task 4); `align`, `AlignedResult` (Task 2); `settings.stats` (Task 3); `parse_args`, `assert_api_key_env_set`, `build_sse_auth`, `log_banner` from `src/mcp/_shared.py`.
- Produces: helpers `_indicators_search`, `_series`, `_align_tool` returning JSON-safe dicts; tools `stat_indicators_search`, `stat_series`, `stat_align`; `main()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_mcp/test_stats_server.py`:

```python
"""Helper-level tests — FastMCP tool invocation internals are not
exercised, same convention as the other MCP tests."""

from __future__ import annotations

import pytest

from src.mcp.stats_server import _align_tool, _indicators_search, _series


class _StubRepo:
    def __init__(self, rows=None, indicators=None, sources=None):
        self.rows = rows or []
        self.indicators = indicators or []
        self.sources = sources or []
        self.calls: list[tuple] = []

    async def search_indicators(self, query, *, source=None, limit=20):
        self.calls.append(("search", query, source, limit))
        return self.indicators

    async def list_sources(self):
        self.calls.append(("list_sources",))
        return self.sources

    async def list_indicators(self, *, source=None, limit=100):
        self.calls.append(("list_indicators", source, limit))
        return self.indicators

    async def series(self, indicator_id, *, since=None, until=None,
                     dims=None, revision=None):
        self.calls.append(("series", indicator_id, since, until, dims, revision))
        return self.rows

    async def get_indicator(self, indicator_id):
        return {"id": indicator_id, "value_kind": "share", "unit": "%",
                "granularity": "week", "title": "t", "source": "fom",
                "code": "c", "question_text": "", "dims_schema": {},
                "entity_vid": None}


async def test_series_rejects_bad_date():
    out = await _series(_StubRepo(), 1, "not-a-date", None, None)
    assert "error" in out
    assert "YYYY-MM-DD" in out["error"]


async def test_series_returns_rows_with_indicator_metadata():
    repo = _StubRepo(rows=[{"period_start": "2026-01-05", "value": 57.5}])
    out = await _series(repo, 1, None, None, None)
    assert out["rows"] == [{"period_start": "2026-01-05", "value": 57.5}]
    assert out["indicator"]["unit"] == "%"


async def test_indicators_search_without_query_returns_the_catalogue():
    """No query and no source means the caller does not yet know what
    exists — answer with the catalogue rather than an error, or the
    caller has to guess a search term to learn anything."""
    repo = _StubRepo(sources=[
        {"source": "fom", "indicators": 3,
         "earliest": "2026-01-05", "latest": "2026-06-01"},
    ])
    out = await _indicators_search(repo, None, None, 20)
    assert out["sources"] == repo.sources
    assert repo.calls == [("list_sources",)]
    assert "error" not in out


async def test_indicators_search_blank_query_is_treated_as_absent():
    repo = _StubRepo(sources=[])
    out = await _indicators_search(repo, "   ", None, 20)
    assert "sources" in out
    assert repo.calls == [("list_sources",)]


async def test_indicators_search_with_source_only_lists_that_source():
    repo = _StubRepo(indicators=[{"id": 1, "source": "fom"}])
    out = await _indicators_search(repo, None, "fom", 20)
    assert out["source"] == "fom"
    assert out["indicators"] == repo.indicators
    assert repo.calls == [("list_indicators", "fom", 20)]


async def test_indicators_search_with_query_runs_the_trigram_search():
    repo = _StubRepo(indicators=[{"id": 1}])
    out = await _indicators_search(repo, "тревожность", None, 20)
    assert out["query"] == "тревожность"
    assert repo.calls[0][0] == "search"


async def test_indicators_search_caps_limit():
    repo = _StubRepo(indicators=[])
    await _indicators_search(repo, "тревожность", None, 10_000)
    assert repo.calls[0][3] <= 100


def test_align_tool_is_json_safe_and_reports_warnings():
    a = [{"period_start": f"2026-01-{d:02d}", "value": float(d)}
         for d in (5, 12, 19, 26)]
    out = _align_tool(a, a, "week", "share", "share", 0)
    assert out["divergence"] == pytest.approx(0.0)
    assert all(isinstance(g, str) for g in out["grid"])
    assert "low_overlap:4<8" in out["warnings"]


def test_align_tool_rejects_malformed_points():
    out = _align_tool([{"value": 1.0}], [], "week", "share", "share", 0)
    assert "error" in out


def test_align_tool_rejects_unknown_granularity():
    out = _align_tool([], [], "fortnight", "share", "share", 0)
    assert "error" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_mcp/test_stats_server.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.mcp.stats_server'`

- [ ] **Step 3: Write the implementation**

Create `src/mcp/stats_server.py`:

```python
"""MCP-3: exact-statistics tools.

Returns data, never prose.  No LLM call exists anywhere in this
server's path, and it reads neither the graph nor Milvus — exact
numbers have a different contract from the semantic contour, where a
worse answer is still an acceptable answer.

The comparison tool takes BOTH series as arguments and reads nothing:
the client fetches the channel-side series from MCP-2 and the
indicator series from here, then hands both over.  That keeps the
boundary clean and keeps the arithmetic away from the model.

Run::

    uv run python -m src.mcp.stats_server --transport stdio
    uv run python -m src.mcp.stats_server --transport http --port 9003
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastmcp import FastMCP

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set,
    build_sse_auth,
    log_banner,
    parse_args,
)
from src.stats.align import GRANULARITIES, VALUE_KINDS, align
from src.storage.stats import StatsRepository

mcp = FastMCP(
    name="kb-llamaindex-stats",
    instructions=(
        "Exact external statistics (polls, official series).  Every tool "
        "returns data, never a written answer.  "
        "START by calling stat_indicators_search with NO arguments — it "
        "returns the catalogue of sources and what each covers.  Do not "
        "guess a search term before you have seen it: matching is "
        "trigram-based, so a wrong guess returns nothing and looks "
        "exactly like the data not existing.  "
        "Then stat_series for one indicator's values, and stat_align to "
        "compare it against a channel-side series fetched from the MCP-2 "
        "server (topic_trend / polarity_evolution).  stat_align is the "
        "arithmetic — do not compute gaps, correlations or lags yourself."
    ),
    auth=build_sse_auth(),
)

_MAX_SEARCH_LIMIT = 100


def _repo() -> StatsRepository:
    return StatsRepository()


def _parse_date(value: str | None, field: str) -> tuple[date | None, str | None]:
    if value is None:
        return None, None
    try:
        return date.fromisoformat(value), None
    except ValueError:
        return None, f"{field} must be ISO YYYY-MM-DD, got {value!r}"


def _points(raw: list[dict[str, Any]], label: str) -> tuple[list, str | None]:
    out: list[tuple[date, float]] = []
    for i, p in enumerate(raw):
        if not isinstance(p, dict) or "period_start" not in p or "value" not in p:
            return [], (
                f"{label}[{i}] must be an object with 'period_start' and 'value'"
            )
        try:
            d = date.fromisoformat(str(p["period_start"]))
            v = float(p["value"])
        except (ValueError, TypeError) as exc:
            return [], f"{label}[{i}] is malformed: {exc}"
        out.append((d, v))
    return out, None


async def _indicators_search(
    repo: Any, query: str | None, source: str | None, limit: int,
) -> dict[str, Any]:
    """Three modes, deliberately behind one tool.

    No query and no source is not an error — it is a caller that does
    not yet know what exists.  Answering it with the catalogue is the
    only way such a caller can learn anything: trigram matching finds
    spelling variants but not synonyms, so a wrong guess is
    indistinguishable from "no such data" and would be reported as the
    statistic not existing.
    """
    capped = max(1, min(int(limit), _MAX_SEARCH_LIMIT))
    blank = not query or not query.strip()
    if blank and source is None:
        return {"sources": await repo.list_sources()}
    if blank:
        rows = await repo.list_indicators(source=source, limit=capped)
        return {"source": source, "indicators": rows}
    rows = await repo.search_indicators(
        query.strip(), source=source, limit=capped,
    )
    return {"query": query.strip(), "source": source, "indicators": rows}


async def _series(
    repo: Any,
    indicator_id: int,
    since: str | None,
    until: str | None,
    dims: dict[str, Any] | None,
) -> dict[str, Any]:
    s, err = _parse_date(since, "since")
    if err:
        return {"error": err}
    u, err = _parse_date(until, "until")
    if err:
        return {"error": err}
    if dims is not None and not isinstance(dims, dict):
        return {"error": "dims must be an object mapping name → value"}
    indicator = await repo.get_indicator(indicator_id)
    if indicator is None:
        return {"error": f"no indicator with id {indicator_id}"}
    rows = await repo.series(indicator_id, since=s, until=u, dims=dims)
    return {"indicator": indicator, "rows": rows}


def _align_tool(
    series_a: list[dict[str, Any]],
    series_b: list[dict[str, Any]],
    granularity: str,
    value_kind_a: str,
    value_kind_b: str,
    max_lag: int,
) -> dict[str, Any]:
    if granularity not in GRANULARITIES:
        return {"error": f"granularity must be one of {sorted(GRANULARITIES)}"}
    for name, kind in (("value_kind_a", value_kind_a), ("value_kind_b", value_kind_b)):
        if kind not in VALUE_KINDS:
            return {"error": f"{name} must be one of {sorted(VALUE_KINDS)}"}
    if max_lag < 0:
        return {"error": "max_lag must be >= 0"}
    pa, err = _points(series_a, "series_a")
    if err:
        return {"error": err}
    pb, err = _points(series_b, "series_b")
    if err:
        return {"error": err}
    res = align(
        pa, pb, granularity=granularity,
        value_kind_a=value_kind_a, value_kind_b=value_kind_b,
        max_lag=max_lag, min_overlap=settings.stats.min_overlap,
    )
    return {
        "grid": [d.isoformat() for d in res.grid],
        "a": res.a, "b": res.b,
        "a_norm": res.a_norm, "b_norm": res.b_norm,
        "gap": res.gap,
        "divergence": res.divergence,
        "best_lag": res.best_lag,
        "correlation": res.correlation,
        "warnings": res.warnings,
    }


@mcp.tool(timeout=120)
async def stat_indicators_search(
    query: str | None = None, source: str | None = None, limit: int = 20,
) -> dict[str, Any]:
    """Discover what external statistics exist, then narrow to one indicator.

    CALL THIS FIRST, WITH NO ARGUMENTS — you get the catalogue: every
    source, how many indicators it holds, and the period it covers.
    Then either pass `source` to list that provider's indicators, or
    pass `query` to search by name and poll wording.

    Do not guess a `query` before you have seen the catalogue.  Matching
    is trigram-based: it finds spelling variants but NOT synonyms, so a
    wrong guess returns nothing and is indistinguishable from the data
    not existing.

    Every indicator carries `unit`, `value_kind` and `granularity` —
    that is what tells you whether two series are comparable at all, and
    it is what `stat_align` needs.
    NOT FOR: values (use `stat_series`) or document text (use MCP-2
    `vector_search`)."""
    return await _indicators_search(_repo(), query, source, limit)


@mcp.tool(timeout=120)
async def stat_series(
    indicator_id: int,
    since: str | None = None,
    until: str | None = None,
    dims: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Values of one indicator over time, newest revision per period.

    USE FOR: "как менялась тревожность", fetching the poll side before
    a comparison.  `since` / `until` are ISO `YYYY-MM-DD` bounds on
    `period_start`; `dims` filters a panel cut, e.g.
    `{"region": "Москва"}`.  Each row carries `source_doc_id` — the
    ingested bulletin the number came from, so a claim can be traced
    back.
    NEXT STEP: to compare against channel attention, fetch a series
    with MCP-2 `topic_trend` and pass both to `stat_align`."""
    return await _series(_repo(), indicator_id, since, until, dims)


@mcp.tool(timeout=120)
async def stat_align(
    series_a: list[dict[str, Any]],
    series_b: list[dict[str, Any]],
    granularity: str = "week",
    value_kind_a: str = "share",
    value_kind_b: str = "share",
    max_lag: int = 4,
) -> dict[str, Any]:
    """Put two series on a common grid and measure how far apart they run.

    USE FOR: comparing channel attention against a poll indicator.
    Each series is a list of `{"period_start": "YYYY-MM-DD", "value":
    <number>}`.  Both are resampled DOWN to `granularity` (never
    interpolated up), z-scored so different units are comparable, and
    correlated across shifts in `[-max_lag, +max_lag]`; the best-fitting
    shift is reported as `best_lag`.  Returns per-period `gap`, a scalar
    `divergence` (mean absolute gap), and `warnings` — read them:
    `sparse:*` means one side had missing buckets, `low_overlap:*` means
    too few common periods for the correlation to mean anything.
    Do NOT compute these numbers yourself; this tool is the arithmetic."""
    return _align_tool(
        series_a, series_b, granularity, value_kind_a, value_kind_b, max_lag,
    )


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-stats",
        transport=args["transport"], host=args["host"], port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=args["host"], port=args["port"])


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_mcp/test_stats_server.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Smoke-test the server starts**

Run: `uv run python -m src.mcp.stats_server --transport http --port 9003`
Expected: banner logged, no traceback. Stop it with Ctrl-C.

- [ ] **Step 6: Update the docs**

- `docs/runbook/mcp.md` — add an MCP-3 section: purpose (exact statistics, no synthesis), the three tools, the stdio/http invocations above, default port 9003.
- `README.md:141` — the runbook table row says "Two MCP servers"; make it three and name MCP-3.
- `README.md:178` — the tree comment `# MCP-1 (search/analyze) + MCP-2 (atomic+GDS)` gains MCP-3.
- `docs/FEATURES.md:101` — "Две MCP-поверхности" becomes three; describe MCP-3 in one sentence in the surrounding Russian style.

- [ ] **Step 7: Commit**

```bash
git add src/mcp/stats_server.py tests/test_mcp/test_stats_server.py docs/runbook/mcp.md README.md docs/FEATURES.md
git commit -m "feat(mcp): add MCP-3 statistics server with search/series/align tools"
```

---

### Task 6: HTTP load endpoint

Data arrives over HTTP, not as files. Row volumes are small, so a single
request carrying one indicator and its observations is the whole ingest path —
no CSV parsing, no per-source adapter, no CLI.

**Route prefix note:** `/stats` is already taken by
`src/api/routes/stats.py`, which reports *ingest-pipeline* statistics over the
`documents` table. That is a different meaning of the word. The external
statistics subsystem gets `/statistics` so the two never blur together in a
URL.

**Files:**
- Create: `src/api/routes/stats_data.py`
- Modify: `src/api/main.py` (register the router)
- Test: `tests/test_api/test_stats_data.py`

**Interfaces:**
- Consumes: `StatsRepository.upsert_indicator` / `upsert_observations` (Task 4);
  `GRANULARITIES`, `VALUE_KINDS` (Task 1).
- Produces: `POST /api/v1/statistics/load` accepting `LoadRequest`
  (`indicator: IndicatorIn`, `observations: list[ObservationIn]`) and returning
  `LoadResponse` (`indicator_id: int`, `observations: int`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_api/test_stats_data.py`:

```python
"""ASGI tests for `POST /api/v1/statistics/load`.

`StatsRepository` is patched so the route is exercised end-to-end
against the real FastAPI app without a live Postgres — same approach as
`tests/test_api/test_ingest.py`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


def _body(**overrides) -> dict:
    body = {
        "indicator": {
            "source": "fom",
            "code": "anxiety",
            "title": "Уровень тревожности",
            "question_text": "Какое настроение преобладает?",
            "unit": "%",
            "value_kind": "share",
            "granularity": "week",
        },
        "observations": [
            {
                "period_start": "2026-01-05",
                "period_end": "2026-01-11",
                "value": 57.5,
                "sample_n": 1500,
            },
        ],
    }
    body.update(overrides)
    return body


async def _post(body: dict) -> tuple[int, dict]:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/statistics/load", json=body, headers=_api_key_header(),
        )
    return resp.status_code, resp.json()


@pytest.mark.asyncio
async def test_load_upserts_indicator_then_observations() -> None:
    from src.storage.stats import StatsRepository

    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=7),
        ) as up_ind,
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=1),
        ) as up_obs,
    ):
        code, payload = await _post(_body())

    assert code == 200
    assert payload == {"indicator_id": 7, "observations": 1}
    assert up_ind.await_count == 1
    rows = up_obs.await_args.args[0]
    assert rows[0]["indicator_id"] == 7
    assert rows[0]["dims"] == {}
    assert rows[0]["revision"] == 0


@pytest.mark.asyncio
async def test_load_requires_an_api_key() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/api/v1/statistics/load", json=_body())
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_load_rejects_unknown_value_kind() -> None:
    body = _body()
    body["indicator"]["value_kind"] = "ratio"
    code, payload = await _post(body)
    assert code == 422
    assert "value_kind" in str(payload)


@pytest.mark.asyncio
async def test_load_rejects_unknown_granularity() -> None:
    body = _body()
    body["indicator"]["granularity"] = "fortnight"
    code, payload = await _post(body)
    assert code == 422
    assert "granularity" in str(payload)


@pytest.mark.asyncio
async def test_load_rejects_period_end_before_period_start() -> None:
    body = _body()
    body["observations"][0]["period_end"] = "2026-01-01"
    code, payload = await _post(body)
    assert code == 422
    assert "period_end" in str(payload)


@pytest.mark.asyncio
async def test_load_accepts_an_empty_observation_list() -> None:
    """Registering an indicator before any data exists is legitimate —
    it is how a source gets seeded, and `list_sources` is built to show
    such a source with NULL period bounds."""
    from src.storage.stats import StatsRepository

    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=7),
        ),
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=0),
        ),
    ):
        code, payload = await _post(_body(observations=[]))

    assert code == 200
    assert payload == {"indicator_id": 7, "observations": 0}


@pytest.mark.asyncio
async def test_load_carries_dims_and_revision_through() -> None:
    from src.storage.stats import StatsRepository

    body = _body(observations=[
        {
            "period_start": "2026-06-01",
            "period_end": "2026-06-30",
            "value": 8.1,
            "dims": {"region": "Москва"},
            "revision": 1,
        },
    ])
    with (
        patch.object(
            StatsRepository, "upsert_indicator", new=AsyncMock(return_value=2),
        ),
        patch.object(
            StatsRepository, "upsert_observations", new=AsyncMock(return_value=1),
        ) as up_obs,
    ):
        code, _ = await _post(body)

    assert code == 200
    row = up_obs.await_args.args[0][0]
    assert row["dims"] == {"region": "Москва"}
    assert row["revision"] == 1
    assert row["sample_n"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_stats_data.py -v`
Expected: FAIL — 404 on the route, because it does not exist yet.

- [ ] **Step 3: Write the route**

Create `src/api/routes/stats_data.py`:

```python
"""Write path for the external-statistics subsystem.

One endpoint: `POST /api/v1/statistics/load` takes an indicator and its
observations and upserts both.  Row volumes are small, so there is no
batching protocol, no file upload and no per-source adapter — a caller
posts JSON.

The prefix is `/statistics`, not `/stats`: the latter already means
ingest-pipeline statistics over the `documents` table
(`src/api/routes/stats.py`), which is a different thing entirely.

Reads do NOT live here — they are served by MCP-3
(`src/mcp/stats_server.py`), which is the surface agents talk to.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator

from src.api.auth import require_api_key
from src.stats.align import GRANULARITIES, VALUE_KINDS
from src.storage.stats import StatsRepository

router = APIRouter(
    prefix="/statistics",
    tags=["statistics"],
    dependencies=[Depends(require_api_key)],
)

# Small by design: the subsystem takes curated series, not bulk dumps.
_MAX_OBSERVATIONS = 1000


class IndicatorIn(BaseModel):
    source: str = Field(min_length=1)
    code: str = Field(min_length=1)
    title: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    value_kind: str
    granularity: str
    question_text: str = ""
    dims_schema: dict[str, Any] = Field(default_factory=dict)
    entity_vid: str | None = None

    @field_validator("value_kind")
    @classmethod
    def _known_value_kind(cls, v: str) -> str:
        if v not in VALUE_KINDS:
            raise ValueError(f"value_kind must be one of {sorted(VALUE_KINDS)}")
        return v

    @field_validator("granularity")
    @classmethod
    def _known_granularity(cls, v: str) -> str:
        if v not in GRANULARITIES:
            raise ValueError(f"granularity must be one of {sorted(GRANULARITIES)}")
        return v


class ObservationIn(BaseModel):
    period_start: date
    period_end: date
    value: float
    dims: dict[str, Any] = Field(default_factory=dict)
    sample_n: int | None = None
    revision: int = Field(default=0, ge=0)
    source_doc_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def _ordered_period(self) -> ObservationIn:
        if self.period_end < self.period_start:
            raise ValueError("period_end must not precede period_start")
        return self


class LoadRequest(BaseModel):
    indicator: IndicatorIn
    observations: list[ObservationIn] = Field(
        default_factory=list, max_length=_MAX_OBSERVATIONS,
    )


class LoadResponse(BaseModel):
    indicator_id: int
    observations: int


@router.post(
    "/load",
    response_model=LoadResponse,
    status_code=status.HTTP_200_OK,
    summary="Upsert one indicator and its observations",
)
async def load_statistics(req: LoadRequest) -> LoadResponse:
    """Idempotent: re-posting the same payload changes nothing.

    Values are stored exactly as supplied — alignment and normalisation
    are computed on read, so changing the normalisation method never
    requires reloading a source.
    """
    repo = StatsRepository()
    ind = req.indicator
    try:
        indicator_id = await repo.upsert_indicator(
            source=ind.source,
            code=ind.code,
            title=ind.title,
            unit=ind.unit,
            value_kind=ind.value_kind,
            granularity=ind.granularity,
            question_text=ind.question_text,
            dims_schema=ind.dims_schema,
            entity_vid=ind.entity_vid,
        )
    except ValueError as exc:  # defence in depth; pydantic catches this first
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc),
        ) from exc

    rows = [
        {
            "indicator_id": indicator_id,
            "period_start": o.period_start,
            "period_end": o.period_end,
            "dims": o.dims,
            "value": o.value,
            "sample_n": o.sample_n,
            "revision": o.revision,
            "source_doc_id": o.source_doc_id,
        }
        for o in req.observations
    ]
    written = await repo.upsert_observations(rows)
    return LoadResponse(indicator_id=indicator_id, observations=written)
```

- [ ] **Step 4: Register the router**

In `src/api/main.py`, import `stats_data` alongside the other route modules and
register it next to the existing stats router:

```python
app.include_router(stats_data.router, prefix="/api/v1")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_stats_data.py -v`
Expected: PASS (7 tests)

- [ ] **Step 6: Verify against the live stack**

The API container reaches Postgres over the compose network, so this is the
path to test end-to-end. Post one indicator with two observations, then post
the identical payload a second time: both must return 200 and the same
`indicator_id`, and the second must not create duplicate rows — that is what
the `UNIQUE (indicator_id, period_start, dims, revision)` key is for.

Then post the same period with `"revision": 1` and a different value, and
confirm a *new* row is added rather than the first being overwritten — history
is retained.

If the API is not currently running, say so in your report rather than
skipping the check silently.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check src/api/routes/stats_data.py tests/test_api/test_stats_data.py
git add src/api/routes/stats_data.py src/api/main.py tests/test_api/test_stats_data.py
git commit -m "feat(stats): add POST /api/v1/statistics/load"
```

### Task 7: Expose channel-side series on MCP-2

Without this the comparison use case does not assemble: `topic_trend` and `polarity_evolution` exist only as analytics-catalog primitives, and `channel_message_timeline` measures ingest volume, not topic attention.

**Files:**
- Modify: `src/mcp/tools_server.py`
- Test: `tests/test_mcp/test_tools_server_trend.py` (create)

**Interfaces:**
- Consumes: `topic_trend`, `polarity_evolution` from `src/analytics/primitives/dynamics.py`; the existing `_deps["graph_store"]` bootstrapped in `_init()` (`src/mcp/tools_server.py:129`).
- Produces: helpers `_topic_trend`, `_polarity_evolution`; tools `topic_trend`, `polarity_evolution`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_mcp/test_tools_server_trend.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_mcp/test_tools_server_trend.py -v`
Expected: FAIL — `ImportError: cannot import name '_topic_trend'`

- [ ] **Step 3: Write the implementation**

Add to `src/mcp/tools_server.py`, next to `_stats_by` / `_timeline`:

```python
_TREND_GRANULARITIES = ("day", "week", "month", "quarter", "year")


def _period_in_window(period: str, since: str | None, until: str | None) -> bool:
    """`topic_trend` buckets are ISO-ordered strings ("2026-05",
    "2026-05-04"), so a prefix-safe lexicographic compare is exact."""
    if since and period < since[: len(period)]:
        return False
    return not (until and period > until[: len(period)])


async def _topic_trend(
    store: Any, topic: str, granularity: str,
    since: str | None, until: str | None, _fn: Any = None,
) -> dict[str, Any]:
    from datetime import date

    from src.analytics.primitives.dynamics import topic_trend as _primitive

    if not topic or not topic.strip():
        return {"error": "topic must be a non-empty string"}
    if granularity not in _TREND_GRANULARITIES:
        return {"error": f"granularity must be one of {list(_TREND_GRANULARITIES)}"}
    for name, value in (("since", since), ("until", until)):
        if value is not None:
            try:
                date.fromisoformat(value)
            except ValueError:
                return {"error": f"{name} must be ISO YYYY-MM-DD, got {value!r}"}
    if store is None:
        return {"error": "graph store unavailable"}
    fn = _fn or _primitive
    res = await fn(store, topic=topic.strip(), granularity=granularity)
    rows = [r for r in res.rows if _period_in_window(r["period"], since, until)]
    return {"topic": topic.strip(), "granularity": granularity, "rows": rows}


async def _polarity_evolution(
    store: Any, name: str | None, rel_type: str | None, _fn: Any = None,
) -> dict[str, Any]:
    from src.analytics.primitives.dynamics import polarity_evolution as _primitive

    if not name and not rel_type:
        return {"error": "provide at least one of name / rel_type"}
    if store is None:
        return {"error": "graph store unavailable"}
    fn = _fn or _primitive
    res = await fn(store, name=name, rel_type=rel_type)
    return {"name": name, "rel_type": rel_type, "rows": res.rows}
```

And the two tools, following the file's existing `@mcp.tool` style:

```python
@mcp.tool(timeout=1800)
async def topic_trend(
    topic: str,
    granularity: str = "month",
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """How often a topic is mentioned across ingested documents, per period
    — the channel-side ATTENTION series.

    USE FOR: "как часто писали про X", and as the channel-side input to
    the MCP-3 `stat_align` tool when comparing attention against a poll
    indicator.  `granularity`: day / week / month (default) / quarter /
    year.  `since` / `until` are ISO `YYYY-MM-DD` bounds applied to the
    returned buckets.  Returns `{topic, granularity, rows:[{period,
    mentions}]}`.
    NOT FOR: ingest volume (use `channel_message_timeline`) or message
    content (use `vector_search`)."""
    await _init()
    return await _topic_trend(
        _deps.get("graph_store"), topic, granularity, since, until,
    )


@mcp.tool(timeout=1800)
async def polarity_evolution(
    name: str | None = None, rel_type: str | None = None,
) -> dict[str, Any]:
    """How the polarity of an entity's relations shifts over time — the
    channel-side VALUATION series.

    USE FOR: "как менялось отношение к X", and as the channel-side input
    to MCP-3 `stat_align` when comparing tone against poll assessments.
    Provide at least one of `name` (entity) / `rel_type` (relation type).
    Polarity is computed over graph EDGES, not per-message sentiment —
    it is a coarser signal than a poll's rating scale, so read it as
    direction rather than magnitude.
    NOT FOR: mention counts (use `topic_trend`)."""
    await _init()
    return await _polarity_evolution(
        _deps.get("graph_store"), name, rel_type,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_mcp/test_tools_server_trend.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Run the whole suite**

Run: `uv run pytest tests/test_mcp tests/test_stats tests/test_storage tests/test_config tests/test_scripts -q`
Expected: PASS, no regressions.

- [ ] **Step 6: Update the MCP-2 docs**

In `docs/runbook/mcp.md`, add `topic_trend` and `polarity_evolution` to the MCP-2 tool list, noting they are the channel-side inputs to MCP-3's `stat_align`.

- [ ] **Step 7: Commit**

```bash
git add src/mcp/tools_server.py tests/test_mcp/test_tools_server_trend.py docs/runbook/mcp.md
git commit -m "feat(mcp): expose topic_trend and polarity_evolution on MCP-2"
```

---

## Verification

After Task 7, the end-to-end path should be walkable by hand:

1. `POST /api/v1/statistics/load` with one indicator and three weekly
   observations, one of which restates an earlier period at `revision: 1`
2. Start MCP-3: `uv run python -m src.mcp.stats_server --transport http --port 9003`
3. From an MCP client: `stat_indicators_search()` with no arguments → the
   catalogue, showing `fom` with its indicator count and covered period. Then
   `stat_indicators_search("тревожность")` → note the `id`
4. `stat_series(<id>)` → three rows, the 2026-01-12 period showing revision 1 (`55.2`), not revision 0
5. From MCP-2: `topic_trend("тревожность", granularity="week")`
6. `stat_align(<channel rows>, <indicator rows>, granularity="week", value_kind_a="level", value_kind_b="share", max_lag=4)` → a `warnings` list containing `low_overlap:…` for the three-point fixture, which is the correct answer for that little data.

Step 6 returning a low-overlap warning rather than a confident number is the
success criterion, not a failure: the subsystem is supposed to refuse to
dress up noise.

## Notes for the implementer

- **Do not** add these tools to the analytics `CATALOG`. The planner path is deliberately out of scope; `prim.fn(store, **params)` stays as it is.
- **Do not** touch synthesis, `/api/v1/search/*`, or `SearchOrchestratorWorkflow`.
- If trigram search proves too weak in practice (synonyms like «настроения» vs «социальное самочувствие»), the escape hatch is a Milvus collection over the registry, following `entity_er_vec` (`src/config.py:667`) — but that is a separate spec, not a quiet addition here.
- `sample_n` is stored but unused. Confidence intervals are deliberately deferred.

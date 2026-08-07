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
- `scripts/stat_import.py` — CSV loader CLI with a per-source adapter seam.
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
    def __init__(self, rows=None, indicators=None):
        self.rows = rows or []
        self.indicators = indicators or []
        self.calls: list[tuple] = []

    async def search_indicators(self, query, *, source=None, limit=20):
        self.calls.append(("search", query, source, limit))
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


async def test_indicators_search_rejects_empty_query():
    out = await _indicators_search(_StubRepo(), "  ", None, 10)
    assert "error" in out


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
        "returns data, never a written answer.  Typical flow: "
        "stat_indicators_search to find an indicator, stat_series to get "
        "its values, then stat_align to compare it against a channel-side "
        "series fetched from the MCP-2 server."
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
    repo: Any, query: str, source: str | None, limit: int,
) -> dict[str, Any]:
    if not query or not query.strip():
        return {"error": "query must be a non-empty string"}
    capped = max(1, min(int(limit), _MAX_SEARCH_LIMIT))
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
    query: str, source: str | None = None, limit: int = 20,
) -> dict[str, Any]:
    """Find external statistical indicators by name or poll wording.

    USE FOR: "какие есть показатели про тревожность", discovering what
    can be compared before calling `stat_series`.  Each hit carries
    `unit`, `value_kind` and `granularity`, which is what tells you
    whether two indicators are comparable at all.  Optional `source`
    filters to one provider (e.g. "fom").  Matching is trigram-based,
    so it finds spelling variants but NOT synonyms — try the actual
    wording you expect on the bulletin.
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

### Task 6: CSV loader CLI

**Files:**
- Create: `scripts/stat_import.py`, `tests/test_stats/test_import.py`
- Create fixture: `tests/test_stats/fixtures/fom_sample.csv`

**Interfaces:**
- Consumes: `StatsRepository.upsert_indicator` / `upsert_observations` (Task 4).
- Produces: `parse_csv(text: str) -> tuple[list[dict], list[str]]` returning `(rows, errors)`; `main()` CLI entry.

- [ ] **Step 1: Create the fixture**

`tests/test_stats/fixtures/fom_sample.csv`:

```csv
source,code,title,question_text,unit,value_kind,granularity,entity_vid,period_start,period_end,dims,value,sample_n,revision
fom,anxiety,Уровень тревожности,"Какое настроение преобладает?",%,share,week,,2026-01-05,2026-01-11,{},57.5,1500,0
fom,anxiety,Уровень тревожности,"Какое настроение преобладает?",%,share,week,,2026-01-12,2026-01-18,{},54.0,1500,0
fom,anxiety,Уровень тревожности,"Какое настроение преобладает?",%,share,week,,2026-01-12,2026-01-18,{},55.2,1500,1
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_stats/test_import.py`:

```python
from __future__ import annotations

from pathlib import Path

from scripts.stat_import import parse_csv

FIXTURE = Path(__file__).parent / "fixtures" / "fom_sample.csv"


def test_parse_csv_reads_all_rows():
    rows, errors = parse_csv(FIXTURE.read_text(encoding="utf-8"))
    assert errors == []
    assert len(rows) == 3


def test_parse_csv_keeps_a_restatement_as_a_separate_revision():
    """A revised value for an already-loaded period must arrive as its
    own row with revision=1 — history is retained, never overwritten."""
    rows, _ = parse_csv(FIXTURE.read_text(encoding="utf-8"))
    week2 = [r for r in rows if r["period_start"].isoformat() == "2026-01-12"]
    assert sorted(r["revision"] for r in week2) == [0, 1]
    assert {r["value"] for r in week2} == {54.0, 55.2}


def test_parse_csv_normalises_types():
    rows, _ = parse_csv(FIXTURE.read_text(encoding="utf-8"))
    r = rows[0]
    assert r["value"] == 57.5
    assert r["sample_n"] == 1500
    assert r["dims"] == {}
    assert r["value_kind"] == "share"


def test_parse_csv_reports_bad_rows_without_aborting():
    text = (
        "source,code,title,question_text,unit,value_kind,granularity,"
        "entity_vid,period_start,period_end,dims,value,sample_n,revision\n"
        "fom,x,T,,%,share,week,,not-a-date,2026-01-11,{},1.0,10,0\n"
        "fom,x,T,,%,bogus,week,,2026-01-05,2026-01-11,{},1.0,10,0\n"
        "fom,x,T,,%,share,week,,2026-01-05,2026-01-11,{},2.0,10,0\n"
    )
    rows, errors = parse_csv(text)
    assert len(rows) == 1
    assert len(errors) == 2
    assert any("period_start" in e for e in errors)
    assert any("value_kind" in e for e in errors)


def test_parse_csv_rejects_missing_columns():
    rows, errors = parse_csv("source,code\nfom,x\n")
    assert rows == []
    assert errors and "missing columns" in errors[0]
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_stats/test_import.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.stat_import'`

- [ ] **Step 4: Write the implementation**

Create `scripts/stat_import.py`:

```python
"""Load external statistics from a flat CSV into `stat_*`.

Raw values are stored EXACTLY as supplied — alignment and normalisation
happen on read, so changing the normalisation method never requires
reloading a source.  `entity_vid` is a curated column, not inferred.

Usage::

    uv run python -m scripts.stat_import path/to/fom_dominanty.csv
    uv run python -m scripts.stat_import path/to/file.csv --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger  # noqa: E402

from src.stats.align import GRANULARITIES, VALUE_KINDS  # noqa: E402
from src.storage.stats import StatsRepository  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402

REQUIRED_COLUMNS = (
    "source", "code", "title", "question_text", "unit", "value_kind",
    "granularity", "entity_vid", "period_start", "period_end", "dims",
    "value", "sample_n", "revision",
)


def parse_csv(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse and validate.  Bad rows are collected, not fatal — one
    malformed line must not cost the whole load."""
    reader = csv.DictReader(io.StringIO(text))
    missing = [c for c in REQUIRED_COLUMNS if c not in (reader.fieldnames or [])]
    if missing:
        return [], [f"missing columns: {', '.join(missing)}"]

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for lineno, raw in enumerate(reader, start=2):
        try:
            if raw["value_kind"] not in VALUE_KINDS:
                raise ValueError(f"value_kind {raw['value_kind']!r} is not valid")
            if raw["granularity"] not in GRANULARITIES:
                raise ValueError(f"granularity {raw['granularity']!r} is not valid")
            try:
                period_start = date.fromisoformat(raw["period_start"])
            except ValueError:
                raise ValueError(
                    f"period_start {raw['period_start']!r} is not ISO YYYY-MM-DD",
                ) from None
            period_end = date.fromisoformat(raw["period_end"])
            sample = raw["sample_n"].strip()
            rows.append({
                "source": raw["source"].strip(),
                "code": raw["code"].strip(),
                "title": raw["title"].strip(),
                "question_text": raw["question_text"].strip(),
                "unit": raw["unit"].strip(),
                "value_kind": raw["value_kind"].strip(),
                "granularity": raw["granularity"].strip(),
                "entity_vid": raw["entity_vid"].strip() or None,
                "period_start": period_start,
                "period_end": period_end,
                "dims": json.loads(raw["dims"] or "{}"),
                "value": float(raw["value"]),
                "sample_n": int(sample) if sample else None,
                "revision": int(raw["revision"] or 0),
            })
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            errors.append(f"line {lineno}: {exc}")
    return rows, errors


async def load(rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Upsert indicators first, then their observations."""
    repo = StatsRepository()
    ids: dict[tuple[str, str], int] = {}
    for r in rows:
        key = (r["source"], r["code"])
        if key not in ids:
            ids[key] = await repo.upsert_indicator(
                source=r["source"], code=r["code"], title=r["title"],
                unit=r["unit"], value_kind=r["value_kind"],
                granularity=r["granularity"],
                question_text=r["question_text"], entity_vid=r["entity_vid"],
            )
    observations = [
        {
            "indicator_id": ids[(r["source"], r["code"])],
            "period_start": r["period_start"], "period_end": r["period_end"],
            "dims": r["dims"], "value": r["value"],
            "sample_n": r["sample_n"], "revision": r["revision"],
            "source_doc_id": None,
        }
        for r in rows
    ]
    n = await repo.upsert_observations(observations)
    return len(ids), n


def main() -> None:
    configure_logging()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("path", type=Path)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    rows, errors = parse_csv(args.path.read_text(encoding="utf-8"))
    for e in errors:
        logger.error("stat_import  {e}", e=e)
    if not rows:
        raise SystemExit("nothing to load")
    if args.dry_run:
        logger.info(
            "stat_import  dry-run: {n} rows parsed, {e} rejected",
            n=len(rows), e=len(errors),
        )
        return
    n_ind, n_obs = asyncio.run(load(rows))
    logger.info(
        "stat_import  loaded {i} indicators / {o} observations ({e} rejected)",
        i=n_ind, o=n_obs, e=len(errors),
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_stats/test_import.py -v`
Expected: PASS (5 tests)

- [ ] **Step 6: Verify end to end against the live database**

Run: `uv run python -m scripts.stat_import tests/test_stats/fixtures/fom_sample.csv`
Expected: `loaded 1 indicators / 3 observations (0 rejected)`.

Run it a **second time**: the counts are identical and no error appears — the upsert is idempotent.

- [ ] **Step 7: Lint**

Run: `uv run ruff check scripts/stat_import.py tests/test_stats`

- [ ] **Step 8: Commit**

```bash
git add scripts/stat_import.py tests/test_stats/test_import.py tests/test_stats/fixtures
git commit -m "feat(stats): add CSV import CLI for external statistics"
```

---

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

1. `uv run python -m scripts.stat_import tests/test_stats/fixtures/fom_sample.csv`
2. Start MCP-3: `uv run python -m src.mcp.stats_server --transport http --port 9003`
3. From an MCP client: `stat_indicators_search("тревожность")` → note the `id`
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

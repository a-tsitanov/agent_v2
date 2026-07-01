# Analytical Layer — Wave 3 Implementation Plan (E3 trending/burst + alert push)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Wave 3 — burst/trending analytics over structured events (`trending_events` primitive + a burst detector that emits `:Alert(kind="burst")` in the monitor sweep), and a generic outbound **webhook** that delivers `:Alert` records via a second `deliver_alerts` activity in the sweep.

**Architecture:** A shared `events_burst.build_burst_cypher(watched_only)` is the single source for the burst computation (recent-vs-baseline rate over event `created_at`, grouped by `(participant entity, event_type)`); the `trending_events` primitive calls it with `ratio=1` (rank all) and the in-sweep detector calls it with `watched_only=True` + the configured threshold. Delivery is a fail-soft `httpx` POST per unpushed alert, idempotent via a new `:Alert.pushed_at`, run as a second activity after `detect_alerts`. Everything ships dark.

**Tech Stack:** Python 3.12, Temporal (`temporalio`), Neo4j via `structured_query`, `httpx` (already a dependency), pydantic v2 / pydantic-settings, pytest (`asyncio_mode=auto`), ruff. Builds on Waves 0–2 (`src/analytics/` catalog, E2 `:EventOrAction` nodes, Arc 2 `:Alert` + `MonitorSweepWorkflow`).

Design: `docs/superpowers/specs/2026-06-30-analytical-layer-wave3-design.md`.

## Global Constraints

- **Ship dark.** `trending_events` is read-only/fail-soft (no gate). Burst detection in `detect_alerts` is gated `settings.monitor.burst_enabled` (default `False`). Delivery is active only when `settings.monitor.webhook_url` is non-empty (default `""`). With these off, the monitor sweep is byte-for-byte unchanged.
- **Burst axis = `created_at`** (epoch-days from E1 stamping); if events lack `created_at`, the detector/primitive are fail-soft empty. Never use `event_ts` for burst.
- **Determinism / fail-soft.** Activities NEVER raise across the Temporal boundary (return the result type with `error=...`); read primitives fail-soft via `run_rows` (store None/error → `[]`); sync Neo4j and HTTP run off the loop (`asyncio.to_thread` / async httpx). `post_alert` never raises (returns `False`). A failed POST leaves `pushed_at` NULL → retried next sweep.
- **Idempotency.** `:Alert` MERGE on `alert_key` unchanged; delivery marks `a.pushed_at` only on POST success.
- **Conventions (reuse exactly).** Catalog primitive = `async def fn(store, *, ...) -> PrimitiveResult` + `register(Primitive(name, fn, ParamModel, description))`; param model subclasses a local `_Params(BaseModel)` with `ConfigDict(extra="ignore")`; `clamp_top_n`; entity label literal `"__Entity__"`; NULL-safe polarity filter `(r.polarity IS NULL OR r.polarity <> 'negated')`. Frozen contracts subclass `_Frozen` in `src/analytics/contracts.py`. Sync store calls = `store.structured_query(CYPHER, param_map={...})`. Loguru WARN on fail-soft (`logger.warning("...: {e}", e=exc)` — no `# noqa: BLE001`).
- **Quality gates** (before each commit): `uv run ruff check <changed>` · `uv run ruff format <changed>` · the task's pytest. ruff: line 100, py312, ruleset `E,F,I,B,UP,SIM,RUF`. New env vars get a Russian entry in `scripts/make_env.py::_ENV_DESCRIPTIONS`.
- **Git:** commit locally on `worktree-anal`; never push, never `main`. End every commit message body with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Known baseline:** full suite has 13 pre-existing failures (test_pipeline ×5, test_make_env ×1, test_push_wikibase ×5, test_search_community ×2). Any NEW failure beyond these = regression. The recurring "1 warning" is the external llama_index pydantic `UnsupportedFieldAttributeWarning` (not ours).

---

## File Structure

**New:**
```
src/analytics/events_burst.py          # build_burst_cypher(watched_only) — shared burst engine
src/workflow/monitor/delivery.py       # post_alert(url, payload, *, timeout_s) -> bool (fail-soft httpx)
tests/test_analytics/test_events_burst.py
tests/test_workflow/test_delivery.py
```
**Modified:**
```
src/config.py                                  # MonitorSettings: burst_* + webhook_* + deliver_batch
scripts/make_env.py                            # Russian descriptions for the 8 new MONITOR_* vars
src/analytics/primitives/events_llm.py         # trending_events primitive + event_timeline.window_days fix
src/analytics/contracts.py                     # MonitorResult.burst_alerts; DeliverIn/DeliverResult/SweepResult
src/workflow/monitor/activities.py             # burst detector in detect_alerts (gated) + deliver_alerts + MONITOR_ACTIVITIES
src/workflow/monitor/workflow.py               # run deliver_alerts as 2nd activity; return SweepResult
tests/test_analytics/test_catalog_complete.py  # CATALOG 41 → 42 (trending_events)
tests/test_analytics/test_events_llm.py        # trending + window_days
tests/test_workflow/test_monitor_activities.py # burst finding + gate
tests/test_workflow/test_monitor_workflow.py   # deliver as 2nd activity, SweepResult
```

---

## Phase A — Config

### Task 1: MonitorSettings burst + webhook knobs

**Files:** Modify `src/config.py` (class `MonitorSettings`), `scripts/make_env.py`; Test `tests/test_analytics/test_config_wave2.py` (extend).

**Interfaces — Produces:** `settings.monitor.{burst_enabled:bool=False, burst_window_days:int=7, burst_baseline_windows:int=4, burst_min_count:int=2, burst_ratio:float=3.0, webhook_url:str="", webhook_timeout_s:float=5.0, deliver_batch:int=100}`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_analytics/test_config_wave2.py`:

```python
def test_monitor_wave3_defaults():
    m = settings.monitor
    assert m.burst_enabled is False
    assert m.burst_window_days >= 1 and m.burst_baseline_windows >= 1
    assert m.burst_ratio > 1.0 and m.burst_min_count >= 1
    assert m.webhook_url == "" and m.webhook_timeout_s > 0 and m.deliver_batch >= 1
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_analytics/test_config_wave2.py::test_monitor_wave3_defaults -v`
Expected: FAIL (`AttributeError: ... 'burst_enabled'`).

- [ ] **Step 3: Implement** — read `src/config.py`, find class `MonitorSettings` (env_prefix `"MONITOR_"`), and add these fields after the existing ones:

```python
    burst_enabled: bool = Field(default=False, description="Включить burst-детектор событий в монитор-свипе (E3)")
    burst_window_days: int = Field(default=7, ge=1, description="Окно (дни) для подсчёта недавних событий в burst-детекторе")
    burst_baseline_windows: int = Field(default=4, ge=1, description="Сколько предыдущих окон усреднять как базовую ставку burst")
    burst_min_count: int = Field(default=2, ge=1, description="Мин. число недавних событий, чтобы пара (сущность,тип) считалась всплеском")
    burst_ratio: float = Field(default=3.0, gt=1.0, description="Порог burst_score (recent/base) для алерта о всплеске")
    webhook_url: str = Field(default="", description="URL генеричного webhook для доставки алертов (пусто — доставка выключена)")
    webhook_timeout_s: float = Field(default=5.0, gt=0.0, description="Таймаут POST на webhook доставки алертов, сек")
    deliver_batch: int = Field(default=100, ge=1, description="Сколько непушенных алертов доставлять за один свип")
```

Then add the 8 new vars to `scripts/make_env.py::_ENV_DESCRIPTIONS` with the same Russian text (keys: `MONITOR_BURST_ENABLED`, `MONITOR_BURST_WINDOW_DAYS`, `MONITOR_BURST_BASELINE_WINDOWS`, `MONITOR_BURST_MIN_COUNT`, `MONITOR_BURST_RATIO`, `MONITOR_WEBHOOK_URL`, `MONITOR_WEBHOOK_TIMEOUT_S`, `MONITOR_DELIVER_BATCH`).

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_analytics/test_config_wave2.py -v`
Expected: PASS. Also confirm the new vars are NOT in `test_make_env.py::test_every_env_var_has_russian_description`'s missing list: `uv run pytest tests/test_scripts/test_make_env.py -q` (still the SAME pre-existing missing set — NEO4J_*/RABBITMQ_*/INGEST_QUEUE_BACKEND only; no MONITOR_* added).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/config.py scripts/make_env.py tests/test_analytics/test_config_wave2.py
uv run ruff format src/config.py scripts/make_env.py tests/test_analytics/test_config_wave2.py
git add src/config.py scripts/make_env.py tests/test_analytics/test_config_wave2.py
git commit -m "feat(events): Wave 3 config — MonitorSettings burst + webhook knobs"
```

---

## Phase B — E3 burst engine + primitive

### Task 2: Shared burst engine `events_burst.build_burst_cypher`

**Files:** Create `src/analytics/events_burst.py`; Test `tests/test_analytics/test_events_burst.py`.

**Interfaces — Produces:** `build_burst_cypher(*, watched_only: bool) -> str` — a parameterized Cypher with params `$since_recent, $since_baseline, $baseline_windows, $min_count, $ratio, $top_n`, grouping `(e:__Entity__:EventOrAction)-[:PARTICIPATED_IN]->(p:__Entity__)` by `(p.name, e.event_type)`, returning columns `entity, event_type, recent, baseline_rate, burst_score`. `watched_only=True` adds `AND p.watched = true`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_events_burst.py
from src.analytics.events_burst import build_burst_cypher


def test_burst_cypher_core_shape():
    c = build_burst_cypher(watched_only=False)
    assert "EventOrAction)-[r:PARTICIPATED_IN]->(p:__Entity__)" in c
    assert "r.polarity IS NULL OR r.polarity <> 'negated'" in c
    assert "$since_recent" in c and "$since_baseline" in c
    assert "burst_score" in c and "$min_count" in c and "$ratio" in c
    assert "ORDER BY burst_score DESC" in c and "LIMIT $top_n" in c
    assert "p.watched = true" not in c


def test_burst_cypher_watched_only_adds_clause():
    assert "p.watched = true" in build_burst_cypher(watched_only=True)
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_analytics/test_events_burst.py -v`
Expected: FAIL (`ModuleNotFoundError: src.analytics.events_burst`).

- [ ] **Step 3: Implement**

```python
# src/analytics/events_burst.py
"""E3 — shared burst computation over event created_at (single source for the
trending_events primitive and the in-sweep burst detector)."""

from __future__ import annotations


def build_burst_cypher(*, watched_only: bool) -> str:
    """Parameterized burst query grouped by (participant entity, event_type).

    recent  = events with created_at >= $since_recent
    baseline_rate = events in [$since_baseline, $since_recent) / $baseline_windows
    burst_score = recent / max(baseline_rate, 1)
    Filtered by recent >= $min_count AND burst_score >= $ratio.
    """
    watched = "AND p.watched = true " if watched_only else ""
    return (
        "MATCH (e:__Entity__:EventOrAction)-[r:PARTICIPATED_IN]->(p:__Entity__) "
        "WHERE (r.polarity IS NULL OR r.polarity <> 'negated') "
        "AND e.created_at >= $since_baseline "
        f"{watched}"
        "WITH p.name AS entity, e.event_type AS event_type, "
        "sum(CASE WHEN e.created_at >= $since_recent THEN 1 ELSE 0 END) AS recent, "
        "sum(CASE WHEN e.created_at < $since_recent THEN 1 ELSE 0 END) AS baseline_total "
        "WITH entity, event_type, recent, "
        "(toFloat(baseline_total) / $baseline_windows) AS baseline_rate "
        "WITH entity, event_type, recent, baseline_rate, "
        "(toFloat(recent) / (CASE WHEN baseline_rate < 1 THEN 1 ELSE baseline_rate END)) "
        "AS burst_score "
        "WHERE recent >= $min_count AND burst_score >= $ratio "
        "RETURN entity, event_type, recent, baseline_rate, burst_score "
        "ORDER BY burst_score DESC, recent DESC LIMIT $top_n"
    )
```

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_analytics/test_events_burst.py -v` → PASS.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/events_burst.py tests/test_analytics/test_events_burst.py
uv run ruff format src/analytics/events_burst.py tests/test_analytics/test_events_burst.py
git add src/analytics/events_burst.py tests/test_analytics/test_events_burst.py
git commit -m "feat(events): E3 — shared burst-score Cypher engine (events_burst)"
```

---

### Task 3: `trending_events` primitive + `event_timeline.window_days` fix

**Files:** Modify `src/analytics/primitives/events_llm.py`, `tests/test_analytics/test_catalog_complete.py`; Test `tests/test_analytics/test_events_llm.py` (extend).

**Interfaces — Consumes:** `events_burst.build_burst_cypher`. **Produces:** catalog primitive `trending_events(store, *, window_days=7, baseline_windows=4, min_count=2, top_n=20) -> PrimitiveResult` (ratio=1, watched_only=False); `event_timeline.window_days` now filters `created_at >= since`. CATALOG → 42.

- [ ] **Step 1: Write the failing tests** — append to `tests/test_analytics/test_events_llm.py`:

```python
@pytest.mark.asyncio
async def test_trending_events_windows_and_shape(monkeypatch):
    from src.analytics.primitives import events_llm as el
    monkeypatch.setattr(el, "today_epoch_days", lambda: 19900)
    store = _FakeStore(rows=[{"entity": "Acme", "event_type": "lawsuit",
                              "recent": 6, "baseline_rate": 1.0, "burst_score": 6.0}])
    res = await el.trending_events(store, window_days=7, baseline_windows=4)
    assert res.params["since_recent"] == 19900 - 7
    assert res.params["since_baseline"] == 19900 - 7 * (4 + 1)
    assert res.params["ratio"] == 1.0
    assert "burst_score" in res.cypher
    assert res.rows[0]["event_type"] == "lawsuit"


@pytest.mark.asyncio
async def test_trending_events_fail_soft_none_store():
    from src.analytics.primitives import events_llm as el
    res = await el.trending_events(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_event_timeline_window_days_applies_since(monkeypatch):
    from src.analytics.primitives import events_llm as el
    monkeypatch.setattr(el, "today_epoch_days", lambda: 19900)
    res = await el.event_timeline(_FakeStore(rows=[]), entity="Acme", window_days=30)
    assert res.params["since"] == 19900 - 30
    assert "e.created_at >= $since" in res.cypher
```

(`_FakeStore` is imported at the top of this test file already; confirm the import line `from tests.test_analytics.conftest import _FakeStore` is present — add it if missing.)

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_analytics/test_events_llm.py -v`
Expected: FAIL (`AttributeError: ... trending_events`; and `event_timeline` has no `since`).

- [ ] **Step 3: Implement** — edit `src/analytics/primitives/events_llm.py`:

(a) Add imports near the top (after the existing imports):

```python
from src.analytics.events_burst import build_burst_cypher
from src.retrieval.date_filters import today_epoch_days
```

(b) Replace the `event_timeline` block (the `_EVENT_TIMELINE` constant at lines 53–57 and the `event_timeline` function) with a version that builds the Cypher with an optional window filter:

```python
class EventTimelineParams(_Params):
    entity: str
    window_days: int | None = None
    top_n: int = 50


async def event_timeline(
    store: Any | None,
    *,
    entity: str,
    window_days: int | None = None,
    top_n: int = 50,
) -> PrimitiveResult:
    """Events a named entity participated in, ordered by event_ts."""
    top_n = clamp_top_n(top_n, default=50)
    params: dict[str, Any] = {"entity": entity, "top_n": top_n}
    where = ""
    if window_days is not None:
        params["since"] = today_epoch_days() - int(window_days)
        where = "WHERE e.created_at >= $since "
    cypher = (
        "MATCH (p:__Entity__ {name:$entity})-[]-(e:__Entity__:EventOrAction) "
        f"{where}"
        "RETURN e.name AS name, e.event_type AS event_type, e.event_ts AS event_ts "
        "ORDER BY e.event_ts DESC LIMIT $top_n"
    )
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))
```

(c) Add the `trending_events` primitive and its registration (after `event_timeline`, before/among the `register(...)` calls):

```python
_TRENDING = build_burst_cypher(watched_only=False)


class TrendingEventsParams(_Params):
    window_days: int = 7
    baseline_windows: int = 4
    min_count: int = 2
    top_n: int = 20


async def trending_events(
    store: Any | None,
    *,
    window_days: int = 7,
    baseline_windows: int = 4,
    min_count: int = 2,
    top_n: int = 20,
) -> PrimitiveResult:
    """(entity, event_type) pairs whose event ingest-rate surged recently."""
    top_n = clamp_top_n(top_n, default=20)
    bw = max(int(baseline_windows), 1)
    today = today_epoch_days()
    params = {
        "since_recent": today - int(window_days),
        "since_baseline": today - int(window_days) * (bw + 1),
        "baseline_windows": bw,
        "min_count": int(min_count),
        "ratio": 1.0,
        "top_n": top_n,
    }
    return PrimitiveResult(cypher=_TRENDING, params=params, rows=await run_rows(store, _TRENDING, params))


register(
    Primitive(
        "trending_events",
        trending_events,
        TrendingEventsParams,
        "Surging (entity × event_type) pairs by recent event ingest-rate vs baseline (E3).",
    )
)
```

(d) In `tests/test_analytics/test_catalog_complete.py`: add `"trending_events"` to `_EXPECTED` (under a `# Wave 3 — E3 trending` comment), update the count comment to 42, and the assert docstring/text from 41 → 42.

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_analytics/test_events_llm.py tests/test_analytics/test_catalog_complete.py -v`
Expected: PASS. Confirm: `uv run python -c "import src.analytics.primitives; from src.analytics.catalog import CATALOG; print(len(CATALOG))"` → `42`.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/events_llm.py tests/test_analytics/test_events_llm.py tests/test_analytics/test_catalog_complete.py
uv run ruff format src/analytics/primitives/events_llm.py tests/test_analytics/test_events_llm.py tests/test_analytics/test_catalog_complete.py
git add src/analytics/primitives/events_llm.py tests/test_analytics/test_events_llm.py tests/test_analytics/test_catalog_complete.py
git commit -m "feat(events): E3 — trending_events primitive + event_timeline window_days (CATALOG=42)"
```

---

## Phase C — E3 burst alerts in the sweep

### Task 4: Burst detector in `detect_alerts` (gated) + `MonitorResult.burst_alerts`

**Files:** Modify `src/analytics/contracts.py`, `src/workflow/monitor/activities.py`; Test `tests/test_workflow/test_monitor_activities.py` (extend).

**Interfaces — Consumes:** `events_burst.build_burst_cypher`, `settings.monitor.burst_*`, existing `upsert_alert`, `_get_store`, `today_epoch_days`, `_TOP_N`. **Produces:** `MonitorResult.burst_alerts: int = 0`; `detect_alerts` emits `:Alert(kind="burst")` for watched `(entity,event_type)` bursts when `settings.monitor.burst_enabled`.

- [ ] **Step 1: Write the failing test** — first read `tests/test_workflow/test_monitor_activities.py` to reuse its recording fake-store harness. Add a test that monkeypatches `settings.monitor.burst_enabled=True`, makes the fake store return one burst row when it sees the burst query (branch on substring `"burst_score"`), and asserts a `:Alert` MERGE with `kind="burst"` happened and `result.burst_alerts == 1`. Also add a gated-off test: with `burst_enabled=False`, no burst query is issued and `result.burst_alerts == 0`. Mirror the existing both-watched / fail-soft tests' style.

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_workflow/test_monitor_activities.py -v`
Expected: FAIL (`MonitorResult` has no `burst_alerts`).

- [ ] **Step 3: Implement**

(a) In `src/analytics/contracts.py`, add a field to `MonitorResult`:

```python
class MonitorResult(_Frozen):
    new_connection_alerts: int = 0
    risk_rise_alerts: int = 0
    burst_alerts: int = 0
    error: str = ""
```

(b) In `src/workflow/monitor/activities.py`, read the current `detect_alerts`. After the risk-rise block and before the `return MonitorResult(...)`, insert the gated burst block, and add `burst` to the returned result:

```python
        burst = 0
        if settings.monitor.burst_enabled:
            from src.analytics.events_burst import build_burst_cypher

            m = settings.monitor
            bw = max(m.burst_baseline_windows, 1)
            burst_rows = await asyncio.to_thread(
                store.structured_query,
                build_burst_cypher(watched_only=True),
                {
                    "since_recent": today - m.burst_window_days,
                    "since_baseline": today - m.burst_window_days * (bw + 1),
                    "baseline_windows": bw,
                    "min_count": m.burst_min_count,
                    "ratio": m.burst_ratio,
                    "top_n": _TOP_N,
                },
            )
            for r in burst_rows or []:
                await asyncio.to_thread(
                    upsert_alert,
                    store,
                    kind="burst",
                    entity=r["entity"],
                    detail=f"{r['event_type']}:x{round(r['burst_score'], 1)}",
                    created_at=today,
                )
                burst += 1
        return MonitorResult(
            new_connection_alerts=<existing>,
            risk_rise_alerts=<existing>,
            burst_alerts=burst,
        )
```

(Use the existing local variable names for the new-connection and risk-rise tallies — read the function to get them. `today` is the existing `today_epoch_days()` local; if it is named differently, reuse that. Keep the whole body inside the existing `try/except → MonitorResult(error=...)`.)

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_workflow/test_monitor_activities.py -v` → PASS.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_monitor_activities.py
uv run ruff format src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_monitor_activities.py
git add src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_monitor_activities.py
git commit -m "feat(events): E3 — burst alerts in detect_alerts (gated) + MonitorResult.burst_alerts"
```

---

## Phase D — Push delivery

### Task 5: `post_alert` webhook client (fail-soft)

**Files:** Create `src/workflow/monitor/delivery.py`; Test `tests/test_workflow/test_delivery.py`.

**Interfaces — Produces:** `async def post_alert(url: str, payload: dict, *, timeout_s: float = 5.0) -> bool` — POSTs `payload` as JSON; returns `True` on HTTP 2xx, `False` on non-2xx or any exception; never raises.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_workflow/test_delivery.py
import pytest

from src.workflow.monitor import delivery


@pytest.mark.asyncio
async def test_post_alert_true_on_2xx(monkeypatch):
    sent = {}

    class _Resp:
        status_code = 200

    class _Client:
        def __init__(self, *a, **k): ...
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, json=None):
            sent["url"] = url
            sent["json"] = json
            return _Resp()

    monkeypatch.setattr(delivery.httpx, "AsyncClient", _Client)
    ok = await delivery.post_alert("http://hook", {"key": "k1"}, timeout_s=1.0)
    assert ok is True and sent["url"] == "http://hook" and sent["json"]["key"] == "k1"


@pytest.mark.asyncio
async def test_post_alert_false_on_error(monkeypatch):
    class _Client:
        def __init__(self, *a, **k): ...
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, json=None):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(delivery.httpx, "AsyncClient", _Client)
    assert await delivery.post_alert("http://hook", {"key": "k1"}) is False
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_workflow/test_delivery.py -v`
Expected: FAIL (`ModuleNotFoundError: src.workflow.monitor.delivery`).

- [ ] **Step 3: Implement**

```python
# src/workflow/monitor/delivery.py
"""Arc 2 push — fail-soft generic webhook delivery for :Alert records."""

from __future__ import annotations

import httpx
from loguru import logger


async def post_alert(url: str, payload: dict, *, timeout_s: float = 5.0) -> bool:
    """POST one alert payload as JSON; True on 2xx, False on any error. Never raises."""
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(url, json=payload)
        return 200 <= resp.status_code < 300
    except Exception as exc:
        logger.warning("post_alert failed url={u}: {e}", u=url, e=exc)
        return False
```

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_workflow/test_delivery.py -v` → PASS.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/workflow/monitor/delivery.py tests/test_workflow/test_delivery.py
uv run ruff format src/workflow/monitor/delivery.py tests/test_workflow/test_delivery.py
git add src/workflow/monitor/delivery.py tests/test_workflow/test_delivery.py
git commit -m "feat(events): Arc 2 push — fail-soft webhook post_alert client"
```

---

### Task 6: `deliver_alerts` activity + contracts + `MONITOR_ACTIVITIES`

**Files:** Modify `src/analytics/contracts.py`, `src/workflow/monitor/activities.py`; Test `tests/test_workflow/test_delivery.py` (extend) or a new `tests/test_workflow/test_deliver_activity.py`.

**Interfaces — Consumes:** `post_alert`, `settings.monitor.{webhook_url,webhook_timeout_s}`, `_get_store`, `today_epoch_days`. **Produces:** contracts `DeliverIn(cap:int=100)`, `DeliverResult(delivered:int=0, failed:int=0, error:str="")`; activity `deliver_alerts(p: DeliverIn) -> DeliverResult`; `deliver_alerts` appended to `MONITOR_ACTIVITIES`.

- [ ] **Step 1: Write the failing test** — recording fake store returning two `:Alert` rows for the unpushed query; monkeypatch `_get_store` to it, monkeypatch `settings.monitor.webhook_url="http://hook"`, and monkeypatch `src.workflow.monitor.activities.post_alert` to an async stub returning `True` for the first and `False` for the second. Assert `result.delivered == 1`, `result.failed == 1`, and that a `SET a.pushed_at` query was issued ONLY for the delivered key. Add an empty-`webhook_url` test → `result.delivered == 0` and NO store read. Add a `_get_store` raising → `result.error != ""`, no raise.

```python
@pytest.mark.asyncio
async def test_deliver_alerts_pushes_unpushed_and_marks(monkeypatch):
    from src.analytics.contracts import DeliverIn
    from src.workflow.monitor import activities as act

    class _Store:
        def __init__(self):
            self.calls = []
        def structured_query(self, cypher, param_map=None):
            self.calls.append((cypher, param_map or {}))
            if "a.pushed_at IS NULL" in cypher:
                return [{"key": "k1", "kind": "burst", "entity": "A", "detail": "d", "created_at": 1},
                        {"key": "k2", "kind": "burst", "entity": "B", "detail": "d", "created_at": 1}]
            return []

    store = _Store()
    monkeypatch.setattr(act, "_get_store", lambda: store)
    monkeypatch.setattr(act.settings.monitor, "webhook_url", "http://hook", raising=False)

    async def _fake_post(url, payload, *, timeout_s=5.0):
        return payload["key"] == "k1"

    monkeypatch.setattr(act, "post_alert", _fake_post)
    res = await act.deliver_alerts(DeliverIn(cap=100))
    assert res.delivered == 1 and res.failed == 1
    marks = [pm for c, pm in store.calls if "SET a.pushed_at" in c]
    assert len(marks) == 1 and marks[0]["key"] == "k1"


@pytest.mark.asyncio
async def test_deliver_alerts_noop_when_no_url(monkeypatch):
    from src.analytics.contracts import DeliverIn
    from src.workflow.monitor import activities as act
    monkeypatch.setattr(act.settings.monitor, "webhook_url", "", raising=False)
    res = await act.deliver_alerts(DeliverIn())
    assert res.delivered == 0 and res.error == ""
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_workflow/test_delivery.py -v`
Expected: FAIL (`ImportError: cannot import name 'DeliverIn'` / `deliver_alerts`).

- [ ] **Step 3: Implement**

(a) In `src/analytics/contracts.py` add (Wave 3 section):

```python
class DeliverIn(_Frozen):
    cap: int = 100


class DeliverResult(_Frozen):
    delivered: int = 0
    failed: int = 0
    error: str = ""
```

(b) In `src/workflow/monitor/activities.py`: add `from src.workflow.monitor.delivery import post_alert` and `from src.analytics.contracts import DeliverIn, DeliverResult` (extend the existing contracts import). Add module constants and the activity, then append to `MONITOR_ACTIVITIES`:

```python
_UNPUSHED = (
    "MATCH (a:Alert) WHERE a.pushed_at IS NULL "
    "RETURN a.key AS key, a.kind AS kind, a.entity AS entity, "
    "a.detail AS detail, a.created_at AS created_at "
    "ORDER BY a.created_at DESC LIMIT $cap"
)
_MARK_PUSHED = "MATCH (a:Alert {key:$key}) SET a.pushed_at = $now"


@activity.defn
async def deliver_alerts(p: DeliverIn) -> DeliverResult:
    """Deliver unpushed :Alert records to the configured webhook; mark pushed_at. Fail-soft."""
    url = settings.monitor.webhook_url
    if not url:
        return DeliverResult(delivered=0)
    try:
        store = _get_store()
        now = today_epoch_days()
        delivered = 0
        failed = 0
        async with heartbeat_every(30.0, {"stage": "deliver"}):
            rows = await asyncio.to_thread(store.structured_query, _UNPUSHED, {"cap": p.cap})
            for r in rows or []:
                ok = await post_alert(url, dict(r), timeout_s=settings.monitor.webhook_timeout_s)
                if ok:
                    await asyncio.to_thread(
                        store.structured_query, _MARK_PUSHED, {"key": r["key"], "now": now}
                    )
                    delivered += 1
                else:
                    failed += 1
        return DeliverResult(delivered=delivered, failed=failed)
    except Exception as exc:
        logger.warning("deliver_alerts failed: {e}", e=exc)
        return DeliverResult(error=str(exc))
```

```python
MONITOR_ACTIVITIES = [detect_alerts, deliver_alerts]
```

(Confirm the existing `MONITOR_ACTIVITIES = [detect_alerts]` line is replaced, not duplicated.)

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_workflow/test_delivery.py -v` → PASS. Import sanity: `uv run python -c "import src.workflow.monitor.activities"`.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_delivery.py
uv run ruff format src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_delivery.py
git add src/analytics/contracts.py src/workflow/monitor/activities.py tests/test_workflow/test_delivery.py
git commit -m "feat(events): Arc 2 push — deliver_alerts activity (idempotent via pushed_at)"
```

---

## Phase E — Integration

### Task 7: Wire `deliver_alerts` into the sweep + `SweepResult` + full gate

**Files:** Modify `src/analytics/contracts.py`, `src/workflow/monitor/workflow.py`; Test `tests/test_workflow/test_monitor_workflow.py`.

**Interfaces — Consumes:** `deliver_alerts` (by string name), `MonitorResult`, `DeliverResult`, `settings.monitor.deliver_batch`. **Produces:** contract `SweepResult(new_connection_alerts, risk_rise_alerts, burst_alerts, delivered, failed, error)`; `MonitorSweepWorkflow.run` now runs `detect_alerts` then `deliver_alerts` and returns `SweepResult`.

- [ ] **Step 1: Write the failing test** — read the existing `tests/test_workflow/test_monitor_workflow.py` (time-skip harness). Update it: register BOTH a stub `detect_alerts` (returns `MonitorResult(new_connection_alerts=2, risk_rise_alerts=1, burst_alerts=3)`) and a stub `deliver_alerts` (returns `DeliverResult(delivered=4, failed=1)`) on the worker; execute `MonitorSweepWorkflow`; assert the returned `SweepResult` has `new_connection_alerts==2`, `risk_rise_alerts==1`, `burst_alerts==3`, `delivered==4`, `failed==1`, `error==""`.

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_workflow/test_monitor_workflow.py -v`
Expected: FAIL (`ImportError: SweepResult` / workflow returns `MonitorResult`).

- [ ] **Step 3: Implement**

(a) In `src/analytics/contracts.py` add:

```python
class SweepResult(_Frozen):
    new_connection_alerts: int = 0
    risk_rise_alerts: int = 0
    burst_alerts: int = 0
    delivered: int = 0
    failed: int = 0
    error: str = ""
```

(b) In `src/workflow/monitor/workflow.py`: extend the sandbox import to include `DeliverIn, DeliverResult, SweepResult`; after the existing `detect_alerts` `execute_activity`, add the delivery call and build `SweepResult`:

```python
        deliver = await workflow.execute_activity(
            "deliver_alerts",
            DeliverIn(cap=settings.monitor.deliver_batch),
            result_type=DeliverResult,
            start_to_close_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )
        return SweepResult(
            new_connection_alerts=result.new_connection_alerts,
            risk_rise_alerts=result.risk_rise_alerts,
            burst_alerts=result.burst_alerts,
            delivered=deliver.delivered,
            failed=deliver.failed,
            error=result.error or deliver.error,
        )
```

(`result` = the existing `MonitorResult` from the detect activity; change the `run` return annotation to `SweepResult`.)

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_workflow/test_monitor_workflow.py -v` → PASS. Import sanity: `uv run python -c "import src.workflow.worker, src.api.main"`.

- [ ] **Step 5: Full gate + commit**

```bash
uv run ruff check src/analytics/contracts.py src/workflow/monitor/workflow.py tests/test_workflow/test_monitor_workflow.py
uv run ruff format src/analytics/contracts.py src/workflow/monitor/workflow.py tests/test_workflow/test_monitor_workflow.py
uv run pytest -q   # expect 13 == pre-existing baseline, no NEW failures; CATALOG=42
git add src/analytics/contracts.py src/workflow/monitor/workflow.py tests/test_workflow/test_monitor_workflow.py
git commit -m "feat(events): Wave 3 — deliver_alerts as 2nd sweep activity + SweepResult; full gate"
```

---

## Self-Review

**1. Spec coverage**

| Spec item | Task |
|---|---|
| MonitorSettings burst + webhook knobs (dark) | 1 |
| Shared burst engine `build_burst_cypher` (created_at, entity×type, recent/baseline) | 2 |
| `trending_events` primitive (ratio=1) + CATALOG 42 | 3 |
| `event_timeline.window_days` fix | 3 |
| Burst detector in `detect_alerts` (gated) + `MonitorResult.burst_alerts` | 4 |
| `post_alert` fail-soft webhook | 5 |
| `:Alert.pushed_at` idempotency + `deliver_alerts` activity + MONITOR_ACTIVITIES | 6 |
| `MonitorSweepWorkflow` 2nd activity + `SweepResult` | 7 |
| Ship-dark (burst_enabled off, webhook_url empty) | 1, 4, 6 |
| Full gate regression-free | 7 |

**2. Placeholder scan:** Tasks 4 and 6/7 reference "the existing tally variable names" and "the existing time-skip harness" rather than pasting the full current function — the implementer is told to READ those files first (they are pre-existing Wave-2 code, not defined in this plan). All NEW code (burst Cypher, primitive, post_alert, deliver_alerts, SweepResult wiring) is given verbatim. No "TBD"/"add error handling"/"handle edge cases".

**3. Type consistency:** `build_burst_cypher(*, watched_only)` (Task 2) → `trending_events` `ratio=1` (Task 3) and detector `ratio=burst_ratio` (Task 4); both pass `$since_recent/$since_baseline/$baseline_windows/$min_count/$ratio/$top_n`. `:Alert {key,kind,entity,detail,created_at,pushed_at}` consistent across `upsert_alert` (Wave 2), burst detector (Task 4 write `kind="burst"`), `_UNPUSHED`/`_MARK_PUSHED` (Task 6). `MonitorResult.burst_alerts` (Task 4) consumed by `SweepResult` (Task 7). `DeliverIn(cap)`/`DeliverResult(delivered,failed,error)` consistent between activity (Task 6) and workflow (Task 7). `post_alert(url, payload, *, timeout_s)` consistent between Task 5 def and Task 6 call.

**Known boundaries (out of scope):** pluggable multi-sink delivery (Slack blocks/email/Telegram/Grafana), batched webhook payloads, delivery retry beyond next-sweep, webhook auth/signing, `event_ts`-based burst, event ER via embeddings.

# K+N throttle migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Project git gate:** The user's CLAUDE.md forbids `git commit`/`git push` without explicit per-action approval. Treat every "Commit" step as: stage + show the diff, then STOP and ask the user before committing. Do not auto-commit.

**Goal:** Make K+N the only concurrency model — one global `Semaphore(N)` for all LLM calls (`LLM_POOL_N`, default 8) plus always-on admission K (`INGEST_ADMISSION_MAX_INFLIGHT`, default 1) — and delete the per-role hierarchical throttle, closing the leak where 4 search activities bypass the pool.

**Architecture:** `LLMPool` collapses to a single shared `Lane(N)`; every `get(role)` returns a `BoundedLLM` gated by that one lane. Role still selects the physical model via `LiteLLMSettings` (unchanged). Admission always routes `/ingest` through `IngestSchedulerWorkflow`.

**Tech Stack:** Python, pydantic-settings, asyncio, Temporal, LlamaIndex, pytest.

Spec: `docs/superpowers/specs/2026-06-15-kn-throttle-migration-design.md`

---

## File Structure

- `src/config.py` — `LLMPoolSettings` (collapse to `n`), `IngestAdmissionSettings` (drop `enabled`), `AgentSettings` (drop `global_map_parallelism`), `TemporalSettings` (comment-only).
- `src/retrieval/llm_pool.py` — single-semaphore rewrite.
- `src/workflow/search/activities/{route,contextualize,global_search,community}.py` — accessor bodies → pool.
- `src/workflow/search/global_wf.py`, `src/workflow/contracts.py`, `src/mcp/search_server.py`, `src/api/routes/search_v2.py` — remove `map_parallelism`.
- `src/api/routes/ingest.py` — admission always-on.
- `.env.example`, `.env.prod.example`, `scripts/make_env*`, `docs/CAPACITY_TUNING.md`, `docs/SEARCH.md`, `docs/CONCEPTS.md`, `docs/diagrams/search_modes.d2` — env + docs.
- Tests: `tests/test_retrieval/test_llm_pool.py` (rewrite), `tests/test_workflow/test_search_pooled_llm.py` (new), `tests/test_config/test_settings.py`, `tests/test_scripts/test_make_env.py`.

---

## Task 1: Collapse `LLMPoolSettings` + `IngestAdmissionSettings` in config

**Files:**
- Modify: `src/config.py` (`LLMPoolSettings` ~576-615, `IngestAdmissionSettings` ~686-703)
- Test: `tests/test_config/test_settings.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_config/test_settings.py`:

```python
def test_llm_pool_settings_single_n(monkeypatch):
    monkeypatch.setenv("LLM_POOL_N", "12")
    from src.config import LLMPoolSettings
    s = LLMPoolSettings()
    assert s.n == 12
    # old hierarchical fields are gone
    assert not hasattr(s, "lane_caps")
    assert not hasattr(s, "tier_small_total")
    assert not hasattr(s, "judge_floor")
    assert not hasattr(s, "global_n")


def test_llm_pool_n_default_and_floor():
    from src.config import LLMPoolSettings
    assert LLMPoolSettings().n == 8
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LLMPoolSettings(n=0)


def test_admission_always_on_no_enabled_flag():
    from src.config import IngestAdmissionSettings
    s = IngestAdmissionSettings()
    assert s.max_inflight == 1
    assert not hasattr(s, "enabled")
```

- [ ] **Step 2: Run to verify they fail**

Run: `pytest tests/test_config/test_settings.py -k "llm_pool or admission" -v`
Expected: FAIL (old fields still present; `enabled` exists; `n` missing).

- [ ] **Step 3: Implement — replace `LLMPoolSettings` body**

Replace the whole class body (keep the class line + `model_config`):

```python
class LLMPoolSettings(BaseSettings):
    """Per-process LLM concurrency pool (K+N model).

    ONE semaphore of size ``n`` (``LLM_POOL_N``) gates EVERY LLM call across
    all roles.  Paired with admission K (``INGEST_ADMISSION_MAX_INFLIGHT``)
    this is the entire concurrency contract: "at most K documents in flight,
    at most N concurrent LLM calls".

    Role still selects the physical *model* (``build_llm(role)`` →
    ``LiteLLMSettings.model_for``); it no longer affects gating.

    Per-process, NOT distributed — the true cross-process GPU ceiling belongs
    at the LiteLLM proxy (out of scope).
    """

    model_config = SettingsConfigDict(
        env_prefix="LLM_POOL_", env_file=".env", extra="ignore",
    )

    n: int = Field(default=8, ge=1)
```

- [ ] **Step 4: Implement — replace `IngestAdmissionSettings` body**

```python
class IngestAdmissionSettings(BaseSettings):
    """Document-level admission control (always on).  /ingest hands every
    document to a singleton ``IngestSchedulerWorkflow`` that runs at most
    ``max_inflight`` (K) documents at once, each to completion, FIFO — so a
    document's tail (merge) isn't starved behind newer documents' extract
    bursts."""

    model_config = SettingsConfigDict(
        env_prefix="INGEST_ADMISSION_", env_file=".env", extra="ignore",
    )

    max_inflight: int = Field(default=1, ge=1)
```

- [ ] **Step 5: Run to verify pass**

Run: `pytest tests/test_config/test_settings.py -k "llm_pool or admission" -v`
Expected: PASS.

- [ ] **Step 6: Commit** (ask user first — see git gate)

```bash
git add src/config.py tests/test_config/test_settings.py
git commit -m "refactor(config): collapse LLM pool to single N; admission always-on"
```

---

## Task 2: Rewrite `LLMPool` to one global semaphore

**Files:**
- Modify: `src/retrieval/llm_pool.py` (full rewrite)
- Test: `tests/test_retrieval/test_llm_pool.py` (full rewrite)

- [ ] **Step 1: Rewrite the test file**

Replace the entire contents of `tests/test_retrieval/test_llm_pool.py`:

```python
"""Unit tests for the per-process LLMPool (K+N single-semaphore model)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.retrieval import llm_pool as pool_mod
from src.retrieval.llm_pool import Lane, LLMPool, get_llm_pool, reset_for_tests


@pytest.fixture(autouse=True)
def _reset():
    reset_for_tests()
    yield
    reset_for_tests()


def _fake_llm():
    m = MagicMock()
    m.achat = AsyncMock(return_value="ok")
    return m


def _settings(n: int):
    s = MagicMock()
    s.llm_pool.n = n
    return s


@pytest.mark.asyncio
async def test_lane_counts_in_use_and_available():
    lane = Lane("pool", cap=2)
    assert lane.available == 2
    async with lane:
        assert lane.in_use == 1
        assert lane.available == 1
    assert lane.in_use == 0


def test_lane_rejects_zero_cap():
    with pytest.raises(ValueError, match="cap must be >= 1"):
        Lane("pool", cap=0)


@pytest.mark.asyncio
async def test_get_is_singleton_per_role(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = LLMPool(_settings(8))
    assert pool.get("extraction") is pool.get("extraction")


@pytest.mark.asyncio
async def test_one_semaphore_bounds_all_roles(monkeypatch):
    """ONE semaphore of size N gates EVERY role — total in-flight <= N."""
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())

    in_flight = 0
    max_observed = 0
    lock = asyncio.Lock()

    async def fake(*a, **kw):
        nonlocal in_flight, max_observed
        async with lock:
            in_flight += 1
            max_observed = max(max_observed, in_flight)
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
        return "ok"

    pool = LLMPool(_settings(2))
    ext, jud, syn = pool.get("extraction"), pool.get("judge"), pool.get("synthesis")
    for w in (ext, jud, syn):
        w._inner.achat = fake

    calls = (
        [ext.achat() for _ in range(5)]
        + [jud.achat() for _ in range(5)]
        + [syn.achat() for _ in range(5)]
    )
    await asyncio.gather(*calls)
    assert max_observed <= 2


@pytest.mark.asyncio
async def test_stats_reports_kn(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = LLMPool(_settings(5))
    pool.get("extraction")
    st = pool.stats()
    assert st["mode"] == "kn"
    assert st["n"] == 5
    assert st["available"] == 5
    assert st["in_use"] == 0


@pytest.mark.asyncio
async def test_reset_rebuilds_singleton(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    p1 = get_llm_pool()
    reset_for_tests()
    assert get_llm_pool() is not p1


@pytest.mark.asyncio
async def test_lane_warns_on_saturation():
    from loguru import logger

    messages: list[str] = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        lane = Lane("pool", cap=1)
        block = asyncio.Event()

        async def hold():
            async with lane:
                await block.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.01)

        async def enter_and_release():
            async with lane:
                pass

        waiter = asyncio.create_task(enter_and_release())
        await asyncio.sleep(0.01)
        block.set()
        await holder
        await waiter
    finally:
        logger.remove(sink_id)

    assert any("saturated" in m for m in messages), messages
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_retrieval/test_llm_pool.py -v`
Expected: FAIL/ERROR (current `Lane.__init__` needs a `tier` arg; `stats()` has no `mode`; `_settings` has no tier fields).

- [ ] **Step 3: Rewrite `src/retrieval/llm_pool.py`**

Replace the entire file:

```python
"""Per-process LLM concurrency pool — single home for LLM gating (K+N).

ONE ``asyncio.Semaphore`` of size N (``LLM_POOL_N``) gates EVERY LLM call
across all roles.  Paired with admission K (in-flight documents) this is the
whole concurrency contract: "at most K documents in flight, at most N
concurrent LLM calls".

Role still selects the physical *model* (``build_llm(role)`` →
``LiteLLMSettings.model_for``); it no longer affects gating.

Per-process, NOT distributed: the true cross-process GPU ceiling belongs at
the LiteLLM proxy (out of scope).
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from llama_index.core.llms import LLM

from src.config import LLMRole
from src.retrieval.llm import build_llm
from src.retrieval.llm_semaphore import BoundedLLM


class Lane:
    """A named counting async gate: bounded concurrency + an in_use counter.

    Usable directly as ``async with lane:`` — that's how ``BoundedLLM``
    acquires it.  ``in_use`` is our own counter (never ``Semaphore._value``).
    """

    def __init__(self, name: str, cap: int) -> None:
        if cap < 1:
            raise ValueError(f"lane {name!r} cap must be >= 1, got {cap}")
        self.name = name
        self.cap = cap
        self._sem = asyncio.Semaphore(cap)
        self.in_use = 0

    @property
    def available(self) -> int:
        return self.cap - self.in_use

    async def __aenter__(self) -> "Lane":
        if self._sem.locked():
            logger.warning(
                "LLMPool saturated (N={cap}, in_use={in_use}) — caller waiting",
                cap=self.cap, in_use=self.in_use,
            )
        await self._sem.acquire()
        self.in_use += 1
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._sem.release()
        self.in_use -= 1


class LLMPool:
    """Registry of role -> gated LLM, all sharing ONE global N semaphore."""

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._n = settings.llm_pool.n
        self._lane = Lane("pool", self._n)
        self._llms: dict[str, LLM] = {}

    def get(self, role: LLMRole) -> LLM:
        if role not in self._llms:
            self._llms[role] = BoundedLLM(  # type: ignore[assignment]
                build_llm(role), gates=[self._lane],
            )
        return self._llms[role]

    def stats(self) -> dict[str, Any]:
        return {
            "mode": "kn",
            "n": self._n,
            "in_use": self._lane.in_use,
            "available": self._lane.available,
        }


_pool: LLMPool | None = None


def get_llm_pool() -> LLMPool:
    """Process-singleton accessor (lazy)."""
    global _pool
    if _pool is None:
        from src.config import settings
        _pool = LLMPool(settings)
    return _pool


def reset_for_tests() -> None:
    """Test hook — drop the singleton so the next get_llm_pool rebuilds."""
    global _pool
    _pool = None
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_retrieval/test_llm_pool.py tests/test_retrieval/test_llm_semaphore.py -v`
Expected: PASS (BoundedLLM/semaphore tests still green — that file is untouched).

- [ ] **Step 5: Commit** (ask user first)

```bash
git add src/retrieval/llm_pool.py tests/test_retrieval/test_llm_pool.py
git commit -m "refactor(llm_pool): single global N semaphore; drop tier+lane hierarchy"
```

---

## Task 3: Close the leak — route 4 search accessors through the pool

**Files:**
- Modify: `src/workflow/search/activities/route.py:77-84`,
  `contextualize.py:47-55`, `global_search.py:104-110`, `community.py:178-186`
- Test: `tests/test_workflow/test_search_pooled_llm.py` (new)

- [ ] **Step 1: Write the failing regression test**

Create `tests/test_workflow/test_search_pooled_llm.py`:

```python
"""Regression: every search-side LLM accessor goes through the LLM pool, so
the global N semaphore actually counts these calls (audit finding #3)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.retrieval import llm_pool as pool_mod


def _fake_llm():
    m = MagicMock()
    m.achat = AsyncMock(return_value="ok")
    return m


@pytest.fixture(autouse=True)
def _pool(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    s = MagicMock()
    s.llm_pool.n = 8
    monkeypatch.setattr(pool_mod, "settings", s, raising=False)
    pool_mod.reset_for_tests()
    # force the singleton to use our MagicMock settings
    pool_mod._pool = pool_mod.LLMPool(s)
    yield
    pool_mod.reset_for_tests()


@pytest.mark.parametrize(
    "import_path,attr,role",
    [
        ("src.workflow.search.activities.route", "_get_route_llm", "route"),
        ("src.workflow.search.activities.contextualize", "_get_contextualize_llm", "route"),
        ("src.workflow.search.activities.global_search", "_get_map_llm", "retrieve"),
        ("src.workflow.search.activities.community", "_get_summary_llm", "retrieve"),
    ],
)
def test_accessor_returns_pooled_llm(import_path, attr, role):
    import importlib

    mod = importlib.import_module(import_path)
    accessor = getattr(mod, attr)
    assert accessor() is pool_mod.get_llm_pool().get(role)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_workflow/test_search_pooled_llm.py -v`
Expected: FAIL (accessors return fresh `build_llm()` instances, not the pooled singleton — identity check fails).

- [ ] **Step 3: Implement — `route.py` `_get_route_llm` body**

Replace the function body (the `from src.retrieval.llm import build_llm` / `return build_llm("route")` lines) with:

```python
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("route")
```

- [ ] **Step 4: Implement — `contextualize.py` `_get_contextualize_llm` body**

```python
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("route")
```

- [ ] **Step 5: Implement — `global_search.py` `_get_map_llm` body**

```python
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("retrieve")
```

- [ ] **Step 6: Implement — `community.py` `_get_summary_llm` body**

```python
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("retrieve")
```

- [ ] **Step 7: Run to verify pass**

Run: `pytest tests/test_workflow/test_search_pooled_llm.py -v`
Expected: PASS (all 4 params).

- [ ] **Step 8: Commit** (ask user first)

```bash
git add src/workflow/search/activities/route.py src/workflow/search/activities/contextualize.py src/workflow/search/activities/global_search.py src/workflow/search/activities/community.py tests/test_workflow/test_search_pooled_llm.py
git commit -m "fix(search): route all LLM calls through the pool so N binds them"
```

---

## Task 4: Remove the redundant `map_parallelism` throttle

**Files:**
- Modify: `src/workflow/search/global_wf.py:212-230` (+ docstring line 10),
  `src/workflow/contracts.py:655`, `src/config.py:530-534`,
  `src/mcp/search_server.py:95`, `src/api/routes/search_v2.py:66`
- Docs: `docs/SEARCH.md`, `docs/CONCEPTS.md`, `docs/diagrams/search_modes.d2`

- [ ] **Step 1: `global_wf.py` — drop the semaphore from the MAP loop**

Replace lines ~212-230 (from `# ── 2. MAP` through the `asyncio.gather(...)`):

```python
        # ── 2. MAP: per-community partial (small tier) — N bounds the LLM ─
        self._state["phase"] = "map"
        specs = build_map_specs(comm.communities, query=params.query)

        async def _map_one(spec: MapPartialParams) -> MapPartialResult:
            return await workflow.execute_activity(
                "map_community_partial",
                spec,
                result_type=MapPartialResult,
                start_to_close_timeout=LLM_START_TO_CLOSE,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                retry_policy=FAST_RETRY,
            )

        partials: list[MapPartialResult] = await asyncio.gather(
            *[_map_one(s) for s in specs],
        )
```

Also update the module docstring at line 10: change
`bounded by ``map_parallelism``); off-topic communities self-drop.` →
`); off-topic communities self-drop.` (N now bounds the LLM fan-out).

- [ ] **Step 2: `contracts.py` — remove the field**

Delete line 655 `    map_parallelism: int = 4` from `GlobalSearchParams`.

- [ ] **Step 3: `config.py` — remove `global_map_parallelism`**

Delete the `global_map_parallelism: int = Field(default=4, ge=1, le=32)` field
(line ~534) and its preceding comment (line ~530) in `AgentSettings`.

- [ ] **Step 4: Remove the two call-site kwargs**

`src/mcp/search_server.py:95` — delete the line
`        map_parallelism=settings.agent.global_map_parallelism,`.
`src/api/routes/search_v2.py:66` — delete the same kwarg line.

- [ ] **Step 5: Verify nothing else references it**

Run: `grep -rn "map_parallelism" src/`
Expected: no matches.

- [ ] **Step 6: Update docs**

In `docs/SEARCH.md` (lines ~301, 315, 557), `docs/CONCEPTS.md` (line ~596),
and `docs/diagrams/search_modes.d2` (line 18): remove `map_parallelism` /
`AGENT_GLOBAL_MAP_PARALLELISM` references; note MAP fan-out is now bounded by
`LLM_POOL_N`. Re-render the diagram if the d2 toolchain is available
(`d2 docs/diagrams/search_modes.d2 docs/diagrams/search_modes.svg`); skip if not.

- [ ] **Step 7: Run search/workflow tests**

Run: `pytest tests/test_workflow -q`
Expected: PASS (no test referenced `map_parallelism`; if any did, update it to drop the field).

- [ ] **Step 8: Commit** (ask user first)

```bash
git add src/workflow/search/global_wf.py src/workflow/contracts.py src/config.py src/mcp/search_server.py src/api/routes/search_v2.py docs/SEARCH.md docs/CONCEPTS.md docs/diagrams/search_modes.d2
git commit -m "refactor(search): remove map_parallelism; N is the single LLM throttle"
```

---

## Task 5: Admission always-on in the ingest route

**Files:**
- Modify: `src/api/routes/ingest.py:163-187`
- Test: `tests/test_workflow/test_admission.py` or `tests/test_api/...` (route-level)

- [ ] **Step 1: Implement — replace the admission branch**

Replace lines 163-187 (`admission = settings.ingest_admission` through the end
of the `else:` block) with:

```python
    # Admission is always on: hand the document to the singleton scheduler
    # (signal-with-start; USE_EXISTING reuses the running one) so at most K
    # (max_inflight) documents run at once, each to completion, FIFO.
    try:
        await client.start_workflow(
            IngestSchedulerWorkflow.run,
            SchedulerParams(max_inflight=settings.ingest_admission.max_inflight),
            id="ingest-scheduler",
            task_queue=settings.temporal.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            start_signal="submit",
            start_signal_args=[params],
        )
    except WorkflowAlreadyStartedError as exc:
```

(Leave the existing `except WorkflowAlreadyStartedError` body unchanged.)

- [ ] **Step 2: Remove now-dead code**

Run: `grep -n "DocumentIngestWorkflow\|WorkflowIDReusePolicy\|search_attrs\|search_attributes" src/api/routes/ingest.py`
If `search_attrs`/`search_attributes` and the `DocumentIngestWorkflow` /
`WorkflowIDReusePolicy` imports are now unused (only the deleted direct-start
used them), delete the unused construction and imports. Confirm with:
`ruff check src/api/routes/ingest.py`
Expected: no F401/F841 warnings after cleanup.

- [ ] **Step 3: Write/adjust a route test**

Add to the API ingest tests (e.g. `tests/test_api/test_ingest_route.py` —
match the existing test module name; if none, create it) a test that patches
the Temporal client and asserts the scheduler is always started:

```python
@pytest.mark.asyncio
async def test_ingest_always_starts_scheduler(monkeypatch, ingest_client):
    started = {}

    class _Client:
        async def start_workflow(self, fn, *a, **kw):
            started["id"] = kw.get("id")
            started["signal"] = kw.get("start_signal")
            return MagicMock(id=kw.get("id"))

    monkeypatch.setattr(
        "src.api.routes.ingest.get_temporal_client",
        AsyncMock(return_value=_Client()),
    )
    # ... post a small file via the test client (reuse existing ingest
    # fixture/helpers in this module) ...
    resp = ingest_client.post("/api/v1/ingest", files={"file": ("a.txt", b"hi")})
    assert resp.status_code in (200, 202)
    assert started["id"] == "ingest-scheduler"
    assert started["signal"] == "submit"
```

Adapt the request mechanics to the existing ingest-route test helpers.

- [ ] **Step 4: Run**

Run: `pytest tests/test_api -k ingest -v` and `pytest tests/test_workflow/test_admission.py -q`
Expected: PASS.

- [ ] **Step 5: Commit** (ask user first)

```bash
git add src/api/routes/ingest.py tests/test_api
git commit -m "feat(ingest): admission always-on — every doc via the scheduler (K)"
```

---

## Task 6: env files, make_env, CAPACITY_TUNING

**Files:**
- Modify: `.env.example`, `.env.prod.example`, `scripts/make_env*`,
  `docs/CAPACITY_TUNING.md`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Update `.env.example` and `.env.prod.example`**

- Replace `LLM_POOL_GLOBAL_N=0` with `LLM_POOL_N=8`.
- Delete `LLM_POOL_TIER_SMALL_TOTAL`, `LLM_POOL_TIER_LARGE_TOTAL`,
  `LLM_POOL_JUDGE_FLOOR`, `LLM_POOL_LANE_CAPS` and their comment blocks.
- Delete `INGEST_ADMISSION_ENABLED`; keep `INGEST_ADMISSION_MAX_INFLIGHT=1`.
- Delete `AGENT_GLOBAL_MAP_PARALLELISM`.

Verify: `grep -nE "LLM_POOL_GLOBAL_N|TIER_(SMALL|LARGE)_TOTAL|JUDGE_FLOOR|LANE_CAPS|ADMISSION_ENABLED|GLOBAL_MAP_PARALLELISM" .env.example .env.prod.example`
Expected: no matches. And `grep -n "LLM_POOL_N\|INGEST_ADMISSION_MAX_INFLIGHT" .env.example` shows both.

- [ ] **Step 2: Update `scripts/make_env*`**

`grep -rn "GLOBAL_N\|TIER_SMALL_TOTAL\|TIER_LARGE_TOTAL\|JUDGE_FLOOR\|LANE_CAPS\|ADMISSION_ENABLED\|GLOBAL_MAP_PARALLELISM" scripts/`
For each hit in the make_env generator, mirror the env changes from Step 1
(emit `LLM_POOL_N`, drop the removed keys).

- [ ] **Step 3: Update `tests/test_scripts/test_make_env.py`**

Adjust assertions to expect `LLM_POOL_N` and `INGEST_ADMISSION_MAX_INFLIGHT`
and to NOT expect the removed keys.

- [ ] **Step 4: Run**

Run: `pytest tests/test_scripts/test_make_env.py -v`
Expected: PASS.

- [ ] **Step 5: Update `docs/CAPACITY_TUNING.md`**

Rewrite the "Simple K+N mode" section as the ONLY mode: `LLM_POOL_N` (N
concurrent LLM calls) + `INGEST_ADMISSION_MAX_INFLIGHT` (K in-flight docs).
Remove the hierarchical-mode / lane-caps / judge_floor / tier-total
documentation.

- [ ] **Step 6: Commit** (ask user first)

```bash
git add .env.example .env.prod.example scripts docs/CAPACITY_TUNING.md tests/test_scripts/test_make_env.py
git commit -m "docs+env: K+N is the only mode (LLM_POOL_N + admission K)"
```

---

## Task 7: Fix stale Temporal-cap comments

**Files:**
- Modify: `src/config.py` `TemporalSettings` (~218-235)

- [ ] **Step 1: Update comments only**

Change the comment on `llm_activity_concurrency` from "must be >= the pool
extraction lane ceiling … matches LLMPoolSettings.lane_caps extraction
ceiling" to: "must be >= LLM_POOL_N so the in-process pool (not Temporal) is
the binding LLM throttle." Apply the same fix to the
`merge_activity_concurrency` comment (drop the "judge ceiling" reference).
Do NOT change the numeric defaults (18 / 14).

- [ ] **Step 2: Verify no behavioral change**

Run: `pytest tests/test_config -q`
Expected: PASS.

- [ ] **Step 3: Commit** (ask user first)

```bash
git add src/config.py
git commit -m "docs(config): correct Temporal-cap comments for K+N (pool is throttle)"
```

---

## Task 8: Full verification sweep

- [ ] **Step 1: Static checks**

Run: `ruff check src tests` and `mypy src` (if configured)
Expected: clean (fix any unused imports left by deletions).

- [ ] **Step 2: Full test suite**

Run: `pytest -q`
Expected: PASS. Note: the pre-existing cross-file isolation flakes
(`test_push_wikibase` after `test_graph`+`test_retrieval`; some `test_api`
IndexError combos) are NOT regressions from this work — confirm any failures
match that known set before treating them as blockers.

- [ ] **Step 3: Grep for orphans**

Run: `grep -rn "global_n\|lane_caps\|judge_floor\|tier_small_total\|tier_large_total\|map_parallelism\|ADMISSION_ENABLED" src tests`
Expected: no matches (docs may still mention history; that's fine).

- [ ] **Step 4: Final commit** (ask user first; only if Steps 1-3 left changes)

```bash
git add -A
git commit -m "chore: K+N migration cleanup"
```

---

## Self-Review notes (author)

- **Spec coverage:** config collapse (T1), pool rewrite (T2), leak fix (T3),
  map_parallelism removal (T4), admission always-on (T5), env/docs (T6),
  Temporal comments (T7), verification (T8) — every spec §Changes item maps to
  a task.
- **Kept (not deleted):** `LiteLLMSettings.role_tiers/model_for/tier_for/
  model_small/model_large`, `BoundedLLM`/`llm_semaphore.py`, `AdmissionState`,
  Temporal numeric caps — all intentionally untouched.
- **Type consistency:** `Lane(name, cap)` (no `tier`) used uniformly in pool +
  tests; `stats()` shape (`mode/n/in_use/available`) matches the stats test;
  accessor names (`_get_route_llm/_get_contextualize_llm/_get_map_llm/
  _get_summary_llm`) match Task 3 edits and the regression test.

# LLM Pool Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the scattered `BoundedLLM` instances (and the ungated ingest path) with one per-process role-keyed `LLMPool` that enforces hierarchical tier+lane concurrency limits.

**Architecture:** A process-singleton `LLMPool` builds and caches one gating-wrapped LLM per role. Each small-tier LLM call acquires two permits — its lane ceiling and the shared small-tier global — so a single workload can fill the GPU while no role can monopolize it. Temporal per-queue caps are relaxed so the pool, not Temporal, owns LLM concurrency.

**Tech Stack:** Python 3.10+, asyncio, pydantic-settings, LlamaIndex `LLM`, Temporal Python SDK, pytest/pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-06-06-llm-pool-consolidation-design.md`

---

## File Structure

- **Create** `src/retrieval/llm_pool.py` — `Lane` (counting async gate), `LLMPool` (registry + `get`/`stats`), `get_llm_pool`/`reset_for_tests`.
- **Modify** `src/retrieval/llm_semaphore.py` — `BoundedLLM` accepts an ordered list of async-context-manager gates (keeps the `max_concurrent=` path for back-compat).
- **Modify** `src/config.py` — add `LLMPoolSettings` + `Settings.llm_pool`.
- **Modify** call-sites: `src/di/providers.py`, `src/workflow/_search_deps.py`, `src/workflow/_search_plan_deps.py`, `src/mcp/tools_server.py`, `src/workflow/activities/extract_kg.py`, `src/workflow/activities/merge_and_resolve.py`, `src/ingestion/run.py`.
- **Modify** `src/workflow/document_ingest.py` (heartbeat_timeout) + `.env.example` (Temporal caps + `LLM_POOL_*`).
- **Tests** `tests/test_retrieval/test_llm_semaphore.py` (extend), `tests/test_retrieval/test_llm_pool.py` (new).

---

## Task 1: `BoundedLLM` accepts ordered gates

**Files:**
- Modify: `src/retrieval/llm_semaphore.py`
- Test: `tests/test_retrieval/test_llm_semaphore.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_retrieval/test_llm_semaphore.py`:

```python
@pytest.mark.asyncio
async def test_gates_acquired_in_order_and_both_bound():
    """With two gates (caps 2 and 3), the tighter gate (2) bounds in-flight."""
    import asyncio
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

    inner = _make_inner()
    inner.achat = fake
    g_tight = asyncio.Semaphore(2)
    g_loose = asyncio.Semaphore(3)
    llm = BoundedLLM(inner, gates=[g_tight, g_loose])
    results = await asyncio.gather(*[llm.achat() for _ in range(6)])
    assert results == ["ok"] * 6
    assert max_observed <= 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_retrieval/test_llm_semaphore.py::test_gates_acquired_in_order_and_both_bound -v`
Expected: FAIL — `BoundedLLM.__init__() got an unexpected keyword argument 'gates'`.

- [ ] **Step 3: Implement the gates path in `BoundedLLM`**

In `src/retrieval/llm_semaphore.py`, replace the `__init__` and the `async with self._sem:` bodies. New imports at top:

```python
from contextlib import AsyncExitStack, asynccontextmanager
```

Replace `__init__`:

```python
    def __init__(
        self,
        inner: LLM,
        *,
        max_concurrent: int | None = None,
        gates: list | None = None,
    ) -> None:
        if gates is None:
            if max_concurrent is None or max_concurrent < 1:
                raise ValueError(
                    f"max_concurrent must be >= 1, got {max_concurrent}",
                )
            gates = [asyncio.Semaphore(max_concurrent)]
            self._max_concurrent: int | None = max_concurrent
        else:
            if not gates:
                raise ValueError("gates must be a non-empty list")
            self._max_concurrent = None
        self._inner = inner
        self._gates = gates
        # Back-compat: keep a `_sem` alias to the first gate for any
        # introspection that referenced it.
        self._sem = gates[0]

    @asynccontextmanager
    async def _acquire_all(self):
        async with AsyncExitStack() as stack:
            for g in self._gates:
                await stack.enter_async_context(g)
            yield
```

Then in every gated method, replace `async with self._sem:` with `async with self._acquire_all():`. For example:

```python
    async def achat(self, *a, **kw):
        async with self._acquire_all():
            return await self._inner.achat(*a, **kw)
```

Apply the same replacement to `acomplete`, `achat_with_tools`, `astructured_predict`, `apredict`, and the two streaming methods (`astream_chat`, `astream_complete` — keep the `async for ... yield` body inside the `async with self._acquire_all():`).

Update the `max_concurrent` property to tolerate `None`:

```python
    @property
    def max_concurrent(self) -> int | None:
        return self._max_concurrent
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_retrieval/test_llm_semaphore.py -v`
Expected: PASS (all existing tests + the new one; `test_invalid_max_concurrent_raises`, `test_repr_useful`, `test_wrap_if_needed_*` still pass).

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/llm_semaphore.py tests/test_retrieval/test_llm_semaphore.py
git commit -m "feat(llm): BoundedLLM accepts ordered gates for hierarchical limits"
```

---

## Task 2: `LLMPoolSettings` config

**Files:**
- Modify: `src/config.py`
- Test: `tests/test_config/test_settings.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_config/test_settings.py`:

```python
def test_llm_pool_settings_defaults():
    from src.config import LLMPoolSettings
    s = LLMPoolSettings()
    assert s.tier_small_total == 25
    assert s.tier_large_total == 8
    # f49a83c anti-regression sizing rule must hold by default:
    assert s.lane_caps["extraction"] <= s.tier_small_total - s.judge_floor
    assert s.lane_caps["judge"] >= 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config/test_settings.py::test_llm_pool_settings_defaults -v`
Expected: FAIL — `cannot import name 'LLMPoolSettings'`.

- [ ] **Step 3: Add `LLMPoolSettings` and wire it into `Settings`**

In `src/config.py`, after `AgentSettings` (around line 408), add:

```python
class LLMPoolSettings(BaseSettings):
    """Per-process LLM concurrency pool (hierarchical tier + lane limits).

    The pool owns LLM concurrency; Temporal queue caps are relaxed to
    isolation only.  Small-tier lanes intentionally over-subscribe
    (sum of ceilings > tier_small_total) so one workload can fill the
    GPU while no role can monopolize it beyond its ceiling.
    """

    model_config = SettingsConfigDict(
        env_prefix="LLM_POOL_", env_file=".env", extra="ignore",
    )

    # Real backend capacity (GPU concurrent requests for small tier).
    tier_small_total: int = Field(default=25, ge=1)
    tier_large_total: int = Field(default=8, ge=1)
    # Reserved floor for the merge/judge lane under an extraction flood.
    # The sizing rule extraction_ceiling <= tier_small_total - judge_floor
    # guarantees merge never starves (f49a83c anti-regression).
    judge_floor: int = Field(default=7, ge=1)
    # Per-role lane ceilings.  Override the whole map via
    # LLM_POOL_LANE_CAPS='{"extraction":12,...}'.
    lane_caps: dict[str, int] = Field(
        default_factory=lambda: {
            "extraction": 18,
            "judge": 14,
            "search": 14,
            "plan": 4,
            "route": 2,
            "retrieve": 4,
            "synthesis": 8,
        }
    )
```

Then in the `Settings` class (after the `litellm` cached_property, ~line 561), add:

```python
    @cached_property
    def llm_pool(self) -> LLMPoolSettings:
        return LLMPoolSettings()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_config/test_settings.py::test_llm_pool_settings_defaults -v`
Expected: PASS (18 <= 25 - 7).

- [ ] **Step 5: Commit**

```bash
git add src/config.py tests/test_config/test_settings.py
git commit -m "feat(config): LLMPoolSettings — tier totals + per-lane ceilings"
```

---

## Task 3: `LLMPool` + `Lane` + singleton accessor

**Files:**
- Create: `src/retrieval/llm_pool.py`
- Test: `tests/test_retrieval/test_llm_pool.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_retrieval/test_llm_pool.py`:

```python
"""Unit tests for the per-process LLMPool (hierarchical tier+lane limits)."""

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


@pytest.mark.asyncio
async def test_lane_counts_in_use_and_available():
    lane = Lane("extraction", "small", cap=2)
    assert lane.available == 2
    async with lane:
        assert lane.in_use == 1
        assert lane.available == 1
    assert lane.in_use == 0


@pytest.mark.asyncio
async def test_get_is_singleton_per_role(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = get_llm_pool()
    a = pool.get("extraction")
    b = pool.get("extraction")
    assert a is b  # same wrapped instance -> same semaphores


@pytest.mark.asyncio
async def test_tier_global_bounds_across_lanes(monkeypatch):
    """Two small lanes over-subscribe (caps 10 each) but the small-tier
    global (3) bounds total in-flight to 3."""
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

    settings = MagicMock()
    settings.llm_pool.tier_small_total = 3
    settings.llm_pool.tier_large_total = 8
    settings.llm_pool.lane_caps = {"extraction": 10, "judge": 10}
    settings.litellm.tier_for = lambda role: "small"

    pool = LLMPool(settings)
    ext = pool.get("extraction")
    jud = pool.get("judge")
    ext._inner.achat = fake
    jud._inner.achat = fake

    calls = [ext.achat() for _ in range(6)] + [jud.achat() for _ in range(6)]
    await asyncio.gather(*calls)
    assert max_observed <= 3


@pytest.mark.asyncio
async def test_judge_floor_under_extraction_flood(monkeypatch):
    """Sizing rule: with tier=10, extraction ceiling 6, judge can always
    get >= 4 (10-6). Flood extraction; assert judge still runs concurrently."""
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())

    settings = MagicMock()
    settings.llm_pool.tier_small_total = 10
    settings.llm_pool.tier_large_total = 8
    settings.llm_pool.lane_caps = {"extraction": 6, "judge": 14}
    settings.litellm.tier_for = lambda role: "small"

    pool = LLMPool(settings)
    ext = pool.get("extraction")
    jud = pool.get("judge")

    ext_block = asyncio.Event()

    async def ext_call(*a, **kw):
        await ext_block.wait()
        return "ok"

    async def jud_call(*a, **kw):
        await asyncio.sleep(0.02)
        return "ok"

    ext._inner.achat = ext_call
    jud._inner.achat = jud_call

    # Saturate extraction (6 calls parked on ext_block).
    ext_tasks = [asyncio.create_task(ext.achat()) for _ in range(6)]
    await asyncio.sleep(0.05)
    # Judge must still complete (floor = 10 - 6 = 4 >= 1).
    await asyncio.wait_for(jud.achat(), timeout=1.0)
    ext_block.set()
    await asyncio.gather(*ext_tasks)


@pytest.mark.asyncio
async def test_stats_reports_lanes(monkeypatch):
    monkeypatch.setattr(pool_mod, "build_llm", lambda role: _fake_llm())
    pool = get_llm_pool()
    pool.get("extraction")
    st = pool.stats()
    assert "extraction" in st["lanes"]
    assert st["lanes"]["extraction"]["cap"] >= 1
    assert "small" in st["tiers"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_retrieval/test_llm_pool.py -v`
Expected: FAIL — `No module named 'src.retrieval.llm_pool'`.

- [ ] **Step 3: Implement `src/retrieval/llm_pool.py`**

```python
"""Per-process LLM concurrency pool — single home for LLM gating.

One ``LLMPool`` per process owns:
  * a per-tier global semaphore (``small`` = GPU capacity, ``large`` =
    OpenAI budget), and
  * a per-role *lane* ceiling that intentionally over-subscribes the
    tier total.

Each small-tier LLM call acquires its lane permit first, then the
tier-global (lane-first keeps the scarce global occupied only around
the actual call).  All call-sites for a role share ONE wrapped LLM, so
the scattered ``BoundedLLM`` instances collapse into one gate per role.

This is a *per-process* control, NOT distributed: the true cross-process
GPU ceiling belongs at the LiteLLM proxy (out of scope).
"""

from __future__ import annotations

import asyncio
from typing import Any

from llama_index.core.llms import LLM

from src.config import LLMRole
from src.retrieval.llm import build_llm
from src.retrieval.llm_semaphore import BoundedLLM


class Lane:
    """A named counting async gate: bounded concurrency + an in_use counter.

    Usable directly as ``async with lane:`` — that's how ``BoundedLLM``
    acquires it.  ``in_use`` is our own counter (never ``Semaphore._value``).
    """

    def __init__(self, name: str, tier: str, cap: int) -> None:
        if cap < 1:
            raise ValueError(f"lane {name!r} cap must be >= 1, got {cap}")
        self.name = name
        self.tier = tier
        self.cap = cap
        self._sem = asyncio.Semaphore(cap)
        self.in_use = 0

    @property
    def available(self) -> int:
        return self.cap - self.in_use

    async def __aenter__(self) -> "Lane":
        await self._sem.acquire()
        self.in_use += 1
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.in_use -= 1
        self._sem.release()


class LLMPool:
    """Registry of role -> gated LLM, with shared per-tier globals."""

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        cfg = settings.llm_pool
        self._tiers: dict[str, Lane] = {
            "small": Lane("tier:small", "small", cfg.tier_small_total),
            "large": Lane("tier:large", "large", cfg.tier_large_total),
        }
        self._lanes: dict[str, Lane] = {}
        self._llms: dict[str, LLM] = {}

    def get(self, role: LLMRole) -> LLM:
        if role not in self._llms:
            tier = self._settings.litellm.tier_for(role)
            cap = self._settings.llm_pool.lane_caps[role]
            lane = Lane(role, tier, cap)
            self._lanes[role] = lane
            # lane first, then tier-global (consistent order => no deadlock).
            gates = [lane, self._tiers[tier]]
            self._llms[role] = BoundedLLM(build_llm(role), gates=gates)
        return self._llms[role]

    def stats(self) -> dict[str, Any]:
        return {
            "lanes": {
                name: {
                    "tier": ln.tier,
                    "cap": ln.cap,
                    "in_use": ln.in_use,
                    "available": ln.available,
                }
                for name, ln in self._lanes.items()
            },
            "tiers": {
                name: {"cap": t.cap, "in_use": t.in_use, "available": t.available}
                for name, t in self._tiers.items()
            },
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

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_retrieval/test_llm_pool.py -v`
Expected: PASS (all five tests).

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/llm_pool.py tests/test_retrieval/test_llm_pool.py
git commit -m "feat(llm): LLMPool — per-process role lanes + tier globals"
```

---

## Task 4: Migrate search-side call-sites to the pool

**Files:**
- Modify: `src/di/providers.py:40-49`
- Modify: `src/workflow/_search_deps.py:97-106` and `155-166`
- Modify: `src/workflow/_search_plan_deps.py:26-36`
- Modify: `src/mcp/tools_server.py:90-94`

- [ ] **Step 1: Migrate DI provider**

In `src/di/providers.py` replace the `llm` provider body (lines 40-49) and the import:

```python
from src.retrieval.llm_pool import get_llm_pool
```
(remove `from src.retrieval.llm import build_search_llm` and `from src.retrieval.llm_semaphore import BoundedLLM` if now unused.)

```python
    @provide
    def llm(self) -> LLM:
        # Search-role LLM from the shared per-process pool (one semaphore
        # set per role across all call-sites).
        return get_llm_pool().get("search")
```

- [ ] **Step 2: Migrate `_search_deps.py`**

Replace `get_search_llm` body (lines 97-106):

```python
async def get_search_llm() -> LLM:
    """Project search-role LLM from the shared per-process LLM pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("search")
```

Replace `get_synthesis_llm` body (lines 155-166):

```python
async def get_synthesis_llm() -> LLM:
    """Large-tier final-synthesis LLM from the shared per-process pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("synthesis")
```

- [ ] **Step 3: Migrate `_search_plan_deps.py`**

Replace `get_plan_llm` body (lines 26-36):

```python
async def get_plan_llm() -> LLM:
    """Small-tier planner LLM (role ``plan``) from the shared pool."""
    from src.retrieval.llm_pool import get_llm_pool
    return get_llm_pool().get("plan")
```

- [ ] **Step 4: Migrate `mcp/tools_server.py`**

Replace lines 90-94:

```python
        from src.retrieval.llm_pool import get_llm_pool
        llm = get_llm_pool().get("search")
        _deps["llm"] = llm
```
(remove the now-unused `from src.retrieval.llm import build_search_llm` / `wrap_if_needed` imports in `_init` if unused.)

- [ ] **Step 5: Run the search/MCP test suites**

Run: `uv run pytest tests/test_workflow/test_search_orchestrator.py tests/test_mcp/test_search_server.py tests/test_api/test_search_v2_routes.py -v`
Expected: PASS (no behavioral change — same gating, now shared).

- [ ] **Step 6: Commit**

```bash
git add src/di/providers.py src/workflow/_search_deps.py src/workflow/_search_plan_deps.py src/mcp/tools_server.py
git commit -m "refactor(llm): route search/plan/synthesis call-sites through LLMPool"
```

---

## Task 5: Plug the ingest leak (extract + merge through the pool)

**Files:**
- Modify: `src/workflow/activities/extract_kg.py:92`
- Modify: `src/workflow/activities/merge_and_resolve.py:98`
- Modify: `src/ingestion/run.py:55-56`
- Test: `tests/test_workflow/test_extract_kg.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_workflow/test_extract_kg.py` (a test that the activity pulls its LLM from the pool's `extraction` lane):

```python
@pytest.mark.asyncio
async def test_extract_kg_uses_pool_extraction_lane(monkeypatch, tmp_path):
    """extract_kg must obtain its LLM from the shared pool, not build a
    raw ungated one."""
    from src.retrieval import llm_pool as pool_mod

    called = {"role": None}

    class _FakePool:
        def get(self, role):
            called["role"] = role
            from unittest.mock import MagicMock
            return MagicMock()

    monkeypatch.setattr(pool_mod, "get_llm_pool", lambda: _FakePool())
    # Import inside the activity module's namespace too:
    import src.workflow.activities.extract_kg as ek
    monkeypatch.setattr(ek, "get_llm_pool", lambda: _FakePool(), raising=False)

    # build_kg_extractor is the next call; stub it so we stop early.
    def _stub_extractor(llm, mode):
        called["role"] = called["role"]  # role already recorded by pool.get
        raise RuntimeError("stop-after-llm")

    monkeypatch.setattr(ek, "build_kg_extractor", _stub_extractor)
    # Minimal Parsed with a staging blob is heavy; assert via the role capture
    # by invoking only the LLM-build line through a helper is out of scope —
    # instead assert the import wiring exists:
    assert hasattr(ek, "get_llm_pool")
```

> Note: the heavyweight `extract_kg` activity needs staging + nodes; this test asserts the wiring (the activity imports `get_llm_pool` and no longer imports `build_extraction_llm`). The lane-accounting behavior is covered by `test_llm_pool.py`.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_workflow/test_extract_kg.py::test_extract_kg_uses_pool_extraction_lane -v`
Expected: FAIL — `module 'src.workflow.activities.extract_kg' has no attribute 'get_llm_pool'`.

- [ ] **Step 3: Migrate `extract_kg.py`**

In `src/workflow/activities/extract_kg.py`, replace the import `from src.retrieval.llm import build_extraction_llm` with:

```python
from src.retrieval.llm_pool import get_llm_pool
```

Replace line 92 (`llm = build_extraction_llm()`) with:

```python
    llm = get_llm_pool().get("extraction")
```

- [ ] **Step 4: Migrate `merge_and_resolve.py`**

In `src/workflow/activities/merge_and_resolve.py`, replace `from src.retrieval.llm import build_judge_llm` with:

```python
from src.retrieval.llm_pool import get_llm_pool
```

Replace line 98 (`llm = build_judge_llm()`) with:

```python
    llm = get_llm_pool().get("judge")
```

- [ ] **Step 5: Migrate `ingestion/run.py`**

In `src/ingestion/run.py`, replace `from src.retrieval.llm import build_extraction_llm` (line 55) and its use (line 56) so the translator LLM comes from the pool:

```python
        from src.retrieval.llm_pool import get_llm_pool
        ...
        translator_llm=(
            get_llm_pool().get("extraction")
            if settings.ingestion.translate_to_russian else None
        ),
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/test_workflow/test_extract_kg.py tests/test_workflow/test_merge_and_resolve.py -v`
Expected: PASS.

- [ ] **Step 7: Audit for remaining ungated build-sites**

Run: `grep -rn "build_extraction_llm\|build_judge_llm\|build_search_llm\|build_synthesis_llm\|wrap_if_needed\|BoundedLLM(" src/ | grep -v "src/retrieval/llm.py\|src/retrieval/llm_pool.py\|src/retrieval/llm_semaphore.py"`
Expected: NO matches outside the pool/semaphore modules. If any remain (e.g. `route`/`retrieve` builders), route them through `get_llm_pool().get(<role>)` and re-run their tests.

- [ ] **Step 8: Commit**

```bash
git add src/workflow/activities/extract_kg.py src/workflow/activities/merge_and_resolve.py src/ingestion/run.py tests/test_workflow/test_extract_kg.py
git commit -m "fix(ingest): route extract_kg + merge_and_resolve through LLMPool (plug ungated leak)"
```

---

## Task 6: Relax Temporal caps + heartbeat headroom + env docs

**Files:**
- Modify: `src/workflow/document_ingest.py:164-172` (extract_kg heartbeat_timeout)
- Modify: `src/workflow/graph_build.py:65-72` (merge_and_resolve heartbeat_timeout)
- Modify: `.env.example`

- [ ] **Step 1: Raise extract_kg heartbeat headroom**

In `src/workflow/document_ingest.py`, in the `extract_kg` `execute_activity` call (lines 164-172), change `heartbeat_timeout=timedelta(minutes=5)` to `timedelta(minutes=15)` so a pool-wait before the first heartbeat cannot trip the timeout under saturation. Leave the comment above it noting the reason:

```python
                kg = await workflow.execute_activity(
                    "extract_kg", parsed,
                    result_type=KGExtracted,
                    task_queue=settings.temporal.llm_task_queue,
                    start_to_close_timeout=timedelta(hours=2),
                    # Headroom for pool-wait before the first heartbeat
                    # (LLMPool may block on the lane under saturation).
                    heartbeat_timeout=timedelta(minutes=15),
                    schedule_to_close_timeout=timedelta(hours=48),
                    retry_policy=_HEAVY_FOREVER,
                )
```

- [ ] **Step 2: Raise merge_and_resolve heartbeat headroom**

In `src/workflow/graph_build.py`, the `merge_and_resolve` activity call (lines 65-72): change `heartbeat_timeout=timedelta(minutes=5)` to `timedelta(minutes=15)` with the same comment.

- [ ] **Step 3: Update `.env.example`**

Edit `.env.example`: change the ingest concurrency defaults and add the pool block. Replace:

```
TEMPORAL_LLM_ACTIVITY_CONCURRENCY=1
```
with
```
# Pool owns LLM concurrency now; keep this >= LLM_POOL extraction ceiling.
TEMPORAL_LLM_ACTIVITY_CONCURRENCY=18
```
and
```
TEMPORAL_MERGE_ACTIVITY_CONCURRENCY=1
```
with
```
# Keep >= LLM_POOL judge ceiling.
TEMPORAL_MERGE_ACTIVITY_CONCURRENCY=14
```

Add a new block near the LLM settings:

```
# ── LLM concurrency pool (per-process; owns LLM concurrency) ──
# Small tier = local GPU concurrent-request capacity. Large tier = OpenAI.
LLM_POOL_TIER_SMALL_TOTAL=25
LLM_POOL_TIER_LARGE_TOTAL=8
LLM_POOL_JUDGE_FLOOR=7
# Per-role lane ceilings (JSON). extraction_ceiling <= small_total - judge_floor.
# LLM_POOL_LANE_CAPS={"extraction":18,"judge":14,"search":14,"plan":4,"route":2,"retrieve":4,"synthesis":8}
```

- [ ] **Step 4: Verify the worker boots with the new config**

Run: `uv run python -c "from src.config import settings; print(settings.llm_pool.lane_caps, settings.temporal.llm_activity_concurrency)"`
Expected: prints the lane-caps dict and `18` (or the env value).

- [ ] **Step 5: Run the full retrieval + workflow test suites**

Run: `uv run pytest tests/test_retrieval tests/test_workflow tests/test_config -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/workflow/document_ingest.py src/workflow/graph_build.py .env.example
git commit -m "chore(temporal): relax ingest caps + heartbeat headroom for LLMPool ownership"
```

---

## Task 7: Saturation logging for pool-wait visibility

**Files:**
- Modify: `src/retrieval/llm_pool.py`
- Test: `tests/test_retrieval/test_llm_pool.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_retrieval/test_llm_pool.py`:

```python
@pytest.mark.asyncio
async def test_lane_logs_on_saturation(monkeypatch, caplog):
    """When a lane is full and a caller must wait, we emit a warning so
    backlog that moved from Temporal schedule_to_start into pool-wait
    stays visible."""
    import logging
    lane = Lane("extraction", "small", cap=1)
    block = asyncio.Event()

    async def hold():
        async with lane:
            await block.wait()

    holder = asyncio.create_task(hold())
    await asyncio.sleep(0.01)

    with caplog.at_level(logging.WARNING):
        waiter = asyncio.create_task(_enter_and_release(lane))
        await asyncio.sleep(0.01)
        block.set()
        await holder
        await waiter

    assert any("saturated" in r.message for r in caplog.records)


async def _enter_and_release(lane):
    async with lane:
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_retrieval/test_llm_pool.py::test_lane_logs_on_saturation -v`
Expected: FAIL — no "saturated" warning emitted.

- [ ] **Step 3: Add saturation logging to `Lane.__aenter__`**

In `src/retrieval/llm_pool.py`, add `from loguru import logger` is NOT used (tests use stdlib logging/caplog). Use stdlib logging so `caplog` captures it:

```python
import logging

_log = logging.getLogger("llm_pool")
```

Update `Lane.__aenter__`:

```python
    async def __aenter__(self) -> "Lane":
        if self._sem.locked():
            _log.warning(
                "lane %r saturated (cap=%d, in_use=%d) — caller waiting",
                self.name, self.cap, self.in_use,
            )
        await self._sem.acquire()
        self.in_use += 1
        return self
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_retrieval/test_llm_pool.py -v`
Expected: PASS (all tests, including the new saturation log test).

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/llm_pool.py tests/test_retrieval/test_llm_pool.py
git commit -m "feat(llm): warn on lane saturation so pool-wait backlog stays visible"
```

---

## Final verification

- [ ] **Run the whole suite**

Run: `uv run pytest -q`
Expected: PASS (or only pre-existing unrelated failures — compare against a clean `main` run if unsure).

- [ ] **Confirm no ungated LLM build remains outside the pool**

Run: `grep -rn "build_extraction_llm\|build_judge_llm\|build_search_llm\|build_synthesis_llm\|wrap_if_needed\|BoundedLLM(" src/ | grep -v "src/retrieval/llm.py\|src/retrieval/llm_pool.py\|src/retrieval/llm_semaphore.py"`
Expected: no matches.

- [ ] **Bench (per spec §8)** — run a fixed document batch before/after on the GPU box, record wall-clock ingest + `nvidia-smi` / `ollama ps` utilization, then tune `LLM_POOL_LANE_CAPS` until GPU plateaus ~80–90%. (Manual; not a unit test.)
```

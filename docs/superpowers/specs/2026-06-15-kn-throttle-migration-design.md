# K+N throttle migration — design

**Date:** 2026-06-15
**Status:** approved (brainstorming)
**Author:** a.tzitanov + Claude

## Goal

Make **K+N** the single, only concurrency-control model and delete the
per-role hierarchical throttle entirely.

- **N** = max concurrent in-process LLM requests (`LLM_POOL_N`, one
  `asyncio.Semaphore`). Default **8**.
- **K** = max in-flight documents through ingest (`INGEST_ADMISSION_MAX_INFLIGHT`,
  the FIFO admission scheduler). Default **1**, always on.

Close the audit finding that N is a *leaky* ceiling: four search activities
call `build_llm()` directly and are counted by neither lanes nor N.

## Background / current state

- `src/retrieval/llm_pool.py` has two modes selected by `global_n`:
  hierarchical (default, `global_n==0`) with per-tier globals + per-role
  lanes, and a global/simple mode (`global_n>0`) with one `Semaphore(N)`.
- Committed `.env`/`.env.example` ship `LLM_POOL_GLOBAL_N=0` and
  `INGEST_ADMISSION_ENABLED=false` → the **hierarchical** path is what
  actually runs today.
- Leak: `route.py:84`, `contextualize.py:55`, `global_search.py:110`,
  `community.py:186` call `build_llm(role)` → a raw, ungated `OpenAILike`.
  These bypass `get_llm_pool()` entirely, so N (and the old lanes) never
  bind them.
- A second, redundant LLM throttle exists in global search:
  `global_wf.py:215` `asyncio.Semaphore(map_parallelism)`.

## Key distinction (do NOT confuse)

There are two unrelated notions of "tier" in the codebase:

| Concept | Where | Fate |
|---|---|---|
| Gating capacity (`tier_small_total`, `tier_large_total`, `lane_caps`, `judge_floor`) | `LLMPoolSettings` | **DELETED** — replaced by single N |
| Model routing (`role_tiers`, `tier_for`, `model_for`, `model_small`, `model_large`) | `LiteLLMSettings` | **KEPT** — `build_llm(role)` still resolves role→physical model |

## Changes

### 1. `src/config.py`

**`LLMPoolSettings`** — collapse to one knob:
- Remove `tier_small_total`, `tier_large_total`, `judge_floor`, `lane_caps`.
- Replace `global_n` with `n: int = Field(default=8, ge=1)` (env `LLM_POOL_N`).
  No "disabled" value — K+N is always the model.
- Rewrite the class docstring to describe the single-semaphore model.

**`IngestAdmissionSettings`** — K always-on:
- Remove the `enabled: bool` field. Keep `max_inflight: int = Field(default=1, ge=1)`.
- Rewrite docstring (no opt-in branch).

**`TemporalSettings`** — keep `llm_activity_concurrency=18` /
`merge_activity_concurrency=14` (slot isolation; invariant: never remove the
Temporal cap, the pool is the throttle). Only update the stale comments that
say "must be ≥ pool extraction/judge lane ceiling" → "must be ≥ N so the pool
binds before Temporal".

### 2. `src/retrieval/llm_pool.py`

- Delete the hierarchical branch and the `MagicMock`/`global_n` guard.
- `LLMPool.__init__` builds ONE shared gate: a single `Lane("pool", n)` (keep
  `Lane` for its `in_use` counter + saturation warning; **remove its `tier`
  field/param**).
- `get(role)` → always `BoundedLLM(build_llm(role), gates=[self._lane])`.
  All roles share the one semaphore.
- `stats()` → `{"mode": "kn", "n": <N>, "in_use": ..., "available": ...}`.
- `BoundedLLM` (`llm_semaphore.py`) and the `gates=` path are unchanged.

### 3. Close the leak — route all LLM calls through the pool

In each of the four search activities, change ONLY the body of the existing
accessor (the indirection stays for test monkeypatching):

- `route.py:77 _get_route_llm()` → `return get_llm_pool().get("route")`
- `contextualize.py:47 _get_contextualize_llm()` → `get_llm_pool().get("route")`
- `global_search.py:104 _get_map_llm()` → `get_llm_pool().get("retrieve")`
- `community.py:178 _get_summary_llm()` → `get_llm_pool().get("retrieve")`

(All other pooled call-sites already use `get_llm_pool().get(...)`.)

### 4. Remove the redundant global-search throttle (`map_parallelism`)

Delete `map_parallelism` everywhere — N is now the only LLM concurrency cap:
- `global_wf.py:215` — drop the `asyncio.Semaphore`; fan out the MAP partials
  directly (Temporal schedules them; N bounds the LLM calls).
- `workflow/contracts.py:655` — remove the `map_parallelism` field from params.
- `config.py:530,534` — remove `global_map_parallelism`.
- `mcp/search_server.py:95`, `api/routes/search_v2.py:66` — drop the arg.
- `global_wf.py:10` docstring; `docs/SEARCH.md`, `docs/CONCEPTS.md`,
  `docs/diagrams/search_modes.d2` (+ re-render svg) — update references.

### 5. `src/api/routes/ingest.py` — admission always-on

Remove the `if admission.enabled: … else <direct start>` branch (lines ~163-185).
Always: `start_workflow(IngestSchedulerWorkflow.run, SchedulerParams(max_inflight=settings.ingest_admission.max_inflight))`.
Delete the now-dead direct `DocumentIngestWorkflow` start from the route.

### 6. env + docs

`.env.example`, `.env.prod.example`, `scripts/make_env*`,
`docs/CAPACITY_TUNING.md`:
- `LLM_POOL_GLOBAL_N=0` → `LLM_POOL_N=8`.
- Delete `LLM_POOL_TIER_SMALL_TOTAL`, `LLM_POOL_TIER_LARGE_TOTAL`,
  `LLM_POOL_JUDGE_FLOOR`, `LLM_POOL_LANE_CAPS`.
- Delete `INGEST_ADMISSION_ENABLED`; keep `INGEST_ADMISSION_MAX_INFLIGHT=1`.
- Delete `AGENT_GLOBAL_MAP_PARALLELISM`.
- Rewrite the "Simple K+N mode" section of `CAPACITY_TUNING.md` as the only mode.

## Testing (TDD)

- `tests/test_retrieval/test_llm_pool.py`: replace hierarchical cases with —
  (a) N gates the (N+1)-th concurrent acquire; (b) all roles share one
  semaphore; (c) `stats()` shape; (d) `n<1` rejected by config.
- **Regression for the leak**: assert each of the four search accessors
  (`_get_route_llm`, `_get_contextualize_llm`, `_get_map_llm`,
  `_get_summary_llm`) returns a `BoundedLLM` whose gate is the pool's shared
  lane (i.e. goes through `get_llm_pool()`), not a raw LLM.
- `tests/test_config/test_settings.py`: new fields/defaults present, removed
  fields gone, `LLM_POOL_N` env parsing, `IngestAdmissionSettings` has no
  `enabled`.
- `tests/test_workflow/test_admission.py`: unchanged (AdmissionState logic is
  unaffected). Add/adjust a test that the ingest route always starts the
  scheduler.
- `tests/test_scripts/test_make_env.py`: new env var set.
- `test_llm_semaphore.py`: unchanged (BoundedLLM kept).

## Out of scope (YAGNI)

- LiteLLM-proxy `max_parallel_requests` (the true cross-process GPU ceiling)
  — separate effort; per-process pool does not protect across replicas.
- Retuning Temporal queue caps.
- Any non-pool audit findings (#1, #2, #5, #6, #8 … tracked separately).

## Risks / notes

- Per-process only: with multiple worker processes each gets its own
  `Semaphore(N)`; the real GPU ceiling still belongs at the proxy (out of scope).
- Removing per-role headroom (old `judge_floor`/synthesis reservation) means a
  large ingest can contend with interactive search under one N — accepted
  trade by design; N is tuned via benchmark afterwards (start conservative 8).

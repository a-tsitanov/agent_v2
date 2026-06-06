# LLM Pool Consolidation — Design

**Date:** 2026-06-06
**Status:** Draft (pending review)
**Goal:** Consolidate all in-process LLM concurrency control into one place — a per-process, role-keyed pool with hierarchical (tier + lane) limits — and plug the ingest path that currently bypasses gating.

---

## 1. Motivation

Today there is **no single LLM concurrency control**. The `BoundedLLM`
asyncio-semaphore wrapper is constructed independently in several places, each
with its **own** semaphore:

- `src/di/providers.py:46` — DI singleton (search role)
- `src/workflow/_search_deps.py` — search deps (`_state` cache)
- `src/workflow/_search_plan_deps.py` — plan role (own `_state`)
- `src/mcp/tools_server.py:90` — MCP search server

And the **ingest path bypasses gating entirely**: `extract_kg`
(`build_extraction_llm()`) and `merge_and_resolve` (`build_judge_llm()`) build a
raw `OpenAILike` with no semaphore. The only thing throttling ingest is the
Temporal per-queue `max_concurrent_activities` caps (`kb-ingest-llm`=1,
`kb-ingest-merge`=1), sized for a weak GPU.

The comments call `BoundedLLM` "process-wide", but it is **per-instance**, and
there are several instances per process plus separate processes (worker vs
MCP/API). Result: fragmented, partially-absent control.

### Scope decision

This is a **code-consolidation** effort, scoped to **one process**. It does
**not** attempt distributed/cross-process limiting. The true hard ceiling on the
shared GPU (all processes + replicas) belongs at the **LiteLLM proxy**
(`max_parallel_requests`) and is **out of scope** here — noted as the correct
home for global GPU protection, to be done separately if desired.

---

## 2. Key decisions (locked during brainstorming)

| # | Decision |
|---|---|
| D1 | **Goal = code consolidation**, per-process (not distributed). |
| D2 | **Pool owns LLM concurrency.** Temporal queue caps are relaxed (raised) and kept only for queue isolation / future multi-machine deploy. |
| D3 | **Pool = registry of named lanes keyed by role**, grouped by backend tier. |
| D4 | **Gating via LLM wrapper** (`BoundedLLM`-style), one permit per LLM call. `num_workers` becomes a secondary, internal fan-out knob. |
| D5 | **Hierarchical sizing**: a per-tier global semaphore + per-lane ceilings that intentionally over-subscribe. Small-tier calls acquire **two** permits (tier-global + lane). |
| D6 | **Approach A**: pool caches one `BoundedLLM` per role (singleton per process); minimal change to the wrapper. |

---

## 3. Architecture

New module **`src/retrieval/llm_pool.py`**:

```
LLMPool  (one per process, lazily built)
  ├─ _tier_sems:  dict[tier, Semaphore]          # "small" -> Semaphore(tier_small_total), ...
  ├─ _lane_sems:  dict[role, Semaphore]          # per-role ceiling (may over-subscribe)
  ├─ _lanes:      dict[role, LLM]                # cached wrapped LLM per role
  ├─ _counts:     dict[key, int]                 # own in_use counters (NOT Semaphore._value)
  ├─ get(role: LLMRole) -> LLM                    # lazy build via build_llm(role), wrap, cache
  └─ stats() -> dict                              # {lane: {tier,cap,in_use,available}, tier: {...}}

get_llm_pool() -> LLMPool                          # module-level process singleton
```

### Component boundaries

- **`LLMPool`** — *what*: owns tier-global + per-lane semaphores; builds/caches
  one wrapped LLM per role. *Depends on*: `src/retrieval/llm.build_llm`, the
  gating wrapper, `LLMPoolSettings`. *Used as*: `get_llm_pool().get("extraction")`.
- **Gating wrapper** (`BoundedLLM`, in `llm_semaphore.py`) — gates every async
  `a*` method. **Change from today**: instead of constructing its own single
  semaphore, it acquires an **ordered list of semaphores** supplied by the pool
  (`[lane_sem, tier_sem]` for small roles; `[lane_sem]` for large). Everything
  else (method set, streaming hold, `__getattr__` passthrough) is unchanged.
- **`get_llm_pool()`** — module singleton. `asyncio.Semaphore` (Py 3.10+) does
  not bind to a loop at construction; each process has exactly one running loop
  (worker `asyncio.run`; API/MCP server loop), so a lazy singleton is correct.

**Invariant:** all call-sites for a given role share the **same** wrapped LLM
and therefore the **same** lane + tier semaphores. This is the consolidation.

### Acquire order

Per LLM call, acquire **lane first, then tier-global**; release in reverse
(`tier`, then `lane`). Lane-first keeps the scarce tier-global permit occupied
only briefly (around the actual call), not while waiting for a role slot.
Counting semaphores acquired in a consistent order cannot deadlock.

### Not in scope (YAGNI — listed as possible future extensions)

Distributed limiter; priority / weighted fair-queue; acquire-timeout; a separate
`prometheus_client` endpoint for pool gauges.

---

## 4. Lanes, tiers, sizing

Lane = role. Tier governs the shared budget.

| Lane | Tier / backend | Consumer |
|---|---|---|
| `extraction` | small / Ollama-GPU | ingest: `extract_kg` |
| `judge` | small / Ollama-GPU | ingest: merge + ER |
| `search` | small / Ollama-GPU | interactive search |
| `plan`, `route`, `retrieve` | small / Ollama-GPU | search orchestration |
| `synthesis` | large / OpenAI | final answer synthesis |

### Hierarchical model

- One **small-tier global** semaphore = real GPU capacity (e.g. 25).
- One **large-tier global** — optional; with only `synthesis` on large it can be
  the lane itself (no separate global needed).
- **Per-lane ceilings over-subscribe** (sum of small ceilings > tier total) so a
  single workload can fill the GPU when others are idle (no idle "просадки"),
  while no single role can monopolize beyond its ceiling.

Example small-tier sizing (tier_small_total = 25):

| Lane | Ceiling |
|---|---|
| extraction | 18 |
| judge | 14 |
| search | 14 |
| plan | 4 |
| route | 2 |
| retrieve | 4 |

(Ceilings sum to 56 ≫ 25 — intentional over-subscription. The tier-global 25 is
the real cap.)

### Isolation guarantee (anti-regression for f49a83c)

Hierarchy gives **soft** isolation (anti-monopoly), not automatic hard floors.
To **guarantee** the merge/`judge` lane a floor under an `extraction` flood, size:

```
extraction_ceiling ≤ tier_small_total − judge_floor
```

e.g. judge_floor = 7 ⇒ extraction_ceiling ≤ 18. This is a **hard sizing rule**
in the spec and is enforced by a test (§7.3).

### Config

New `LLMPoolSettings` (env prefix `LLM_POOL_`): `tier_small_total`,
`tier_large_total`, and per-lane ceilings. Single source of truth, replacing the
scattered `AGENT_LLM_MAX_CONCURRENT`.

---

## 5. Call-site migration

Replace every LLM build site with `get_llm_pool().get(<role>)`:

| File | Now | Becomes | Effect |
|---|---|---|---|
| `di/providers.py:46` | `BoundedLLM(build_search_llm(),…)` | `pool.get("search")` | dedup instance |
| `workflow/_search_deps.py` | `wrap_if_needed(build_search_llm())` | `pool.get("search")` | shared semaphore |
| `workflow/_search_plan_deps.py` | `wrap_if_needed(build_llm("plan"))` | `pool.get("plan")` | shared semaphore |
| `mcp/tools_server.py:90` | `wrap_if_needed(build_search_llm())` | `pool.get("search")` | shared semaphore |
| `activities/extract_kg.py:92` | `build_extraction_llm()` | `pool.get("extraction")` | **leak plugged** |
| `activities/merge_and_resolve.py:98` | `build_judge_llm()` | `pool.get("judge")` | **leak plugged** |
| synthesis activity | `build_synthesis_llm()` | `pool.get("synthesis")` | under pool |
| `ingestion/run.py:56` | `build_extraction_llm()` | `pool.get("extraction")` | non-Temporal ingest |

- `entity_resolution.py` receives `llm` as a parameter from `merge_and_resolve`
  → already rides the `judge` lane; no change.
- **Audit remaining `build_llm(...)` / `build_*_llm()` call-sites** during
  implementation (grep) and route `route`/`retrieve` (and any stragglers) to
  their lanes — so no path leaks ungated like ingest does today.
- `build_extraction_llm`/`build_judge_llm`/… remain as thin factories **inside**
  the pool (it calls `build_llm(role)`); direct external calls are removed.
- `wrap_if_needed` / direct `BoundedLLM(...)` are removed from call-sites (live
  only inside the pool).

---

## 6. Temporal cap relaxation & interaction risks

Since the pool now owns LLM concurrency, Temporal caps are raised so the **pool
binds first**:

```
TEMPORAL_LLM_ACTIVITY_CONCURRENCY   1 → ≥ extraction ceiling   (e.g. 18)
TEMPORAL_MERGE_ACTIVITY_CONCURRENCY 1 → ≥ judge ceiling         (e.g. 14)
```

Rule: each worker pool's `max_concurrent_activities` ≥ the sum of lane ceilings
it hosts. Temporal activity slots are cheap (coroutines).

**Queues are NOT collapsed.** Lane semaphores now provide extract↔merge
isolation, but the separate `kb-ingest-llm` / `kb-ingest-merge` queues are kept
(free, preserves multi-machine / multi-GPU deploy). Only their caps rise.

### Risks (designed-for)

1. **Heartbeat while waiting on the pool.** An activity may start (occupying a
   slot) and block on the lane semaphore before its first LLM call heartbeats →
   risk of `heartbeat_timeout` (5 min) under saturation. Mitigations: (a) size
   lanes so waits are short; (b) raise `heartbeat_timeout` on ingest activities;
   (c) explicit saturation test (§7). No acquire-timeout in v1.
2. **Backlog visibility shifts.** Previously backlog showed as Temporal
   `schedule_to_start`. Now tasks start and wait *inside* the activity on the
   pool → backlog moves into pool-wait. Therefore `stats()` / saturation logging
   (§7) is **mandatory**, else backlog becomes invisible.
3. **Slots vs permits.** If `max_concurrent_activities` < lane ceiling, Temporal
   becomes the bottleneck again. The sizing rule above prevents this.

---

## 7. Error handling, observability, testing

### Error / timeout
- Both permits acquired via `async with`; released on exception /
  `CancelledError` (Temporal cancel frees both). Streaming holds the permit for
  the whole stream (unchanged).
- Acquire is **blocking, no timeout** (parity with current `BoundedLLM`); the
  ultimate ceiling is Temporal `schedule_to_close`.
- `in_use` tracked via the pool's **own** inc/dec counters, never
  `Semaphore._value`.

### Observability (mandatory — see risk #2)
- `LLMPool.stats()` → per lane `{tier, cap, in_use, available}` + tier-global
  `{cap, in_use}`.
- v1: structured log on saturation (available == 0 with waiters) + `stats()`
  for a diag/admin endpoint. Full Prometheus gauge = future extension.

### Tests (extend `tests/test_retrieval/test_llm_semaphore.py`)
1. **Lane cap:** N+5 concurrent calls on a lane cap=N → at most N in flight
   (mock LLM with a barrier/event).
2. **Hierarchy:** with over-subscribed lanes, total in-flight across small lanes
   never exceeds `tier_small_total`.
3. **f49a83c anti-regression:** under an `extraction` flood, `judge` retains its
   guaranteed floor; asserts the sizing rule `extraction_ceiling ≤
   tier_small_total − judge_floor`.
4. **Consolidation:** two distinct call-sites of one role get the **same**
   instance / semaphore (singleton invariant).
5. **Leak plugged:** `extract_kg` / `merge_and_resolve` actually pass through the
   pool (observed via the lane counter).

---

## 8. Rollout / benchmarking

Per project norm (benchmark before adopting; opt-in, never blind replacement):

1. Land pool + migration with conservative ceilings.
2. Bench a fixed document batch **before/after**: wall-clock ingest +
   `nvidia-smi` / `ollama ps` GPU utilization.
3. Raise ceilings until GPU plateaus ~80–90% or throughput flattens; keep the
   sizing rule (§4) intact.

---

## 9. Possible future extensions (out of scope)

- LiteLLM-proxy `max_parallel_requests` for the true cross-process GPU ceiling.
- Priority / weighted fair-queue between ingest and search.
- Acquire-timeout + retry.
- Dedicated `prometheus_client` endpoint exporting per-lane gauges.

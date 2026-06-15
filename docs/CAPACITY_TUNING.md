# Capacity Tuning Under Load

How to size the ingest/search pipeline so it stays healthy under document
peaks — and how to diagnose it when it doesn't.

> Context: prod workers used to "globally hang" under document-volume
> peaks. Root cause was a **congestion collapse**, not lock contention.
> The structural fix (heartbeating long LLM activities + removing the
> wall-clock retry cap) shipped in commit `16e7da5`. This guide covers
> the **load-side** tuning that the structural fix deliberately left to
> operators.

---

## 1. The one mental model that matters

There is exactly **one binding constraint**: how many concurrent LLM
requests your **shared LiteLLM proxy** (and the GPU / OpenAI behind it)
can sustain before latency knees over.

Everything else is plumbing around that number:

```
   Temporal activity caps   ──┐
   (isolation only)           │   must be ≥ pool ceilings
                              ▼
   LLMPool tier/lane caps  ───►  THE throttle to the proxy
   (per process)              │   size this to backend capacity
                              ▼
   LiteLLM proxy  ◄───────────┘   the real ceiling
   (GPU small tier / OpenAI large tier)
```

Two facts make the **LLMPool tier totals the single source of truth**:

1. All worker pools run in **one process** (`src/workflow/worker.py`,
   `asyncio.gather`), so the per-process `LLMPool` semaphores cap
   *global* concurrency to the proxy.
2. All tiers share **one proxy** (`LITELLM_BASE_URL`).

**Golden rule:** size `LLM_POOL_TIER_SMALL_TOTAL` to what the proxy
sustains. Keep Temporal caps generously **above** the pool ceilings so
the pool binds first. **Never** make Temporal the limit, and **never**
remove it — it's the isolation/backpressure backstop.

---

## 2. The knobs

### Pool — the real throttle (`LLM_POOL_*`, `src/config.py:570`)

| Env var | Default | Meaning |
|---|---|---|
| `LLM_POOL_GLOBAL_N` | `0` | **Simple mode.** When > 0: ONE global semaphore of size N across all roles/tiers; everything below is **ignored**. `0` = hierarchical mode (rows below). |
| `LLM_POOL_TIER_SMALL_TOTAL` | `25` | **Primary knob (hierarchical).** Max concurrent small-tier (local GPU) calls across the whole process. |
| `LLM_POOL_TIER_LARGE_TOTAL` | `8` | Max concurrent large-tier (OpenAI synthesis) calls. |
| `LLM_POOL_JUDGE_FLOOR` | `7` | Slots reserved for the merge/judge lane so an extraction flood can't starve merge. |
| `LLM_POOL_LANE_CAPS` | see below | Per-role ceilings (JSON). Lanes intentionally over-subscribe the tier total. |

Default lane caps:
`{"extraction":18,"judge":14,"search":14,"plan":4,"route":2,"retrieve":4,"synthesis":8}`

#### Simple **K + N** mode (`LLM_POOL_GLOBAL_N > 0`)

Two knobs instead of seven lanes:

- **N** = `LLM_POOL_GLOBAL_N` — hard ceiling on concurrent LLM calls
  (size N to the proxy/GPU knee, exactly like `tier_small_total` in §4).
- **K** = `INGEST_ADMISSION_MAX_INFLIGHT` (with `INGEST_ADMISSION_ENABLED=true`)
  — how many documents run end-to-end at once.

N is the **GPU safety ceiling** (one document's internal fan-out —
`extract_kg num_workers` + the ER-judge `gather` — is bounded by N even at
K=1). K is the **operator throughput/priority knob**. Trade-off vs
hierarchical: you lose the per-role headroom guarantee (`judge_floor`,
search/synthesis reservations), so a big ingest can contend with
interactive search latency. Benchmark before adopting — it's opt-in,
default off.

### Temporal — isolation, not throttle (`TEMPORAL_*`, `src/config.py:207`)

| Env var | Default | Keep it … |
|---|---|---|
| `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` | `18` | ≥ extraction lane ceiling |
| `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` | `14` | ≥ judge lane ceiling |
| `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` | `4` | ≥ parallel search sessions you allow |
| `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` | `2` | low — synthesis dominates budget |
| `TEMPORAL_ACTIVITY_CONCURRENCY` | `4` | main queue (IO/embedding) |

### Proxy client (`LITELLM_*`, `src/config.py:137`)

| Env var | Default | Note |
|---|---|---|
| `LITELLM_TIMEOUT_S` | `900` | Per-attempt request timeout (15 min). |
| `LITELLM_MAX_RETRIES` | `2` | **Recommend `0`** — see §5. Inner retries stack on top of Temporal's infinite retry and amplify load under overload. |

---

## 3. Invariants you must not break

1. **`extraction_ceiling ≤ tier_small_total − judge_floor`**
   (default `18 ≤ 25 − 7`). Guarantees merge never starves under an
   extraction flood. Enforced as an anti-regression (`f49a83c`). If you
   lower `tier_small_total`, lower the extraction lane cap to match.
2. **Each Temporal cap ≥ its pool lane ceiling.** If a Temporal cap
   drops below the pool ceiling, Temporal becomes a hidden second
   throttle and you're back to double-bookkeeping.
3. **Heartbeat interval ≪ heartbeat_timeout.** Long LLM activities pulse
   every `60s` (`_HEARTBEAT_INTERVAL_S` in `extract_kg.py` /
   `merge_and_resolve.py`) under a `15m` `heartbeat_timeout`. If you ever
   lower the timeout, keep it many multiples of 60s.

---

## 4. Right-sizing `tier_small_total` (the procedure)

The default `25` was a guess, almost certainly **above** what the proxy
sustains — that over-subscription is what tipped peaks into collapse.

1. **Find the knee.** Load-test the proxy at rising concurrency
   (1, 2, 4, 8, 16, …). Watch p95 latency and throughput. The knee is
   where p95 starts climbing super-linearly / throughput plateaus.
   Set `tier_small_total` at or just below that knee — **not** above.
2. **Lacking a load test?** Ramp in prod conservatively: start low
   (e.g. `12`), watch the saturation warning + heartbeat-timeout rate +
   proxy latency (§6), raise until latency starts degrading, then back
   off one step.
3. **Re-check invariant 1** and adjust the extraction lane + Temporal
   caps together.
4. Large tier: `tier_large_total` is OpenAI-budget bound, not GPU —
   size to your rate-limit / cost ceiling.

---

## 5. Recommended load-side changes (deferred from the fix)

These were intentionally **not** shipped in `16e7da5` because they need
operator judgement / your backend numbers:

1. **Right-size `LLM_POOL_TIER_SMALL_TOTAL`** to the proxy knee (§4).
   Biggest lever against saturation.
2. **`LITELLM_MAX_RETRIES=0`.** Temporal now owns retries (infinite,
   with backoff). Inner litellm retries just multiply offered load
   exactly when the backend is already overloaded.
3. **Stop tuning Temporal caps as a second knob.** Set them once,
   generously above pool ceilings, and leave them. The pool total is the
   only number you tune for capacity.

Optional follow-up (separate change): classify **non-retryable** errors
(corrupt blob, schema/validation failures) so a genuinely poison task
fails fast and surfaces, instead of retrying forever now that the 48h
wall is gone.

---

## 6. Diagnosing under load — symptom → knob

| Symptom | Reading | Action |
|---|---|---|
| `LLMPool lane '<role>' saturated … caller waiting` warning, but proxy p95 healthy | demand > current ceiling, backend has headroom | raise `tier_small_total` (+ Temporal caps) |
| Proxy p95 latency explodes at peaks | `tier_small_total` above backend capacity | **lower** `tier_small_total` |
| `heartbeat_timeout` failures / attempt count climbing | should be fixed by the heartbeater; if it recurs, the proxy is so slow a single chunk-call exceeds 15m, or the interval/timeout was changed | lower `tier_small_total`; verify heartbeat interval ≪ timeout |
| Tasks pile up but never fail, complete slowly | working as designed post-fix (wait, don't die) | raise capacity only if proxy has headroom |

### What to watch

- **Saturation log:** `LLMPool lane … saturated (cap=…, in_use=…)`
  (`src/retrieval/llm_pool.py`). First sign the pool is the bottleneck.
- **Temporal slot metrics** (Prometheus exporter, `METRICS_*`):
  `temporal_worker_task_slots_available` per queue. Activity slots at 0 =
  saturated; **workflow** slots at 0 = a worse, separate problem.
- **Proxy latency / error rate** at the LiteLLM layer — the ground truth
  for "is the backend healthy".

---

## 7. Anti-patterns (don't)

- ❌ Remove the Temporal cap "to let more through." More offered load
  into a saturated proxy = deeper collapse. This was the original
  wrong instinct.
- ❌ Raise `tier_small_total` above proxy capacity to "go faster." You
  trade throughput for a latency cliff and retry storms.
- ❌ Set a Temporal cap below the matching pool ceiling. Re-creates the
  hidden second throttle and the double-bookkeeping this design removed.
- ❌ Lower `heartbeat_timeout` near the 60s pulse interval. False
  cancellations come back.

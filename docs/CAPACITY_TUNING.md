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
   Temporal activity caps  ──┐
   (isolation only)           │   must be ≥ LLM_POOL_N
                              ▼
   LLMPool (LLM_POOL_N)  ────►  THE throttle to the proxy
   (per process, global       │   size N to backend capacity
    semaphore)                 ▼
   LiteLLM proxy  ◄───────────┘   the real ceiling
   (GPU / OpenAI)
```

All worker pools run in **one process** (`src/workflow/worker.py`,
`asyncio.gather`), so the per-process `LLMPool` semaphore caps
*global* concurrency to the proxy.

**Golden rule:** size `LLM_POOL_N` to what the proxy sustains. Keep
Temporal caps generously **above** N so the pool binds first. **Never**
make Temporal the limit, and **never** remove it — it's the
isolation/backpressure backstop.

---

## 2. The K + N model

The pipeline uses two knobs:

- **N** = `LLM_POOL_N` (default `8`) — hard ceiling on concurrent LLM
  calls across the entire process (single global semaphore). Size this to
  your proxy/GPU knee.
- **K** = `INGEST_ADMISSION_MAX_INFLIGHT` (default `1`) — how many
  documents run end-to-end concurrently. K=1 means strict
  finish-before-next; raise K to overlap one document's I/O stages with
  another's GPU stage.

**Admission is always on.** `/ingest` hands every document to the
`IngestSchedulerWorkflow` singleton, which enforces the K cap.

### 2a. The knobs

| Env var | Default | Meaning |
|---|---|---|
| `LLM_POOL_N` | `8` | Max concurrent LLM requests (global semaphore). **Primary tuning knob.** |
| `INGEST_ADMISSION_MAX_INFLIGHT` | `1` | Max in-flight documents end-to-end (K). |

### 2b. Temporal — isolation, not throttle (`TEMPORAL_*`, `src/config.py:207`)

| Env var | Default | Keep it … |
|---|---|---|
| `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` | `18` | ≥ `LLM_POOL_N` |
| `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` | `14` | ≥ `LLM_POOL_N` |
| `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` | `4` | ≥ parallel search sessions you allow |
| `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` | `2` | low — synthesis dominates budget |
| `TEMPORAL_ACTIVITY_CONCURRENCY` | `4` | main queue (IO/embedding) |

### 2c. Proxy client (`LITELLM_*`, `src/config.py:137`)

| Env var | Default | Note |
|---|---|---|
| `LITELLM_TIMEOUT_S` | `900` | Per-attempt request timeout (15 min). |
| `LITELLM_MAX_RETRIES` | `2` | **Recommend `0`** — see §4. Inner retries stack on top of Temporal's infinite retry and amplify load under overload. |

---

## 3. Invariants you must not break

1. **Each Temporal cap ≥ `LLM_POOL_N`.** If a Temporal cap drops below
   N, Temporal becomes a hidden second throttle and you lose single-knob
   control.
2. **Heartbeat interval ≪ heartbeat_timeout.** Long LLM activities pulse
   every `60s` (`_HEARTBEAT_INTERVAL_S` in `extract_kg.py` /
   `merge_and_resolve.py`) under a `15m` `heartbeat_timeout`. If you ever
   lower the timeout, keep it many multiples of 60s.

---

## 4. Right-sizing `LLM_POOL_N` (the procedure)

The default `8` is a conservative starting point.

1. **Find the knee.** Load-test the proxy at rising concurrency
   (1, 2, 4, 8, 16, …). Watch p95 latency and throughput. The knee is
   where p95 starts climbing super-linearly / throughput plateaus.
   Set `LLM_POOL_N` at or just below that knee — **not** above.
2. **Lacking a load test?** Ramp in prod conservatively: start low
   (e.g. `4`), watch the saturation warning + heartbeat-timeout rate +
   proxy latency (§5), raise until latency starts degrading, then back
   off one step.
3. **Re-check invariant 1** and adjust Temporal caps together (keep them
   ≥ N).

---

## 5. Recommended load-side changes (deferred from the fix)

These were intentionally **not** shipped in `16e7da5` because they need
operator judgement / your backend numbers:

1. **Right-size `LLM_POOL_N`** to the proxy knee (§4). Biggest lever
   against saturation.
2. **`LITELLM_MAX_RETRIES=0`.** Temporal now owns retries (infinite,
   with backoff). Inner litellm retries just multiply offered load
   exactly when the backend is already overloaded.
3. **Stop tuning Temporal caps as a second knob.** Set them once,
   generously above N, and leave them. N is the only number you tune
   for capacity.

Optional follow-up (separate change): classify **non-retryable** errors
(corrupt blob, schema/validation failures) so a genuinely poison task
fails fast and surfaces, instead of retrying forever now that the 48h
wall is gone.

---

## 6. Diagnosing under load — symptom → knob

| Symptom | Reading | Action |
|---|---|---|
| `LLMPool saturated … caller waiting` warning, but proxy p95 healthy | demand > N, backend has headroom | raise `LLM_POOL_N` (+ Temporal caps) |
| Proxy p95 latency explodes at peaks | N above backend capacity | **lower** `LLM_POOL_N` |
| `heartbeat_timeout` failures / attempt count climbing | should be fixed by the heartbeater; if it recurs, the proxy is so slow a single chunk-call exceeds 15m, or the interval/timeout was changed | lower `LLM_POOL_N`; verify heartbeat interval ≪ timeout |
| Tasks pile up but never fail, complete slowly | working as designed post-fix (wait, don't die) | raise N only if proxy has headroom |

### What to watch

- **Saturation log:** `LLMPool saturated (n=…, in_use=…)`
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
- ❌ Raise `LLM_POOL_N` above proxy capacity to "go faster." You
  trade throughput for a latency cliff and retry storms.
- ❌ Set a Temporal cap below `LLM_POOL_N`. Re-creates the hidden
  second throttle and the double-bookkeeping this design removed.
- ❌ Lower `heartbeat_timeout` near the 60s pulse interval. False
  cancellations come back.

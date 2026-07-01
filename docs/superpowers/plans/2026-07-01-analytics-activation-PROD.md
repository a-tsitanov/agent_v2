# Analytics Layer — PROD Activation Checklist (Waves 0–3)

Copy-pasteable, ops-ready. Companion to the runbook
`2026-07-01-analytics-activation-rollout.md` (which explains the *why* / order /
rollback). This is the *what to type*. Activate **one phase at a time**, verify,
then proceed. Everything is reversible: flip the flag back + recreate the worker.

## 0. Pre-requisites

- Deploy `origin/worktree-anal` (or the merged commit) to the image the worker
  and API run from. Confirm `CATALOG=42` in the **worker** process:
  `docker compose exec <worker> python -c "import src.workflow.analytics.activities; from src.analytics.catalog import CATALOG; print(len(CATALOG))"` → `42`.
  (The API/MCP process shows `0` **by design** — it dispatches to the worker.)
- Neo4j, Temporal, Milvus, Postgres healthy.
- **CRITICAL (community_backend lesson):** every flag below MUST be added to the
  compose **`x-app-env` anchor**, not just `.env`, or the worker container runs
  the dark default. After each phase: `docker compose exec <worker> env | grep <FLAG>` to prove it reached the container.

## Env block (add to the `x-app-env` anchor; recommended prod values)

```yaml
# ── E1 first_seen (Phase 1) ──
EVENTS_FIRST_SEEN_ENABLED: "false"          # → true only AFTER backfill
EVENTS_FIRST_SEEN_BACKFILL_EPOCH: "0"       # sentinel stamp for pre-existing nodes (set to today's epoch-day at backfill)
# ── Arc-2 monitor (Phase 3) ──
MONITOR_ENABLED: "false"                    # → true in Phase 3
MONITOR_TASK_QUEUE: "kb-monitor"
MONITOR_ACTIVITY_CONCURRENCY: "2"
MONITOR_SWEEP_INTERVAL_MINUTES: "30"
MONITOR_NEW_WINDOW_DAYS: "7"
MONITOR_RISK_RISE_DELTA: "0.1"
# ── E3 burst (Phase 6) ──
MONITOR_BURST_ENABLED: "false"              # → true in Phase 6 (after event history builds)
MONITOR_BURST_WINDOW_DAYS: "7"
MONITOR_BURST_BASELINE_WINDOWS: "4"
MONITOR_BURST_MIN_COUNT: "2"
MONITOR_BURST_RATIO: "3.0"
# ── Push (Phase 4) ──
MONITOR_WEBHOOK_URL: ""                     # → set in Phase 4 (Slack/Teams/custom incoming webhook)
MONITOR_WEBHOOK_TIMEOUT_S: "5"
MONITOR_DELIVER_BATCH: "100"
# ── E2 events (Phase 5) ──
EVENTS_EXTRACTION_ENABLED: "false"          # → true only after the events eval passes
```

Also ensure the worker's `WORKER_GROUPS` (env) **includes `monitor`** — e.g.
`WORKER_GROUPS: "main,llm,merge,search,large,graph_build,wiki,scheduler,monitor"`
(or leave `WORKER_GROUPS` unset so all groups fork). Recreate the worker after
changing it.

## Linear checklist

**Phase 1 — E1 first_seen (foundation for E3):**
1. `docker compose exec <worker> python -m scripts.backfill_first_seen --dry-run` → review counts.
2. Set `EVENTS_FIRST_SEEN_BACKFILL_EPOCH` = today's epoch-day, then run for real: `... python -m scripts.backfill_first_seen`.
3. Set `EVENTS_FIRST_SEEN_ENABLED: "true"` in the anchor → `docker compose up -d --force-recreate <worker>`.
4. Verify: ingest a doc; `MATCH (e:__Entity__) WHERE e.created_at IS NOT NULL RETURN count(e)` grows; the `new_events` primitive returns rows.

**Phase 2 — materialize (risk/centrality):**
1. GDS smoke: `curl -XPOST <api>/admin/graph/materialize` and watch worker logs for GDS proc errors.
2. Verify: entities gain `risk_score`/`risk_band`; `risk_score` / `top_central_entities` primitives return rows.

**Phase 3 — Arc-2 monitor:**
1. Set `MONITOR_ENABLED: "true"` (+ `monitor` in `WORKER_GROUPS`) → recreate worker.
2. Seed watchlist: `curl -XPOST <api>/admin/monitor/watch -H 'content-type: application/json' -d '["Entity A","Entity B"]'` (optionally `?watched=true`).
3. `docker compose exec <worker> python -m scripts.setup_monitor_schedule` (idempotent). Or kick once: `curl -XPOST <api>/admin/monitor/sweep`.
4. Verify: after a sweep, `MATCH (a:Alert) RETURN a.kind, count(*)` shows `new_connection`/`risk_rise`; the `alerts` primitive returns them. (risk_rise needs Phase 2.)

**Phase 4 — push:**
1. Set `MONITOR_WEBHOOK_URL: "<incoming-webhook>"` → recreate worker.
2. Verify: next sweep POSTs unpushed alerts; the webhook sink receives JSON `{key,kind,entity,detail,created_at,score}`; `MATCH (a:Alert) WHERE a.pushed_at IS NOT NULL RETURN count(*)` grows; re-sweep does not re-deliver.

**Phase 5 — E2 events (eval-gated; extra LLM cost):**
1. Run the events extraction-quality eval on a sample (needs an extraction LLM):
   `EVENTS_EXTRACTION_ENABLED=true uv run python -m tests.eval.events_eval` → review micro P/R/F1.
2. No-regression on entities/relations: run `ner_eval` with `EVENTS_EXTRACTION_ENABLED` off vs on; confirm entity/relation F1 does NOT drop.
3. If acceptable: `EVENTS_EXTRACTION_ENABLED: "true"` → recreate the **ingest** worker.
4. Verify: new ingests produce `:EventOrAction` nodes; `event_dossier`/`event_timeline` return rows.

**Phase 6 — E3 burst (needs ≥ `baseline_windows × window_days` of event history — default 28 days):**
1. Wait for event history to accumulate (or raise `MONITOR_BURST_RATIO` / lengthen the baseline initially so early sparse history doesn't read as bursty).
2. Set `MONITOR_BURST_ENABLED: "true"` → recreate worker.
3. Verify: `trending_events` primitive ranks surges; after a sweep, `MATCH (a:Alert {kind:"burst"}) RETURN a.entity, a.detail, a.score` appears and (Phase 4) is delivered.

## Rollback (any phase)

| Phase | Rollback |
|---|---|
| 1 | `EVENTS_FIRST_SEEN_ENABLED=false` + recreate worker (stamped props harmless) |
| 2 | stop triggering `/admin/graph/materialize` (scores stale, harmless) |
| 3 | `MONITOR_ENABLED=false` + delete the `monitor-sweep` schedule (`:Alert` nodes inert) |
| 4 | clear `MONITOR_WEBHOOK_URL` → delivery no-ops (already-pushed stay pushed) |
| 5 | `EVENTS_EXTRACTION_ENABLED=false` + recreate ingest worker (existing event nodes harmless) |
| 6 | `MONITOR_BURST_ENABLED=false` + recreate worker |

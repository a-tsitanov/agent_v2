# Analytics Layer — Activation / Rollout Runbook (Waves 0–3)

**Status:** Draft rollout plan — 2026-07-01. Everything below is built, tested, and **dark**; this un-darkens it safely, in dependency order, on the user's stack. Branch `worktree-anal` (pushed to `origin/worktree-anal`, not merged to main).

**This is an OPS runbook, not a code plan.** No code changes required to activate (except possibly a small events-eval harness in Phase 5). Each phase is independently valuable and independently reversible (flip the flag back + recreate the worker).

## Load-bearing lesson (from the community_backend incident)

Flags MUST be wired into the **compose `x-app-env` anchor**, not just `.env` — otherwise the worker container silently runs the default (dark). Verify each flag reaches the worker with `docker compose exec <worker> env | grep <FLAG>` after every phase. `WORKER_GROUPS` is read by the launcher at runtime; the `monitor` group must be present in the worker's env for the sweep to run.

## Dependency graph

```
Phase 0 (read side)      — always safe, verify CATALOG=42 loads at runtime
Phase 1 (E1 first_seen)  — foundation: created_at stamping (needed by E3 burst)
   └─ Phase 2 (materialize) — risk_score/centrality (needed by Arc-2 risk-rise)
        └─ Phase 3 (Arc-2 monitor) — new-connection + risk-rise :Alert nodes
             └─ Phase 4 (push)     — deliver :Alert via webhook
Phase 5 (E2 events)      — LLM extraction (gated on an extraction-quality eval)
   └─ Phase 6 (E3 burst) — needs E2 events + ≥1 baseline window of created_at history
```

Order rationale: risk-rise alerts (Phase 3) need materialized `risk_score` (Phase 2); E3 burst (Phase 6) needs event nodes with `created_at` (Phase 5 + Phase 1) accumulated over the baseline window; push (Phase 4) is independent once alerts exist.

---

## Phase 0 — Read side (zero risk, do first)

Nothing to flip; just confirm the catalog is live.

- Confirm the worker/API import `src.analytics.primitives` so `CATALOG` populates at runtime (Wave-0 integration requirement). Check: `POST /api/v1/analyze {"query": "..."}` returns a plan/answer, and MCP `kb_analyze` is registered.
- Confirm `CATALOG` has **42** primitives in the running process (not just tests). Materialize-dependent primitives (`risk_score`, `top_central_entities`, `link_prediction`) return empty until Phase 2 — expected.
- **Rollback:** n/a.

## Phase 1 — E1 first_seen stamping

Gives every newly-created node/edge a `created_at` (epoch-days) + `first_doc_id`. **Foundation for E3.**

1. Ensure indexes exist (idempotent): the ingest path calls `ensure_first_seen_indexes`; confirm on the running Neo4j.
2. Backfill legacy elements FIRST: `uv run python -m scripts.backfill_first_seen --dry-run` → review counts → run for real. Stamps a sentinel epoch (`settings.events.first_seen_backfill_epoch`) on pre-existing nodes so they aren't later mis-flagged as "new".
3. Only AFTER backfill: set `EVENTS_FIRST_SEEN_ENABLED=true` (`settings.events.first_seen_enabled`) in the compose anchor, recreate the ingest worker.
4. **Verify:** ingest a doc; new entities have `created_at = today_epoch_days()`; re-mentioned old entities keep their sentinel/original stamp. `new_events` / `entity_new_connections` primitives now return rows.
- **Rollback:** `first_seen_enabled=false`, recreate worker. Stamped data is harmless (extra props).

## Phase 2 — Materialize tier (risk/centrality/link-prediction)

Admin-triggered offline GDS pass; writes `risk_score`/`risk_band`/`betweenness`/etc.

1. **GDS smoke test FIRST** (proc names were unverified vs live GDS): run one materialize and watch worker logs for GDS proc errors. `POST /admin/graph/materialize`.
2. **Verify:** entities gain `risk_score`/`risk_band`; `risk_score`/`top_central_entities`/`link_prediction` primitives return rows. Re-run captures `risk_score_prev` (Wave-2 snapshot).
3. Schedule or re-trigger as needed (it's idempotent per run; consider serializing runs).
- **Rollback:** stop triggering; scores are stale but harmless.

## Phase 3 — Arc-2 monitoring (alerts as :Alert nodes)

Detects new-connection + risk-rise on **watched** entities; persists `:Alert` (no push yet).

1. Add `monitor` to the worker's `WORKER_GROUPS` (compose anchor) — the pool forks and idles until enabled.
2. Set `MONITOR_ENABLED=true` in the anchor; recreate the worker.
3. Seed the watchlist: `POST /admin/monitor/watch` with the entity names to watch (`{names, watched:true}`). (Not gated on `monitor.enabled` by design — pre-seed anytime.)
4. Create the schedule: `uv run python -m scripts.setup_monitor_schedule` (idempotent; interval = `MONITOR_SWEEP_INTERVAL_MINUTES`). Or kick once: `POST /admin/monitor/sweep`.
5. **Verify:** after a sweep, `:Alert` nodes exist for watched entities' new edges / risk rises; the `alerts` primitive returns them. Requires Phase 2 for risk-rise.
- **Rollback:** `MONITOR_ENABLED=false` + delete the schedule; `:Alert` nodes are inert.

## Phase 4 — Push delivery

Delivers unpushed `:Alert` to a webhook.

1. Set `MONITOR_WEBHOOK_URL=<incoming-webhook>` (Slack/Teams/custom) in the anchor; recreate the worker. `MONITOR_WEBHOOK_TIMEOUT_S`/`MONITOR_DELIVER_BATCH` optional.
2. **Verify:** next sweep delivers unpushed alerts (2xx → `pushed_at` set, no re-delivery); check the webhook sink received them; `SweepResult.delivered/failed` in the workflow result.
- **Rollback:** clear `MONITOR_WEBHOOK_URL` → `deliver_alerts` no-ops. Already-pushed stay pushed.
- **Note:** worst-case `deliver_batch × webhook_timeout_s` should stay under the activity's 5-min start-to-close (default 100×5s=500s > 300s only if every POST hangs — benign, retried next sweep; tune if needed).

## Phase 5 — E2 event extraction (highest cost/risk; eval-gated)

Adds `:EventOrAction` nodes via LLM per-chunk extraction. **Extra LLM cost; can affect extraction quality — the core project track.**

1. **Build/run an events extraction-quality eval FIRST.** `tests/eval/` has `test_analytics_faithfulness` + `ner_eval` (entities) but **no events-specific eval** — add a small golden set (docs → expected `(event_type, participants, event_ts)`) mirroring `ner_eval.py`, run on a sample corpus, and confirm precision/recall + that entity/relation extraction quality does NOT regress (compare `ner_eval` with events on vs off).
2. If acceptable: `EVENTS_EXTRACTION_ENABLED=true` in the anchor; recreate the ingest worker.
3. **Verify:** new ingests produce `:EventOrAction` nodes (deduped by the event match-key); `event_dossier`/`event_timeline` return rows; entity/relation quality unchanged vs baseline.
- **Rollback:** `EVENTS_EXTRACTION_ENABLED=false`, recreate worker. Existing event nodes remain (harmless).

## Phase 6 — E3 burst detection

Emits `:Alert(kind="burst")` for watched (entity × event_type) surges.

1. Requires Phase 5 (events) + Phase 1 (created_at on events). **Wait for history:** a full baseline needs `burst_baseline_windows × burst_window_days` (default 4×7 = 28 days) of event ingest history — before that, `baseline_rate` is near-zero and everything reads as bursty. Either wait, or start with a higher `MONITOR_BURST_RATIO` / longer baseline.
2. Set `MONITOR_BURST_ENABLED=true` in the anchor; recreate the worker.
3. **Verify:** `trending_events` primitive returns ranked surges; after a sweep, `:Alert(kind="burst")` nodes appear for watched entities and (Phase 4) get delivered.
- **Rollback:** `MONITOR_BURST_ENABLED=false`, recreate worker.

---

## Decision points (need your input)

1. **Deploy target** — which compose/stack (dev vs prod), and confirm Neo4j/Milvus/Temporal are up.
2. **Events eval** (Phase 5) — do you have/want a golden events set, or should I build the `ner_eval`-style harness first? This is the one code deliverable in the rollout.
3. **Webhook destination** (Phase 4) — Slack/Teams/custom URL.
4. **Merge vs branch** — activate from `origin/worktree-anal`, or open a PR → merge to main first?

## What I can do now without your infra (safe)

- **Phase 0 verification in code:** confirm the worker/API runtime imports populate `CATALOG=42` and the analyze/MCP surfaces are wired (static checks + import sanity).
- **Phase 5 prep:** build the events extraction-quality eval harness (`tests/eval/events_eval.py` mirroring `ner_eval.py`) so the gate exists before you flip `EVENTS_EXTRACTION_ENABLED`.
- Fold the Wave-2/3 deferred review follow-ups into whichever phase touches that code.

Tell me the deploy target (or "just build the events eval") and I'll start.

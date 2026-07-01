# Analytics Layer — LOCAL dev Activation (host-run app + dockerized infra)

For the local setup: infra runs in docker compose (neo4j/temporal/milvus/
postgres/litellm — confirmed **up**), the **app (API + worker) runs on the
host** via `uv run`. Flags go in a host `.env` (+ exported `TEMPORAL_*`).

## Local caveats (from prior local-dev experience)

- `TEMPORAL_*` must be **exported in the shell**, not only in `.env`.
- `API_ENV=development` for the preflight/model validation to pass.
- **The local LLM proxy (litellm) is a placeholder** → E2 event *extraction*
  and its eval (Phase 5) and therefore E3 burst (Phase 6) can't be meaningfully
  validated locally without pointing litellm at a real model. Phases 0–4
  (first_seen, materialize, monitor, push) work locally.
- Run commands from the repo root with the worktree checked out.

## 0. Prereqs

1. Create `.env` from `.env.example` and fill local creds (Neo4j password,
   Milvus, Postgres, etc.). Add the analytics flags from the PROD checklist
   (`2026-07-01-analytics-activation-PROD.md`), all `false`/empty to start.
2. Export Temporal: `export TEMPORAL_ADDRESS=localhost:7233 API_ENV=development`.
3. Sanity: `uv run python -c "from src.graph.store import build_neo4j_graph_store as b; print(b().structured_query('RETURN 1 AS ok'))"` → `[{'ok': 1}]`.

## Phase 1 — first_seen (host)

1. Dry run: `uv run python -m scripts.backfill_first_seen --dry-run` → review counts.
2. Run for real (keep sentinel `0`): `uv run python -m scripts.backfill_first_seen`. Verify undated count → 0. **(DONE on this local Neo4j 2026-07-01: 82 entities + 928 rels stamped, 0 undated left.)**
3. Set `EVENTS_FIRST_SEEN_ENABLED=true` in `.env`.
4. Start the worker (all groups incl. monitor) on the host:
   `WORKER_GROUPS=main,llm,merge,search,large,graph_build,wiki,scheduler,monitor uv run python -m src.workflow.worker`
   and the API: `uv run uvicorn src.api.main:app --port 8000` (separate shell).
5. Ingest a doc; verify `MATCH (e:__Entity__) WHERE e.created_at IS NOT NULL RETURN count(e)` grows.

## Phase 2 — materialize (host)

- `curl -XPOST localhost:8000/admin/graph/materialize` → watch worker logs for GDS proc errors → verify `risk_score` on entities.

## Phase 3 — monitor (host)

1. `.env`: `MONITOR_ENABLED=true`; restart the worker (monitor group already in `WORKER_GROUPS`).
2. Seed: `curl -XPOST localhost:8000/admin/monitor/watch -H 'content-type: application/json' -d '["Entity A"]'`.
3. `uv run python -m scripts.setup_monitor_schedule` (or `curl -XPOST localhost:8000/admin/monitor/sweep`).
4. Verify: `MATCH (a:Alert) RETURN a.kind, count(*)`.

## Phase 4 — push (host)

- `.env`: `MONITOR_WEBHOOK_URL=<e.g. https://webhook.site/...>`; restart worker; next sweep delivers; check the sink got `{key,kind,entity,detail,created_at,score}`.

## Phase 5–6 — events + burst (LOCAL-LIMITED)

- Requires litellm pointed at a REAL extraction model (placeholder won't extract events). With that:
  - Run `EVENTS_EXTRACTION_ENABLED=true uv run python -m tests.eval.events_eval` (+ `ner_eval` off/on).
  - If acceptable: `EVENTS_EXTRACTION_ENABLED=true`, re-ingest; then after event history, `MONITOR_BURST_ENABLED=true`.
- Otherwise these two phases are validated in prod (or a stack with a real LLM), per the PROD checklist.

## Local validation run (2026-07-01)

Executed against the live local stack (dockerized infra + a worktree worker on
`graph_build,monitor`):
- **Phase 1 backfill** — DONE (82 entities + 928 rels stamped `created_at=0`, 0 undated left).
- **Arc 2 monitor sweep** — VALIDATED end-to-end: watched entity `E0_0` + a
  synthetic risk rise → `MonitorSweepWorkflow` returned
  `risk_rise_alerts=1, error=''`; a real `:Alert {key:"risk_rise:E0_0:", detail:"", score:0.9}` persisted (score is a property, NOT in the key).
- **#2 no-churn** — re-sweep with the score drifted to 0.95 → still **one**
  alert node, `score` updated in place (0.95). Confirmed.
- Synthetic demo data reverted afterward (0 alerts, 0 watched); backfill kept.

### ⚠️ Stale-worker drift (blocks Phase 2 locally)

A pre-existing worker from the **main repo checkout**
(`/Users/a.tsitanov/projects/kb-llamaindex`, old code without the Wave-1
materialize activities) is **also polling `graph_build`**. Temporal
load-balances, so a `materialize` trigger routed to the stale worker →
`NotFound: materialize_risk`. Arc 2 was unaffected because its `monitor` queue
is new (the stale worker doesn't poll it). **For real activation: stop/replace
the stale main-repo worker with the new-code worker** (a single worker per
queue), or run the new worker on isolated queues. Matches the known
env/stale-worker drift.

## Rollback

Same as PROD: flip the flag back in `.env` + restart the host worker.

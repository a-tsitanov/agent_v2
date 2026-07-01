# Analytical Layer — Wave 3 Design (E3 trending/burst + alert push channels)

**Status:** Approved design (brainstorming) — 2026-06-30. Next: writing-plans → implementation.
**Branch:** worktree-anal (local, unpushed). Builds on Waves 0–2 (catalog/primitives, E1 first_seen, E2 events, Arc 2 monitoring/alerts).

## Goal

Close the two items Wave 2 explicitly scoped out:

1. **E3** — burst/trending analytics over structured events: an on-demand catalog primitive `trending_events`, plus a burst **detector** wired into the Arc-2 monitor sweep that emits `:Alert(kind="burst")` for watched entities.
2. **Push channels** — make Arc-2 alerts actionable by delivering `:Alert` records to a generic outbound **webhook**, as a second activity in the monitor sweep.

Both ship **dark** and are independently shippable (E3 first).

## Decisions (locked via brainstorming)

- **Burst time axis = `created_at`** (ingest epoch-days, "what we're suddenly *learning* a lot about") — reliable, aligns with Arc-2 novelty. NOT `event_ts` (free-text/sparse). Dependency: event nodes get `created_at` from E1 `stamp_first_seen`; if absent → detector is fail-soft empty.
- **Grain = (participant entity × event_type)** — most actionable, ties to watchlist/risk.
- **Metric** = both a relative `burst_score` (recent vs baseline rate) **and** the absolute `recent` count; sort by `burst_score`, both visible; `min_count` + `ratio` thresholds cut noise.
- **E3 = primitive + alert** — `trending_events` read primitive AND a burst finding in `detect_alerts`.
- **Push channel = generic outbound webhook** (POST JSON to a configured URL — covers Slack/Teams/Discord/custom). Not vendor-specific; pluggable multi-sink deferred.
- **Delivery = 2nd activity in the sweep** — `MonitorSweepWorkflow → detect_alerts → deliver_alerts`. Idempotent via a new `:Alert.pushed_at` (push only `pushed_at IS NULL`). One Schedule/queue.

## Global constraints (carried from Waves 0–2)

- **Ship dark.** `trending_events` is read-only/fail-soft (no gate, like every primitive). Burst detection in `detect_alerts` is gated `settings.monitor.burst_enabled` (default False). Delivery is active only when `settings.monitor.webhook_url` is non-empty (default ""). With these off/empty, the monitor sweep behavior is byte-for-byte unchanged.
- **Determinism / fail-soft.** Activities never raise across the Temporal boundary; read primitives fail-soft via `run_rows`; numbers come from Cypher rows. Sync Neo4j and HTTP run off the event loop (`asyncio.to_thread` / async httpx). The webhook POST never raises — a failure leaves `pushed_at` NULL for retry next sweep.
- **Idempotency.** `:Alert` MERGE on `alert_key` (unchanged); delivery marks `pushed_at` only on POST success; burst alert key `kind=burst` dedups re-reports of the same burst within a sweep window.
- **Conventions (reuse exactly).** Catalog primitive = `async def fn(store, *, ...) -> PrimitiveResult` + `register(Primitive(...))`; param model subclasses a base with `ConfigDict(extra="ignore")`; `clamp_top_n`; entity label literal `"__Entity__"`; NULL-safe polarity filter `(r.polarity IS NULL OR r.polarity <> 'negated')`. Frozen contracts subclass `_Frozen`. New env vars get a Russian description in `scripts/make_env.py`.
- **Quality gates** before each commit: `uv run ruff check`/`format` changed files (ruleset E,F,I,B,UP,SIM,RUF, line 100, py312; no `# noqa: BLE001`) + the task's pytest. Full gate must stay at the 13-failure pre-existing baseline (regression-free).
- **Git:** commit locally on worktree-anal; never push, never main.

## Architecture

### Part A — E3 burst engine (shared, DRY)

**New `src/analytics/events_burst.py`** — single source for the burst computation so the primitive and the detector cannot drift (same pattern as `read_alerts_cypher`):

- `build_burst_cypher(*, watched_only: bool) -> str` returns one parameterized Cypher:
  - recent: `count` of `(:EventOrAction)` with `created_at >= $since_recent` (`$since_recent = today - W`).
  - baseline: `count` over `[today - W·(B+1), today - W)`, averaged per window → `baseline_rate = baseline_count / $baseline_windows`.
  - `burst_score = recent / (CASE WHEN baseline_rate < 1 THEN 1 ELSE baseline_rate END)`.
  - group by `(p.name, e.event_type)` via `(e:__Entity__:EventOrAction)-[:PARTICIPATED_IN]->(p:__Entity__)`.
  - `WHERE` includes recent-window predicate, NULL-safe polarity guard on the participation edge, and `watched_only` → `AND p.watched = true`.
  - `WITH ... WHERE recent >= $min_count AND burst_score >= $ratio` then `RETURN entity, event_type, recent, baseline_rate, burst_score ORDER BY burst_score DESC, recent DESC LIMIT $top_n`.
- Params: `$since_recent, $since_baseline, $baseline_windows, $min_count, $ratio, $top_n`. The primitive passes `$ratio = 1` (return all trending, ranked); the detector passes the configured `burst_ratio` (alert threshold).

> Note: `event_ts` is intentionally **not** in the output — the grain is by `created_at`, and `event_ts` is a free-text per-event field not meaningful at the group level. (`trending_events` reports ingest-burst, not real-world timing.)

### Part B — E3 primitive `trending_events`

Add to **`src/analytics/primitives/events_llm.py`** (event-read family):

- `trending_events(store, *, window_days=7, baseline_windows=4, min_count=2, top_n=20) -> PrimitiveResult` — `watched_only=False`, `ratio=1`; `since_*` computed from `today_epoch_days()`; fail-soft `run_rows`; `clamp_top_n`; `register(Primitive("trending_events", ...))`.
- Also **fix `event_timeline.window_days`** (currently inert): apply `created_at >= $since` when `window_days` is set.
- CATALOG: 41 → **42**; extend `tests/test_analytics/test_catalog_complete.py`.

### Part C — E3 burst alerts in the sweep

In **`src/workflow/monitor/activities.py` `detect_alerts`**, gated by `settings.monitor.burst_enabled`:

- run `build_burst_cypher(watched_only=True)` with `min_count`/`ratio` from settings → for each row `upsert_alert(kind="burst", entity=row["entity"], detail=f"{event_type}:x{round(burst_score,1)}", created_at=today_epoch_days())`.
- `MonitorResult` gains `burst_alerts: int` (contract update in `src/analytics/contracts.py`).

### Part D — Push delivery

- **New `src/workflow/monitor/delivery.py`** — `async def post_alert(url: str, payload: dict, *, timeout_s: float) -> bool`: async httpx POST, returns True on 2xx, False on any error/timeout; never raises (fail-soft, loguru WARN).
- **`:Alert.pushed_at`** — new optional property; delivery idempotency marker.
- **`deliver_alerts(p: DeliverIn) -> DeliverResult` activity** (new, in `monitor/activities.py` + `MONITOR_ACTIVITIES`): if `webhook_url` empty → return `DeliverResult(delivered=0)` (no-op). Else read `:Alert WHERE a.pushed_at IS NULL RETURN ... LIMIT $cap` (cap e.g. 100); for each, `post_alert(url, {key,kind,entity,detail,created_at}, ...)`; on success `SET a.pushed_at = $now` (off-loop). Fail-soft, never raises across boundary; returns `DeliverResult(delivered:int, failed:int, error:str="")`.
- **`MonitorSweepWorkflow`** runs `detect_alerts` then `deliver_alerts` (second `execute_activity`, same queue/Schedule, own retry/timeouts). Sweep result rolls up both tallies.
- Contracts `DeliverIn`/`DeliverResult` (`_Frozen`) in `contracts.py`.

### Part E — Config (`MonitorSettings`, all dark)

`burst_enabled=False`, `burst_window_days=7`, `burst_baseline_windows=4`, `burst_min_count=2`, `burst_ratio=3.0`, `webhook_url=""`, `webhook_timeout_s=5`, `deliver_batch=100`. Russian descriptions added to `scripts/make_env.py::_ENV_DESCRIPTIONS`.

## File structure

**New:** `src/analytics/events_burst.py`, `src/workflow/monitor/delivery.py`, plus tests `tests/test_analytics/test_events_burst.py`, `tests/test_workflow/test_delivery.py`.
**Modified:** `src/config.py` (MonitorSettings burst/webhook), `src/analytics/primitives/events_llm.py` (trending_events + event_timeline fix), `src/analytics/contracts.py` (MonitorResult.burst_alerts, DeliverIn/DeliverResult), `src/workflow/monitor/activities.py` (burst detector + deliver_alerts + MONITOR_ACTIVITIES), `src/workflow/monitor/workflow.py` (2nd activity), `scripts/make_env.py`, `tests/test_analytics/test_catalog_complete.py` (42), `tests/test_workflow/test_monitor_activities.py` + `test_monitor_workflow.py` (burst + deliver), `tests/test_analytics/test_events_llm.py` (trending + window_days).

## Testing

- **events_burst** — `build_burst_cypher` shape: recent/baseline windows, `burst_score`, `min_count`/`ratio` thresholds, `watched_only` toggles the `p.watched = true` clause, polarity guard present.
- **trending_events** — primitive Cypher shape, fail-soft (None store → []), clamp, registered; `event_timeline.window_days` now applies `created_at >= since`.
- **detect_alerts** — burst finding writes `:Alert(kind='burst')` for a watched entity (fake store), gated off when `burst_enabled=False`; `burst_alerts` counted.
- **delivery** — `post_alert` with mocked httpx (2xx→True, error/timeout→False, no raise); `deliver_alerts` pushes only `pushed_at IS NULL`, marks `pushed_at`, empty `webhook_url` → no-op, fail-soft.
- **MonitorSweepWorkflow** — time-skip env runs `detect_alerts` then `deliver_alerts`, returns combined tally.
- **catalog completeness** → 42.
- **Full gate** — 13 == pre-existing baseline, regression-free; with `burst_enabled=false` + empty `webhook_url`, monitor behavior unchanged (E3-burst + push dark).

## Scope / boundaries (out of scope)

- Pluggable multi-sink delivery (Slack-formatted blocks, email/SMTP, Telegram, Grafana annotation) → future; v1 is one generic webhook.
- Batched webhook payloads / delivery retries beyond next-sweep → future (v1 posts one alert per request, retries by leaving `pushed_at` NULL).
- `event_ts`-based burst, event ER via embeddings → not in Wave 3.
- Webhook auth (signing/headers) beyond a plain POST → future (note in plan; a static `webhook_url` may already embed a token).

## Self-review

- **Placeholders:** none — all knobs have concrete defaults; thresholds locked (`ratio=3.0`, `min_count=2`, `W=7`, `B=4`).
- **Consistency:** `:Alert {key,kind,entity,detail,created_at,pushed_at}` consistent across `upsert_alert` (write), burst detector (write), `deliver_alerts` (read + `pushed_at` write), `alerts` read primitive. `MonitorResult` fields consistent between activity and workflow. `build_burst_cypher` single source for primitive (ratio=1) and detector (ratio=burst_ratio).
- **Scope:** single plan; two independently shippable parts (E3, then push). ~10–12 tasks.
- **Ambiguity:** burst axis = `created_at` (explicit); `trending_events` omits `event_ts` (explicit); delivery per-alert POST (explicit).

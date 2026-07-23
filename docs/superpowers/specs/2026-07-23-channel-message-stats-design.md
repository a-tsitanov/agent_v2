# Channel message-processing statistics

**Date:** 2026-07-23
**Status:** Approved (design)

## Problem

Operators have no way to see how many Telegram channel messages have been
processed, broken down by channel, by pipeline status, or over time. The only
existing view is `scripts/check_ingestion.py`, which runs a global
`SELECT status, COUNT(*) FROM documents GROUP BY status` — no per-channel or
per-group dimension, no time axis, CLI-only.

Goal: expose processed-message statistics with three dimensions —
**channel × status**, **group (folder) × status**, and a **daily time series** —
delivered via both an HTTP endpoint and a CLI wrapper.

## Background (current state)

- Each Telegram message becomes one `documents` row. `scripts/tg_ingest.py`
  reads messages via Telethon and POSTs each to `POST /api/v1/ingest`
  (`post_ingest`, `scripts/tg_ingest.py:74`). Dedup lives in the tg_ingest
  state file, not the DB.
- `documents` schema (`scripts/setup_db.py:33`): `id, path, department,
  doc_type, status, error, summary, doc_date, created_at, updated_at`.
  Status ∈ `pending, processing, completed, vector_only, failed` (CHECK
  constraint at `setup_db.py:39`).
- **The channel is not a column.** It survives only inside the filename
  embedded in `path`: `{doc_id}/tg_<channel>_<msgid>.txt`
  (`_message_to_doc`, `scripts/tg_ingest.py:64`).
- **The group is not persisted either.** `group` rides `IngestParams.group`
  into the workflow and is stamped onto Milvus chunk metadata (`doc_group`),
  but never written to `documents`. Canonical groups: `news, analytics,
  digest, opinion, official, data` (`src/retrieval/groups.py`).
- HTTP API is FastAPI + dishka DI. Routers live in `src/api/routes/`, are
  included in `src/api/main.py` with the `/api/v1` prefix, and inject
  `AsyncPostgres` via `FromDishka`. Auth is `require_api_key`
  (`X-API-Key` header, `src/api/auth.py`).
- Postgres access goes through the shared pool (`src/storage/pg_pool.py`);
  `AsyncPostgres` (`src/storage/postgres.py`) is the thin wrapper over
  `documents`.

**Known latent bug (fixed as part of this work):** `finalize.mark_skipped`
writes `status='skipped'` (`src/workflow/activities/finalize.py:179`), but
`skipped` is absent from the `documents.status` CHECK constraint — so a
classifier-skipped document's status UPDATE violates the constraint. Since
this work already edits `setup_db.py`, add `skipped` to the CHECK.

## Design

### Chosen approach

Store the channel and group as first-class columns on `documents`, populated
at insert time from data the ingest caller already supplies. Aggregate with
plain SQL `GROUP BY`. Rejected alternative: deriving the channel by parsing
`path` at query time (works only for TG-named docs, forces a `LIKE` scan on a
growing table, and can't recover the group at all).

### 1. Schema change (`scripts/setup_db.py`)

Idempotent, matching the existing `doc_date` add pattern:

```sql
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_channel TEXT NOT NULL DEFAULT '';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_group   TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS documents_source_channel_idx ON documents (source_channel);
CREATE INDEX IF NOT EXISTS documents_source_group_idx   ON documents (source_group);
```

Also amend the `documents.status` CHECK to include `skipped`:
`CHECK (status IN ('pending','processing','completed','vector_only','failed','skipped'))`.
Applied to fresh installs via the DDL; existing deployments get an idempotent
`ALTER TABLE ... DROP CONSTRAINT ... ADD CONSTRAINT ...` in the setup script.

Non-TG documents (manual uploads) leave both columns `''`. All statistics
queries filter `source_channel <> ''` (channel stats) or `source_group <> ''`
(group stats) so manual uploads never pollute channel counts.

### 2. Populating the columns

**New rows — caller supplies the values explicitly (no filename parsing in
the API):**
- `scripts/tg_ingest.py` `post_ingest` already sends `group`; add a `channel`
  multipart field carrying the channel slug (`channel.lstrip('@')`). All three
  call sites (`read_and_enqueue`, `reingest_channels`, `sync_round`) pass the
  channel they already have in hand.
- `POST /api/v1/ingest` (`src/api/routes/ingest.py`) gains
  `channel: str = Form(default="")`, validated only for length; passes
  `channel` + the existing `group` into `insert_pending`.
- `AsyncPostgres.insert_pending` (`src/storage/postgres.py:73`) gains
  `source_channel: str = ""`, `source_group: str = ""` params and writes them.

**Existing rows — one-shot backfill:** a script (or an idempotent block in
`setup_db.py`, gated on empty `source_channel`) runs:

```sql
UPDATE documents
   SET source_channel = substring(path from 'tg_(.+)_[0-9]+\.txt$')
 WHERE source_channel = ''
   AND path ~ 'tg_.+_[0-9]+\.txt$';
```

`source_group` cannot be backfilled (group was never recorded historically) —
old rows keep `''`. Regex note: `.+` is greedy, so `tg_a_b_123.txt` →
channel `a_b`, msgid `123`. Channels containing a trailing `_<digits>`
segment are a theoretical ambiguity; accepted, since Telegram slugs don't end
that way in practice.

### 3. Shared aggregation SQL (`src/storage/postgres.py`)

Two new `AsyncPostgres` methods — the single source of truth, called by both
the route and the CLI:

- `status_counts_by(dimension, since=None, until=None)` where
  `dimension ∈ {'source_channel','source_group'}`. Returns one row per
  key with a count per status plus a total:
  `SELECT <dim> AS key, status, COUNT(*) FROM documents
   WHERE <dim> <> '' [AND created_at >= since] [AND created_at < until]
   GROUP BY key, status`. The method pivots the (key, status, n) rows into
  `[{key, total, completed, failed, pending, processing, vector_only, skipped}]`
  sorted by total desc.
- `timeline_counts(date_field, group_by=None, channel=None, group=None,
  since=None, until=None)` where `date_field ∈ {'created_at','doc_date'}`.
  Buckets by day: `SELECT date_trunc('day', <date_field>) AS day[, key] ,
  COUNT(*) ... GROUP BY day[, key] ORDER BY day`. Optional `channel`/`group`
  equality filters; optional `group_by ∈ {channel, group}` adds the key
  column. `date_field`/`dimension`/`group_by` are validated against
  hardcoded allowlists before interpolation (never user-interpolated raw) —
  values bind as parameters.

### 4. HTTP endpoints (`src/api/routes/stats.py`)

New router, `/api/v1` prefix, `dependencies=[Depends(require_api_key)]`,
injects `AsyncPostgres` via `FromDishka` + `@inject`. Registered in
`src/api/main.py`.

- `GET /api/v1/stats/messages?group_by=channel|group&since=&until=`
  → per-channel (default) or per-group status breakdown. `group_by` defaults
  to `channel`; `since`/`until` are optional ISO dates filtering `created_at`.
  Response: `{group_by, rows: [{key, total, completed, failed, pending,
  processing, vector_only, skipped}]}`.
- `GET /api/v1/stats/timeline?date_field=created_at|doc_date&group_by=&channel=&group=&since=&until=`
  → daily buckets. `date_field` defaults to `created_at`. Response:
  `{date_field, buckets: [{day, key?, count}]}`.

Invalid enum params (`group_by`, `date_field`) → `422`. Pydantic response
models mirror the shapes above.

### 5. CLI (`scripts/message_stats.py`)

Thin wrapper over the same `AsyncPostgres` methods (no HTTP, direct DB via the
settings DSN). `argparse` subcommands:
- `channels [--since --until]` → calls `status_counts_by('source_channel', …)`,
  prints an aligned table.
- `groups [--since --until]` → `status_counts_by('source_group', …)`.
- `timeline [--date-field --group-by --channel --group --since --until]`
  → `timeline_counts(…)`.

### 6. Testing

- Unit tests for the two `AsyncPostgres` aggregation methods against a
  Postgres fixture (follow the existing DB test pattern in `tests/`): seed
  `documents` rows across channels/groups/statuses/dates, assert the pivoted
  output and the `source_*=''` exclusion.
- Route tests via FastAPI `TestClient` with a dishka override binding a fake
  `AsyncPostgres`: assert JSON shape, `group_by`/`date_field` validation
  (`422`), and auth enforcement.
- A focused test that `insert_pending` persists `source_channel`/`source_group`.

## Out of scope (YAGNI)

- No new analytics/materialized tables — live `GROUP BY` on `documents` is
  cheap at current volumes and the new indexes cover it.
- No week/month bucketing — day only; callers roll up client-side.
- No dashboard/UI — JSON + CLI table only.
- No per-message drill-down endpoint.

## Files touched

- `scripts/setup_db.py` — columns, indexes, CHECK amend, backfill.
- `src/storage/postgres.py` — `insert_pending` params, two aggregation methods.
- `src/api/routes/ingest.py` — `channel` Form field → `insert_pending`.
- `src/api/routes/stats.py` — **new** router.
- `src/api/main.py` — register the stats router.
- `scripts/tg_ingest.py` — send `channel` field from all post sites.
- `scripts/message_stats.py` — **new** CLI.
- `tests/…` — aggregation + route tests.

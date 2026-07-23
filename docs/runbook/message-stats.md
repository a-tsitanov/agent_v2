# Message statistics

## What it does
Processed-message counts (per channel, per group, and over time) come from
the `documents` table's `source_channel` / `source_group` columns. These are
populated by `tg_ingest` via the `/ingest` endpoint's `channel` + `group` form
fields at ingest time. Two read-only surfaces expose the same aggregation
logic (`AsyncPostgres.status_counts_by` / `timeline_counts`): an HTTP API for
dashboards/tooling, and a CLI for direct DB queries without going through the
API.

## HTTP endpoints
Both require the `X-API-Key` header (same key as the rest of `/api/v1`).

- `GET /api/v1/stats/messages?group_by=channel|group[&since=YYYY-MM-DD][&until=YYYY-MM-DD]`
  → per-channel/group counts by status (`pending`/`processing`/`completed`/`vector_only`/`failed`/`skipped`, plus `total`).
- `GET /api/v1/stats/timeline?date_field=created_at|doc_date[&group_by=channel|group][&channel=][&group=][&since=][&until=]`
  → daily message counts, optionally broken out by channel/group and filtered
  to a single channel/group.

Example:

    curl -s -H "X-API-Key: $KEY" \
      "http://localhost:8000/api/v1/stats/messages?group_by=channel&since=2026-07-01" | jq

    curl -s -H "X-API-Key: $KEY" \
      "http://localhost:8000/api/v1/stats/timeline?date_field=doc_date&group_by=channel&channel=somechannel" | jq

## CLI
Direct DB access, no API/key needed — reads straight from Postgres using
`settings.postgres.dsn`:

    python -m scripts.message_stats channels [--since 2026-07-01] [--until 2026-07-23]
    python -m scripts.message_stats groups
    python -m scripts.message_stats timeline [--date-field doc_date] [--group-by channel] [--channel somechannel] [--group somegroup]

`channels`/`groups` print an aligned status-count table; `timeline` prints one
`day  [key]  count` line per bucket.

## Historical-data caveat
`source_group` is empty for documents ingested before this feature shipped —
`group` was not persisted historically, so there is nothing to backfill.
`source_channel` was backfilled from the filename in `path` for Telegram rows
only (via `scripts/setup_db.py`); non-Telegram historical documents have no
`source_channel` either.

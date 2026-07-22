# Manual channel reingest command + low-priority ingest lane

**Date:** 2026-07-22
**Status:** design (awaiting review)

## Problem

Operators need a way to manually **reingest a Telegram channel**: pick a channel
that is already tracked in one of the configured folders, read the newest N
messages of its history, and push them through the normal ingest pipeline —
but at **low priority**, so these bulk backfill documents are dispatched only
when the live feed has nothing waiting. A reingest of a busy channel must never
delay live messages that arrive during it.

Today there is no such command, and neither ingest backlog backend has any
notion of priority (both are strictly FIFO).

## Context (current state)

- **Deployed backend:** `INGEST_QUEUE_BACKEND=rabbitmq`, single queue
  `RABBITMQ_QUEUES=ingest.pending`, `INGEST_ADMISSION_MAX_INFLIGHT=10` (K=10).
- **TG ingester:** `scripts/tg_ingest.py`, a standalone argparse script with two
  modes — continuous sync (`_run_sync`, per-dialog cursor state) and legacy
  backfill (`_run_backfill` / `read_and_enqueue`, no state, reads last `--limit`
  per named channel). Folder machinery already exists: `resolve_folders`,
  `dialog_in_folders`, `resolve_group_map` (folder → `doc_group`).
- **Path:** `scripts/tg_ingest.py` → `POST /api/v1/ingest` → `submit_document`
  (`src/workflow/ingest_submit.py`) → RabbitMQ `publish_ingest`
  (`src/ingest_queue/publisher.py`) → consumer (`src/ingest_queue/consumer.py`,
  global `prefetch=K`) → `DocumentIngestWorkflow`.
- **No API-side dedup:** every `POST /ingest` mints a fresh `doc_id`, so
  re-posting the same message genuinely re-runs the pipeline (no workflow-id
  collision). This is exactly the reprocess semantics reingest wants.
- **Priority absent everywhere:** confirmed by grep across `src/ingest_queue/`,
  `ingest_submit.py`, `ingest_scheduler.py`, `admission.py`, `api/routes/`.

### Why message priority, not a separate queue

RabbitMQ has **no cross-queue preference** for a single consumer. The consumer
declares and consumes *all* configured queues under one shared global
`prefetch=K`, so a second `ingest.backfill` queue would just be round-robined
against `ingest.pending` — it would NOT "drain only when main is empty". Getting
main-first out of two queues would require rewriting the consumer into a
preference/pull model (drain backfill only when pending has 0 ready), which is
more code, more failure modes, and still cannot preempt in-flight work.

RabbitMQ **message priority** is the native primitive for exactly this: within
one queue, a lower-priority message is delivered to a free consumer slot only
when no higher-priority message is ready. That is precisely "dispatch backfill
only when the live lane has nothing waiting." Chosen approach.

## Design

Two parts: (1) a priority lane on the existing queue, (2) a reingest CLI mode
that posts at low priority.

### Part 1 — Priority lane

**Priority levels** (module constants, `src/ingest_queue/`):

- `PRIO_LIVE = 5` — live sync + normal `/ingest`.
- `PRIO_BACKFILL = 0` — manual reingest.

**Queue argument:** declare the work queue with `x-max-priority`, value from a
new setting `RABBITMQ_MAX_PRIORITY` (default `10`).

**Changes:**

| File | Change |
|---|---|
| `src/config.py` (`RabbitMQSettings`) | add `max_priority: int = Field(default=10, ge=1, le=255)` |
| `src/ingest_queue/topology.py` | add `"x-max-priority": cfg.max_priority` to the per-queue `args` dict |
| `src/ingest_queue/publisher.py` | `publish_ingest(params, queue=None, priority=PRIO_LIVE)` → set `aio_pika.Message(..., priority=priority)` |
| `src/workflow/ingest_submit.py` | `submit_document(..., priority: int = PRIO_LIVE)`; forward to `publish_ingest` on the rabbitmq path only (temporal path ignores it — documented) |
| `src/api/routes/ingest.py` | add `priority: int \| None = Form(default=None)`; validate `0 <= priority <= settings.rabbitmq.max_priority` → 422 on out-of-range; `None` → `PRIO_LIVE`; pass to `submit_document` |

Live sync and normal uploads publish at priority 5; reingest publishes at 0.
When a consumer slot frees, the broker delivers a priority-0 message only if no
priority-5 message is ready → live always wins the next free slot.

**IMPORTANT — default priority must be > backfill.** RabbitMQ treats a message
with no `priority` as priority 0. If live messages were published without a
priority they would tie with backfill and lose the ordering. Therefore the
publisher's default is `PRIO_LIVE` (not unset), and `/ingest` maps `None` →
`PRIO_LIVE` before calling `submit_document`.

**Migration (one-time, operator).** The existing durable `ingest.pending` was
declared without `x-max-priority`. Re-declaring it with the arg raises
`PRECONDITION_FAILED` (queue arguments are immutable), which would break both
publish and consume. So, in a quiet window when the queue is drained:

```
# via management UI, or:
rabbitmqadmin delete queue name=ingest.pending
```

Then deploy the new code — `declare_ingest_topology` recreates the queue with
`x-max-priority`. The backlog is transient (persistent messages not yet
admitted); draining first avoids losing any. This step is added to the ingest
runbook. The DLQ (`ingest.dlq`) is unaffected (no priority needed there).

**Accepted limitation.** Priority governs the *next free slot*, not preemption.
With K=10, a burst of backfill can occupy slots and a live message that arrives
mid-reingest waits for one in-flight backfill document to finish (seconds to a
few minutes per document). This is acceptable for KB ingestion; no per-priority
capacity reservation is built (YAGNI). If it ever bites, the follow-up is a
bounded backfill concurrency, tracked separately.

### Part 2 — Reingest CLI mode (`scripts/tg_ingest.py`)

A third mode, selected by a dedicated flag, alongside sync and legacy backfill.

**New flags:**

- `--reingest CHANNEL` — one or more channels (comma-separated), each an
  `@username` or numeric dialog id. Presence of this flag selects reingest mode.
- `--reingest-limit N` — newest N messages per channel (default `100`, `ge=1`).
  Plain newest-N (`iter_messages(limit=N, reverse=True)`); no "0 = full history"
  special case.

**Mode dispatch** (`main`): `--reingest` takes precedence, e.g.

```python
if args.reingest:
    asyncio.run(_run_reingest())
elif args.channels:
    asyncio.run(_run_backfill())
else:
    asyncio.run(_run_sync())
```

**`_run_reingest()` flow:**

1. Connect the Telethon client (existing session/`--session`).
2. Load the account's folders (`GetDialogFiltersRequest`) and build the folder
   spec via `resolve_folders(..., --folders)` and the id→group map via
   `resolve_group_map`, exactly as `_run_sync` does.
3. For each requested channel: resolve it to a dialog/entity and **assert it is
   in a tracked folder** using `dialog_in_folders(dialog, spec)`. If not in any
   configured folder → log an error and exit non-zero ("channel must be in a
   tracked folder"). No silent fall-through.
4. Derive the channel's `group` (folder-based `doc_group`) from the group map,
   so reingested documents are stamped identically to live ones.
5. `iter_messages(channel, limit=N, reverse=True)` → `_message_to_doc` →
   `post_ingest(..., group=<group>, priority=PRIO_BACKFILL)`.
6. **Do not touch the `--state` cursor.** Reingest reads old history; the live
   sync cursor tracks the newest seen id and must stay untouched so the running
   sync is unaffected.

**`post_ingest` change:** add a `priority: int | None = None` parameter; when
set, include `data["priority"] = str(priority)` in the multipart POST. The
reingest path passes `PRIO_BACKFILL` (0); sync/backfill callers pass nothing
(→ live default at the API).

**Reprocess semantics:** because the API mints a fresh `doc_id` per POST, each
reingested message becomes a new document and re-runs the full pipeline. This is
the intended behavior (e.g. reingest after an embedding-model migration).

**Invocation:** one-off container run reusing the mounted session + folders:

```
docker compose -f docker-compose.prod.yml -f docker-compose.tg-ingest.yml \
  run --rm tg-ingest python -m scripts.tg_ingest \
  --api-base http://api:8000 --api-key "$TG_INGEST_API_KEY" \
  --session /data/tg_ingest.session --state /data/tg_ingest.state.json \
  --folders "$TG_INGEST_FOLDERS" \
  --reingest @somechannel --reingest-limit 500
```

No API-side Telethon is introduced; the reingest driver lives where the MTProto
session already lives.

## Behavior summary

- Live feed keeps priority; reingested history only fills otherwise-idle
  capacity.
- Reingest is folder-scoped: refuses channels not in a tracked folder, and
  stamps the correct `doc_group`.
- Reingest re-runs the pipeline for each message (fresh `doc_id`).
- Live sync and its cursor are never affected by a reingest run.

## Testing

Unit tests (no live broker / no live Telegram):

- **topology** — declared work queue carries `x-max-priority == cfg.max_priority`
  (assert on the `arguments` passed to a fake channel's `declare_queue`).
- **publisher** — `publish_ingest(..., priority=0)` produces a `Message` with
  `priority == 0`; default call yields `PRIO_LIVE`.
- **`/ingest` route** — `priority` out of range → 422; `priority=None` →
  `submit_document` called with `PRIO_LIVE`; `priority=0` forwarded as 0.
- **tg_ingest reingest** — with a fake Telethon client + captured POSTs:
  (a) channel not in folder spec → non-zero exit, no POSTs;
  (b) in-folder channel → N POSTs each carrying `priority=0` and the folder's
      `group`;
  (c) the `--state` file is byte-for-byte unchanged after a reingest run.

## Files touched

- `src/config.py` — `RabbitMQSettings.max_priority`
- `src/ingest_queue/topology.py` — `x-max-priority` arg
- `src/ingest_queue/publisher.py` — `priority` param + constants (`PRIO_LIVE`,
  `PRIO_BACKFILL`)
- `src/workflow/ingest_submit.py` — thread `priority`
- `src/api/routes/ingest.py` — `priority` form param + validation
- `scripts/tg_ingest.py` — `--reingest`/`--reingest-limit`, `_run_reingest`,
  `post_ingest` priority param
- docs: this spec + ingest runbook migration note; `.env.example` for
  `RABBITMQ_MAX_PRIORITY`
- tests under `tests/` mirroring the modules above

## Out of scope

- Per-priority capacity reservation / bounded backfill concurrency (follow-up
  only if live latency during reingest proves a problem).
- The Temporal backend priority path (deployed backend is rabbitmq; temporal
  path ignores `priority` and is documented as such).
- Any change to legacy `--channels` backfill mode.

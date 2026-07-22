# Manual channel reingest + low-priority lane

## What it does
`scripts/tg_ingest.py --reingest @channel --reingest-limit N` re-reads the
newest N messages of a folder-tracked channel and posts them to `/ingest` at
RabbitMQ priority 0 (`PRIO_BACKFILL`). Live ingest posts at priority 5
(`PRIO_LIVE`), so the reingest backlog is delivered to a free consumer slot
only when no live message is ready. Each posted message becomes a new document
(the API mints a fresh `doc_id`), so the pipeline reprocesses it. The live sync
cursor (`--state`) is untouched.

## One-time migration (required before deploying this change)
The existing `ingest.pending` queue was declared without `x-max-priority`;
queue arguments are immutable, so it must be deleted and redeclared. Do this in
a quiet window when the queue is drained (persistent messages not yet admitted
would be lost):

    # confirm it is empty first (messages column):
    rabbitmqctl list_queues name messages | grep ingest.pending
    # then delete — the app redeclares it with x-max-priority on next connect:
    rabbitmqadmin delete queue name=ingest.pending

`declare_ingest_topology` stamps `x-max-priority` on EVERY queue listed in
`RABBITMQ_QUEUES`, so repeat the delete+redeclare for each one. The deployed
config is single-queue (`ingest.pending`), so today that's the only queue to
migrate — but check `RABBITMQ_QUEUES` before assuming.

Set `RABBITMQ_MAX_PRIORITY` (default 10) in `.env` if you want a different
ceiling. Restart the API + consumer so `declare_ingest_topology` recreates the
queue with the priority arg. The DLQ needs no change.

If this code is deployed before the migration runs, the failure is loud, not
silent: publishes against the un-migrated queue return 500, and the consumer
crash-loops on the same queue-args mismatch — both fail obviously (not silent
corruption) until the queue is deleted per above, at which point both recover
on their own.

## Running a reingest
One-off container run, reusing the mounted Telethon session + folders:

    docker compose -f docker-compose.prod.yml -f docker-compose.tg-ingest.yml \
      run --rm tg-ingest python -m scripts.tg_ingest \
      --api-base http://api:8000 --api-key "$TG_INGEST_API_KEY" \
      --session /data/tg_ingest.session --state /data/tg_ingest.state.json \
      --folders "$TG_INGEST_FOLDERS" \
      --reingest @somechannel --reingest-limit 500

The channel must be in one of `--folders`; otherwise the command logs an error
and exits non-zero without posting anything.

## Known limitation
Priority governs the next free consumer slot, not preemption. With K=10 a burst
of reingest can occupy all slots; a live message arriving mid-reingest waits for
one in-flight backfill document to finish (seconds to a few minutes). Acceptable
for KB ingest; if it ever matters, bound backfill concurrency (follow-up).

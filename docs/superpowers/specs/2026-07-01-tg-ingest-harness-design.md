# TG → ingest test harness — Design

**Status:** Approved design (brainstorming) — 2026-07-01. Next: writing-plans → implementation.

## Goal

A test harness to exercise the RabbitMQ-backed ingest pipeline with real content: read the last **N** messages from a list of Telegram channels and enqueue each as a document via the real `POST /ingest` endpoint (which uploads to MinIO and publishes `IngestParams` to the rabbit queue). Lets us validate the `INGEST_QUEUE_BACKEND=rabbitmq` path end-to-end.

## Decisions (locked via brainstorming)

- **Read method:** Telethon (MTProto **user** client) — reads channel history via `iter_messages`. (Not Bot API — can't read history.)
- **Mode:** one-shot **backfill** (read last N from each channel, enqueue, exit). Not a continuous watcher.
- **Enqueue path:** HTTP `POST /api/v1/ingest` per message (multipart) — reuses the route's MinIO upload + `IngestParams` build + `publish_ingest`. Most faithful; least code. Requires the API + consumer + rabbit running.
- **document_date:** the **post timestamp** (`message.date`, as `YYYY-MM-DD`). No text-date parsing.
- **Text only:** skip media-only / empty-text messages.

## Components

### 1. RabbitMQ in dev compose (infra for the test)
Add a `rabbitmq` service to **`docker-compose.yml`** (mirror `docker-compose.prod.yml`, but NO profile): `image: rabbitmq:3-management`, ports `5672`/`15672`, healthcheck (`rabbitmq-diagnostics -q ping`), a named volume. In the **default** service set — `docker compose up -d` brings it up, no extra flags.

### 2. `scripts/tg_ingest.py` (the reader → enqueuer)
Small, focused units:
- `_message_to_doc(msg, channel) -> tuple[str, str, str] | None` — **pure**: returns `(filename, text, document_date)` or `None` when the message has no text. `filename = f"tg_{channel}_{msg.id}.txt"`, `text = msg.message`, `document_date = msg.date.date().isoformat()`. Duck-typed message (any object with `.id`, `.message`, `.date`) → unit-testable without Telethon.
- `post_ingest(client, api_base, api_key, filename, text, document_date, queue) -> bool` — one multipart `POST {api_base}/api/v1/ingest` via `httpx` (already a dep): `files={"file": (filename, text.encode(), "text/plain")}`, `data={"queue": queue, "document_date": document_date}`, auth header per the app's `require_api_key` (confirm header name at impl — likely `X-API-Key`). Returns True on 2xx; fail-soft (logs + False on error, never raises).
- `read_and_enqueue(tg_client, http, channels, limit, ...) -> Counter` — orchestration: for each channel, `iter_messages(channel, limit=N, reverse=True)` (oldest→newest); map via `_message_to_doc`; `post_ingest`; tally `sent/skipped/failed`.
- `main()` — argparse + Telethon `TelegramClient` setup (session), runs `read_and_enqueue`, prints the summary, exit 0.

**CLI:** `--channels @a,@b` (comma list; or `--channels-file path`), `--limit N` (default 50), `--queue <name>` (default = first of `RABBITMQ_QUEUES`), `--api-base` (default `http://localhost:8000`), `--session` (default `.tg_ingest.session`).
**Secrets (env):** `TG_API_ID`, `TG_API_HASH` (from my.telegram.org), API key (`--api-key` or an env var matching the app's auth). First run does an interactive Telethon login (phone + code) → writes the session file (gitignored).

### 3. Dependency
Add **`telethon`** as an **optional/dev** dependency group (e.g. `[project.optional-dependencies].tg`) so it doesn't bloat the core app image — the harness is tooling, not runtime. `httpx` is already present.

### 4. Runbook (short, in the script docstring + a docs note)
1. `docker compose up -d rabbitmq` (default service, no profile flag)
2. Host env: `INGEST_QUEUE_BACKEND=rabbitmq`, `RABBITMQ_URL=amqp://guest:guest@localhost:5672/`, `RABBITMQ_QUEUES=<name>`.
3. Start consumer: `uv run python -m src.ingest_queue.consumer`.
4. Start API: `uv run uvicorn src.api.main:app --port 8000`.
5. Run harness: `TG_API_ID=… TG_API_HASH=… uv run python -m scripts.tg_ingest --channels @foo --limit 20 --queue <name>`.

## Data flow
`TG channel → Telethon iter_messages → text + post-date → POST /api/v1/ingest → MinIO put_object + IngestParams → publish_ingest → rabbit queue → consumer → DocumentIngestWorkflow → graph.`

## Testing
- **Unit** (`tests/test_scripts/test_tg_ingest.py`, no network/Telethon):
  - `_message_to_doc` with a fake message (text + `date`) → asserts `filename`, ISO `document_date`, `text`.
  - `_message_to_doc` returns `None` for empty/None text (skip).
  - `post_ingest` with a fake httpx client → asserts the multipart fields (file name/content, `queue`, `document_date`) and 2xx→True / error→False (fail-soft).
- **Integration** (manual, per runbook): live Telethon + rabbit + consumer — not automated.

## Error handling
- Per-message fail-soft: a bad message / failed POST is logged and counted `failed`, never aborts the run.
- Telethon/connection errors at startup → clear message + non-zero-only-if-nothing-sent (or exit 0 with the summary; TBD in plan → **exit 0 with summary always**, so a partial run is not a hard failure).

## Out of scope
Continuous watching, media/file download & OCR, cross-run dedup (re-runs create new `doc_id`s — acceptable for a test; note it), date-from-text parsing, non-text posts.

## Self-review
- **Placeholders:** none — defaults are concrete; the one impl-time confirm (auth header name) is flagged, not left vague.
- **Consistency:** `document_date` = `message.date` everywhere; enqueue strictly via `POST /ingest`; units (`_message_to_doc` pure, `post_ingest` I/O) are separable and match the test plan.
- **Scope:** single script + one compose service + a dep + a test — one plan, appropriately sized.
- **Ambiguity:** date source (post date), text-only, one-shot — all explicit.

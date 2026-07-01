# TG → ingest test harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `scripts/tg_ingest.py` harness that reads the last N messages from Telegram channels (Telethon) and enqueues each as a document via `POST /api/v1/ingest`, plus a profile-gated `rabbitmq` service in the dev compose so the queue path can be exercised.

**Architecture:** One-shot backfill. Pure `_message_to_doc` (message → filename/text/post-date) + fail-soft async `post_ingest` (httpx multipart to the real route) + `read_and_enqueue` orchestration + a thin `main` that owns the Telethon client. Telethon is imported lazily inside `main` only, so the module (and its unit tests) run without the dependency.

**Tech Stack:** Python 3.12, Telethon (MTProto user client, optional dep), httpx (already a dep), argparse, loguru, pytest (`asyncio_mode=auto`), Docker Compose, ruff.

Design: `docs/superpowers/specs/2026-07-01-tg-ingest-harness-design.md`.

## Global Constraints

- **Enqueue only via `POST /api/v1/ingest`** (multipart). Do NOT reimplement MinIO upload / `publish_ingest`. Auth header is `X-API-Key` (checked against `settings.api.keys`; local default key `dev-local-key`). Route form fields used: `file` (UploadFile), `queue` (str, optional), `document_date` (str, ISO `YYYY-MM-DD`, optional).
- **`document_date` = the post timestamp** (`message.date.date().isoformat()`), never parsed from text.
- **Text only:** skip messages whose text is empty/None (media-only).
- **Fail-soft per message:** a failed POST / bad message is logged and counted, never aborts the run. `main` always exits 0 with a summary.
- **Telethon imported lazily** (inside `main`), so `import scripts.tg_ingest` works without telethon installed (unit tests must not require it).
- **Conventions:** loguru `logger` (`from loguru import logger`); ruff ruleset `E,F,I,B,UP,SIM,RUF`, line 100, py312, no `# noqa: BLE001` (mirror the fail-soft broad-catch style used in `src/analytics/store_query.py`). Cyrillic allowed.
- **Git:** commit locally; end each commit body with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## File Structure

**New:** `scripts/tg_ingest.py`, `tests/test_scripts/test_tg_ingest.py`.
**Modified:** `docker-compose.yml` (add `rabbitmq` service + `rabbitmq_data` volume, profile `rabbitmq`), `pyproject.toml` (add `telethon` optional-dependency group), `.gitignore` (ignore `*.session`).

---

## Task 1: RabbitMQ dev-compose service + telethon optional dep

**Files:**
- Modify: `docker-compose.yml` (add service `rabbitmq` + volume `rabbitmq_data`)
- Modify: `pyproject.toml` (`[project.optional-dependencies]` add `tg`)
- Modify: `.gitignore` (add `*.session`)

**Interfaces — Produces:** a `rabbitmq` compose service (profile `rabbitmq`, ports 5672/15672) startable via `docker compose --profile rabbitmq up -d rabbitmq`; `pip install '.[tg]'` provides `telethon`.

- [ ] **Step 1: Add the rabbitmq service** — in `docker-compose.yml`, under `services:`, add (mirrors `docker-compose.prod.yml`):

```yaml
  rabbitmq:
    image: rabbitmq:3-management
    profiles: ["rabbitmq"]
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER:-guest}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD:-guest}
    ports:
      - "${RABBITMQ_PORT:-5672}:5672"
      - "${RABBITMQ_MGMT_PORT:-15672}:15672"
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 20s
    restart: unless-stopped
```

And under the top-level `volumes:` block add `  rabbitmq_data:`.

- [ ] **Step 2: Add the telethon optional dep** — in `pyproject.toml`, under `[project.optional-dependencies]`, add a group:

```toml
tg = ["telethon>=1.36,<2"]
```

- [ ] **Step 3: gitignore the session** — append to `.gitignore`:

```
# Telethon session for the tg_ingest harness
*.session
*.session-journal
```

- [ ] **Step 4: Verify** — the compose parses with the profile:

Run: `docker compose --profile rabbitmq config >/dev/null && echo OK`
Expected: `OK` (no YAML/schema error).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml pyproject.toml .gitignore
git commit -m "chore(ingest): dev rabbitmq compose service (profile) + telethon optional dep"
```

---

## Task 2: `scripts/tg_ingest.py` — `_message_to_doc` + `post_ingest`

**Files:**
- Create: `scripts/tg_ingest.py`
- Test: `tests/test_scripts/test_tg_ingest.py`

**Interfaces — Produces:**
- `_message_to_doc(msg, channel: str) -> tuple[str, str, str] | None` — `(filename, text, document_date)` or `None` when the message text is empty. `filename = f"tg_{channel.lstrip('@')}_{msg.id}.txt"`, `text = msg.message.strip()`, `document_date = msg.date.date().isoformat()`.
- `async post_ingest(http, api_base, api_key, filename, text, document_date, queue) -> bool` — one multipart `POST {api_base}/api/v1/ingest`; True on 2xx, False on non-2xx/error (fail-soft, never raises).

- [ ] **Step 1: Write the failing tests** — create `tests/test_scripts/test_tg_ingest.py`:

```python
from datetime import datetime, timezone

import pytest

from scripts.tg_ingest import _message_to_doc, post_ingest


class _FakeMsg:
    def __init__(self, id, message, date):
        self.id = id
        self.message = message
        self.date = date


def test_message_to_doc_maps_fields():
    m = _FakeMsg(42, "  hello world  ", datetime(2024, 3, 1, 12, 0, tzinfo=timezone.utc))
    fn, text, dd = _message_to_doc(m, "@chan")
    assert fn == "tg_chan_42.txt"
    assert text == "hello world"
    assert dd == "2024-03-01"


def test_message_to_doc_skips_empty():
    m = _FakeMsg(1, None, datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert _message_to_doc(m, "@c") is None
    m2 = _FakeMsg(2, "   ", datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert _message_to_doc(m2, "@c") is None


@pytest.mark.asyncio
async def test_post_ingest_true_on_2xx():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(url=url, headers=headers, files=files, data=data)
            return _Resp()

    ok = await post_ingest(_Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1")
    assert ok is True
    assert sent["url"] == "http://api/api/v1/ingest"
    assert sent["headers"]["X-API-Key"] == "k"
    assert sent["data"]["queue"] == "q1" and sent["data"]["document_date"] == "2024-03-01"
    assert sent["files"]["file"][0] == "f.txt"


@pytest.mark.asyncio
async def test_post_ingest_false_on_error():
    class _Client:
        async def post(self, *a, **k):
            raise RuntimeError("connection refused")

    assert await post_ingest(_Client(), "http://api", "k", "f", "t", "d", "q") is False
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py -q`
Expected: FAIL (`ModuleNotFoundError: scripts.tg_ingest`).

- [ ] **Step 3: Implement** — create `scripts/tg_ingest.py` (helpers only; `main` in Task 3):

```python
"""TG → ingest test harness: read last-N channel messages (Telethon) and
enqueue each via POST /api/v1/ingest (which uploads to MinIO + publishes to
the rabbit queue). One-shot backfill, text-only, document_date = post date.

Runbook:
  1. docker compose --profile rabbitmq up -d rabbitmq
  2. export INGEST_QUEUE_BACKEND=rabbitmq RABBITMQ_URL=amqp://guest:guest@localhost:5672/
     export RABBITMQ_QUEUES=<name>
  3. uv run python -m src.ingest_queue.consumer        # queue → DocumentIngestWorkflow
  4. uv run uvicorn src.api.main:app --port 8000       # the API
  5. TG_API_ID=… TG_API_HASH=… uv run python -m scripts.tg_ingest \
       --channels @foo --limit 20 [--queue <name>] [--api-key dev-local-key]

TG_API_ID / TG_API_HASH come from https://my.telegram.org. First run does an
interactive Telethon login (phone + code) and writes the session file.
"""

from __future__ import annotations

from typing import Any

from loguru import logger


def _message_to_doc(msg: Any, channel: str) -> tuple[str, str, str] | None:
    """Map a Telethon message → (filename, text, document_date), or None if empty."""
    text = (getattr(msg, "message", None) or "").strip()
    if not text:
        return None
    filename = f"tg_{channel.lstrip('@')}_{msg.id}.txt"
    document_date = msg.date.date().isoformat()
    return filename, text, document_date


async def post_ingest(
    http: Any,
    api_base: str,
    api_key: str,
    filename: str,
    text: str,
    document_date: str,
    queue: str | None,
) -> bool:
    """POST one document to /api/v1/ingest (multipart). True on 2xx; fail-soft."""
    data: dict[str, str] = {"document_date": document_date}
    if queue:
        data["queue"] = queue
    try:
        resp = await http.post(
            f"{api_base}/api/v1/ingest",
            headers={"X-API-Key": api_key},
            files={"file": (filename, text.encode("utf-8"), "text/plain")},
            data=data,
        )
        return 200 <= resp.status_code < 300
    except Exception as exc:
        logger.warning("post_ingest failed file={f}: {e}", f=filename, e=exc)
        return False
```

> Note: the `test_post_ingest_true_on_2xx` test passes `queue="q1"` so `data["queue"]` is set — the assertion `sent["data"]["queue"] == "q1"` holds. `post_ingest` omits `queue` from `data` only when it is falsy.

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
uv run ruff format scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
git commit -m "feat(ingest): tg_ingest harness helpers (_message_to_doc + post_ingest)"
```

---

## Task 3: `read_and_enqueue` orchestration + `main` CLI

**Files:**
- Modify: `scripts/tg_ingest.py` (add `read_and_enqueue` + `main`)
- Test: `tests/test_scripts/test_tg_ingest.py` (extend)

**Interfaces — Consumes:** `_message_to_doc`, `post_ingest`. **Produces:**
- `async read_and_enqueue(tg_client, http, *, channels, limit, api_base, api_key, queue) -> collections.Counter` — for each channel, `tg_client.iter_messages(channel, limit=limit, reverse=True)`; map + post; tally `sent/skipped/failed`.
- `main() -> int` — argparse + lazy Telethon client + httpx client; runs `read_and_enqueue`; logs the summary; returns 0.

- [ ] **Step 1: Write the failing test** — append to `tests/test_scripts/test_tg_ingest.py`:

```python
@pytest.mark.asyncio
async def test_read_and_enqueue_tallies_sent_and_skipped():
    from scripts.tg_ingest import read_and_enqueue

    msgs = [
        _FakeMsg(1, "alpha", datetime(2024, 1, 1, tzinfo=timezone.utc)),
        _FakeMsg(2, "", datetime(2024, 1, 2, tzinfo=timezone.utc)),  # skipped (empty)
        _FakeMsg(3, "gamma", datetime(2024, 1, 3, tzinfo=timezone.utc)),
    ]

    class _TG:
        async def iter_messages(self, channel, limit, reverse):
            assert reverse is True
            for m in msgs:
                yield m

    posted: list[str] = []

    class _Resp:
        status_code = 202

    class _HTTP:
        async def post(self, url, headers=None, files=None, data=None):
            posted.append(files["file"][0])
            return _Resp()

    tally = await read_and_enqueue(
        _TG(), _HTTP(), channels=["@c"], limit=10,
        api_base="http://a", api_key="k", queue="q",
    )
    assert tally["sent"] == 2 and tally["skipped"] == 1 and tally["failed"] == 0
    assert posted == ["tg_c_1.txt", "tg_c_3.txt"]
```

- [ ] **Step 2: Run → FAIL**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py::test_read_and_enqueue_tallies_sent_and_skipped -q`
Expected: FAIL (`ImportError: cannot import name 'read_and_enqueue'`).

- [ ] **Step 3: Implement** — add to `scripts/tg_ingest.py` (after `post_ingest`):

```python
from collections import Counter


async def read_and_enqueue(
    tg_client: Any,
    http: Any,
    *,
    channels: list[str],
    limit: int,
    api_base: str,
    api_key: str,
    queue: str | None,
) -> Counter:
    """Backfill: read last-`limit` messages per channel (oldest→newest) and enqueue."""
    tally: Counter = Counter()
    for channel in channels:
        async for msg in tg_client.iter_messages(channel, limit=limit, reverse=True):
            doc = _message_to_doc(msg, channel)
            if doc is None:
                tally["skipped"] += 1
                continue
            filename, text, document_date = doc
            ok = await post_ingest(http, api_base, api_key, filename, text, document_date, queue)
            tally["sent" if ok else "failed"] += 1
    logger.info("tg_ingest tally: {t}", t=dict(tally))
    return tally


def main() -> int:
    import argparse
    import asyncio
    import os

    p = argparse.ArgumentParser(description="Backfill TG channel messages into the ingest queue.")
    p.add_argument("--channels", required=True, help="comma-separated, e.g. @a,@b")
    p.add_argument("--limit", type=int, default=50, help="messages per channel")
    p.add_argument("--queue", default=None, help="target ingest queue (rabbitmq backend)")
    p.add_argument("--api-base", default="http://localhost:8000")
    p.add_argument("--api-key", default=os.environ.get("KB_API_KEY", "dev-local-key"))
    p.add_argument("--session", default=".tg_ingest.session")
    args = p.parse_args()

    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    channels = [c.strip() for c in args.channels.split(",") if c.strip()]

    async def _run() -> None:
        import httpx
        from telethon import TelegramClient

        async with (
            TelegramClient(args.session, api_id, api_hash) as tg,
            httpx.AsyncClient(timeout=30.0) as http,
        ):
            await read_and_enqueue(
                tg, http,
                channels=channels, limit=args.limit,
                api_base=args.api_base, api_key=args.api_key, queue=args.queue,
            )

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

(Move the `from collections import Counter` import to the top of the file with the other imports during ruff-format; it is shown here inline for clarity.)

- [ ] **Step 4: Run → PASS**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py -q`
Expected: PASS (5 tests). Import sanity (telethon NOT needed at import): `uv run python -c "import scripts.tg_ingest"`.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
uv run ruff format scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
git commit -m "feat(ingest): tg_ingest read_and_enqueue orchestration + CLI main"
```

---

## Self-Review

**1. Spec coverage**

| Spec item | Task |
|---|---|
| RabbitMQ in dev compose (profile) | 1 |
| telethon optional dep | 1 |
| `_message_to_doc` (filename/text/post-date, skip empty) | 2 |
| `post_ingest` multipart to `/api/v1/ingest`, `X-API-Key`, fail-soft | 2 |
| `read_and_enqueue` backfill + tally | 3 |
| `main` CLI + lazy Telethon + httpx | 3 |
| document_date = post date | 2 (`_message_to_doc`) |
| text-only skip | 2, 3 |
| unit tests, no network/telethon | 2, 3 |
| runbook | 2 (module docstring) |
| session gitignored | 1 |

**2. Placeholder scan:** none — all code is concrete; `_message_to_doc`/`post_ingest`/`read_and_enqueue`/`main` are given in full. The `INGEST_QUEUE_BACKEND` / `RABBITMQ_QUEUES` env names appear in the runbook as the operator's setup, not code.

**3. Type consistency:** `_message_to_doc(msg, channel) -> tuple|None` (Task 2) consumed by `read_and_enqueue` (Task 3); `post_ingest(http, api_base, api_key, filename, text, document_date, queue) -> bool` (Task 2) called with the same arg order in Task 3; `read_and_enqueue(...) -> Counter` keys `sent/skipped/failed` asserted in the Task-3 test.

**Out of scope:** continuous watch, media download, cross-run dedup, text-date parsing.

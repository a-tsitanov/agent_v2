# Manual channel reingest + low-priority ingest lane — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an operator command to reingest a folder-tracked Telegram channel's newest N messages through the normal pipeline at low priority, so the backfill drains only when the live feed is idle.

**Architecture:** Declare the RabbitMQ work queue with `x-max-priority`; live traffic publishes at `PRIO_LIVE=5`, manual reingest at `PRIO_BACKFILL=0`. RabbitMQ delivers a low-priority message to a free consumer slot only when no higher-priority message is ready — the native "main-first" behavior. A new `--reingest`/`--reingest-limit` CLI mode in `scripts/tg_ingest.py` resolves the channel within the configured folders, reads its newest N messages, and POSTs each at priority 0. Fresh `doc_id` per POST = genuine reprocess; the live sync cursor is never touched.

**Tech Stack:** Python 3.12, FastAPI (multipart `/ingest`), aio_pika (RabbitMQ, lazy-imported), Telethon (MTProto reader), Temporal, pytest + pytest-asyncio, ruff.

## Global Constraints

- Python 3.12; `from __future__ import annotations` at the top of every touched module (matches existing files).
- `aio_pika` is only available on the rabbitmq backend — never import it at module top in code reachable on the temporal backend. The priority constants live in a new **import-light** module `src/ingest_queue/priorities.py` (no aio_pika) so the API route and the tg script can import them unconditionally.
- Priority levels are the single source of truth: `PRIO_LIVE = 5`, `PRIO_BACKFILL = 0`. Never hardcode `5`/`0` elsewhere — import the constants.
- Follow existing test style: `pytest.mark.asyncio` for coroutines, `unittest.mock` `AsyncMock`/`MagicMock`, `SimpleNamespace`/small fake classes for Telethon/RabbitMQ doubles (see `tests/test_scripts/test_tg_ingest.py`, `tests/test_ingest_queue/test_topology.py`).
- Run the test suite with `uv run pytest`.
- Commit after every task.

---

### Task 1: Priority constants + queue `x-max-priority`

**Files:**
- Create: `src/ingest_queue/priorities.py`
- Modify: `src/config.py` (`RabbitMQSettings`, after `consumer_timeout_ms` at line 1075)
- Modify: `src/ingest_queue/topology.py` (`args` dict, lines 38-41)
- Modify: `.env.example` (near the `RABBITMQ_QUEUES` line)
- Test: `tests/test_ingest_queue/test_topology.py`

**Interfaces:**
- Produces: `src.ingest_queue.priorities.PRIO_LIVE` (int, 5), `PRIO_BACKFILL` (int, 0); `RabbitMQSettings.max_priority` (int, default 10); work queues declared with `x-max-priority == cfg.max_priority`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_ingest_queue/test_topology.py`:

```python
@pytest.mark.asyncio
async def test_declares_queue_with_max_priority() -> None:
    cfg = RabbitMQSettings(queues=["q1"], max_priority=7)

    channel = MagicMock()
    channel.declare_exchange = AsyncMock(return_value=MagicMock())
    dlq = MagicMock()
    dlq.bind = AsyncMock()
    q1 = MagicMock()
    channel.declare_queue = AsyncMock(side_effect=[dlq, q1])

    await declare_ingest_topology(channel, cfg)

    work_call = channel.declare_queue.call_args_list[1]  # skip the DLQ
    assert work_call.kwargs["arguments"]["x-max-priority"] == 7


def test_max_priority_default() -> None:
    assert RabbitMQSettings().max_priority == 10


def test_priority_constants() -> None:
    from src.ingest_queue.priorities import PRIO_BACKFILL, PRIO_LIVE

    assert PRIO_LIVE == 5
    assert PRIO_BACKFILL == 0
    assert PRIO_LIVE > PRIO_BACKFILL  # live must outrank backfill
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ingest_queue/test_topology.py -v`
Expected: FAIL — `test_priority_constants` errors on missing module `src.ingest_queue.priorities`; `test_declares_queue_with_max_priority` fails on missing `x-max-priority` key; `test_max_priority_default` fails on missing attribute.

- [ ] **Step 3: Create the priorities module**

Create `src/ingest_queue/priorities.py`:

```python
"""Ingest message priority levels (RabbitMQ backend).

A single work queue declared with ``x-max-priority`` carries two lanes:
live traffic at ``PRIO_LIVE`` and manual channel reingest at
``PRIO_BACKFILL``. RabbitMQ hands a lower-priority message to a free
consumer slot only when no higher-priority message is ready, so backfill
drains only when the live feed is idle.

Kept import-light (no aio_pika) so the API route and the tg_ingest script
can import these constants on any backend.
"""
from __future__ import annotations

PRIO_LIVE = 5
PRIO_BACKFILL = 0
```

- [ ] **Step 4: Add the `max_priority` setting**

In `src/config.py`, inside `RabbitMQSettings`, immediately after the `consumer_timeout_ms` field (currently ending at line 1075), add:

```python
    # Max message priority the work queues advertise (x-max-priority).
    # RabbitMQ delivers a lower-priority message to a free consumer slot only
    # when no higher-priority message is ready — this makes the manual reingest
    # lane (PRIO_BACKFILL=0) drain only when the live feed (PRIO_LIVE=5) has
    # nothing waiting. Immutable per queue: changing it needs a delete +
    # redeclare of the queue (see docs/runbook/reingest-and-priority.md).
    max_priority: int = Field(default=10, ge=1, le=255)
```

(`Field` is already imported and used in this file.)

- [ ] **Step 5: Add `x-max-priority` to the queue args**

In `src/ingest_queue/topology.py`, extend the `args` dict (lines 38-41) to:

```python
    args = {
        "x-dead-letter-exchange": cfg.dlx,
        "x-consumer-timeout": cfg.consumer_timeout_ms,
        "x-max-priority": cfg.max_priority,
    }
```

- [ ] **Step 6: Document the setting in `.env.example`**

In `.env.example`, next to the existing `RABBITMQ_QUEUES` line, add:

```bash
# Max RabbitMQ message priority for ingest work queues (x-max-priority).
# Live ingest publishes at 5; manual channel reingest publishes at 0, so it
# only drains when no live message is ready. Changing this requires deleting +
# redeclaring the queue (see docs/runbook/reingest-and-priority.md).
RABBITMQ_MAX_PRIORITY=10
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_ingest_queue/test_topology.py -v`
Expected: PASS (all, including the pre-existing topology tests).

- [ ] **Step 8: Commit**

```bash
git add src/ingest_queue/priorities.py src/config.py src/ingest_queue/topology.py .env.example tests/test_ingest_queue/test_topology.py
git commit -m "feat(ingest): priority constants + x-max-priority on ingest queues"
```

---

### Task 2: Publisher stamps message priority

**Files:**
- Modify: `src/ingest_queue/publisher.py` (`publish_ingest`, lines 39-67)
- Test: `tests/test_ingest_queue/test_publisher.py` (create)

**Interfaces:**
- Consumes: `PRIO_LIVE` from `src.ingest_queue.priorities`.
- Produces: `publish_ingest(params, queue=None, priority: int = PRIO_LIVE)` — publishes an `aio_pika.Message` whose `.priority == priority`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ingest_queue/test_publisher.py`:

```python
"""publish_ingest stamps the RabbitMQ message priority. No live broker: the
process-global connection + topology declare are monkeypatched, and we inspect
the aio_pika.Message handed to the default exchange."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ingest_queue import publisher
from src.ingest_queue.priorities import PRIO_LIVE
from src.workflow.contracts import IngestParams


def _params() -> IngestParams:
    return IngestParams(doc_id="doc-1", path="s3://bucket/doc-1/file.pdf")


def _fake_channel(captured: dict) -> MagicMock:
    async def _publish(message, routing_key):
        captured["msg"] = message
        captured["routing_key"] = routing_key

    channel = MagicMock()
    channel.default_exchange = MagicMock()
    channel.default_exchange.publish = _publish
    channel.close = AsyncMock()
    return channel


@pytest.mark.asyncio
async def test_publish_sets_backfill_priority(monkeypatch) -> None:
    captured: dict = {}
    channel = _fake_channel(captured)
    conn = MagicMock()
    conn.channel = AsyncMock(return_value=channel)
    monkeypatch.setattr(publisher, "_get_connection", AsyncMock(return_value=conn))
    monkeypatch.setattr(publisher, "declare_ingest_topology", AsyncMock())

    await publisher.publish_ingest(_params(), queue="ingest.pending", priority=0)

    assert captured["msg"].priority == 0
    assert captured["routing_key"] == "ingest.pending"


@pytest.mark.asyncio
async def test_publish_defaults_to_live_priority(monkeypatch) -> None:
    captured: dict = {}
    channel = _fake_channel(captured)
    conn = MagicMock()
    conn.channel = AsyncMock(return_value=channel)
    monkeypatch.setattr(publisher, "_get_connection", AsyncMock(return_value=conn))
    monkeypatch.setattr(publisher, "declare_ingest_topology", AsyncMock())

    await publisher.publish_ingest(_params())

    assert captured["msg"].priority == PRIO_LIVE
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ingest_queue/test_publisher.py -v`
Expected: FAIL — `publish_ingest` has no `priority` param and the Message carries no priority (`.priority` is `None`).

- [ ] **Step 3: Implement priority in the publisher**

In `src/ingest_queue/publisher.py`, add the import near the other `src` imports (after line 21):

```python
from src.ingest_queue.priorities import PRIO_LIVE
```

Change the `publish_ingest` signature (line 39) to:

```python
async def publish_ingest(
    params: IngestParams, queue: str | None = None, priority: int = PRIO_LIVE,
) -> None:
```

Add `priority=priority,` to the `aio_pika.Message(...)` constructor (after `message_id=params.doc_id,` at line 58):

```python
            aio_pika.Message(
                body=params.model_dump_json().encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=params.doc_id,
                priority=priority,
            ),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ingest_queue/test_publisher.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingest_queue/publisher.py tests/test_ingest_queue/test_publisher.py
git commit -m "feat(ingest): publish_ingest stamps message priority"
```

---

### Task 3: Thread priority through `submit_document`

**Files:**
- Modify: `src/workflow/ingest_submit.py` (`submit_document`, lines 30-44)
- Test: `tests/test_workflow/test_ingest_submit.py` (extend)

**Interfaces:**
- Consumes: `PRIO_LIVE`; `publish_ingest(params, queue, priority)`.
- Produces: `submit_document(client, params, queue=None, priority: int = PRIO_LIVE)` — forwards `priority` to `publish_ingest` on the rabbitmq path; temporal path ignores it.

- [ ] **Step 1: Update the failing test**

In `tests/test_workflow/test_ingest_submit.py`, add the import (after line 17):

```python
from src.ingest_queue.priorities import PRIO_LIVE
```

Replace the fake publisher and assertion in `test_rabbitmq_backend_publishes_and_skips_temporal` (lines 47-59) so it records priority:

```python
    published: list = []
    fake_mod = types.ModuleType("src.ingest_queue.publisher")

    async def _publish(p, queue=None, priority=None):
        published.append((p, queue, priority))

    fake_mod.publish_ingest = _publish
    monkeypatch.setitem(sys.modules, "src.ingest_queue.publisher", fake_mod)

    client = AsyncMock()
    params = _params()
    await submit_document(client, params, queue="ingest.bulk")

    assert published == [(params, "ingest.bulk", PRIO_LIVE)]  # default priority
    assert client.start_workflow.await_count == 0
```

Add a new test below it:

```python
@pytest.mark.asyncio
async def test_rabbitmq_backend_forwards_explicit_priority(monkeypatch) -> None:
    monkeypatch.setattr(settings.ingest_admission, "backend", "rabbitmq")
    published: list = []
    fake_mod = types.ModuleType("src.ingest_queue.publisher")

    async def _publish(p, queue=None, priority=None):
        published.append((queue, priority))

    fake_mod.publish_ingest = _publish
    monkeypatch.setitem(sys.modules, "src.ingest_queue.publisher", fake_mod)

    await submit_document(AsyncMock(), _params(), queue="ingest.pending", priority=0)

    assert published == [("ingest.pending", 0)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_workflow/test_ingest_submit.py -v`
Expected: FAIL — `submit_document` has no `priority` param; the fake `_publish` is called with 2 positional args and the recorded tuple lacks priority.

- [ ] **Step 3: Implement priority threading**

In `src/workflow/ingest_submit.py`, add the import (after line 27):

```python
from src.ingest_queue.priorities import PRIO_LIVE
```

Change `submit_document` (lines 30-44) to:

```python
async def submit_document(
    client: Client, params: IngestParams, queue: str | None = None,
    priority: int = PRIO_LIVE,
) -> None:
    """Hand one document to the configured ingest backlog backend.

    ``queue`` / ``priority`` apply to the rabbitmq backend only — the caller
    has validated ``queue`` ∈ RabbitMQSettings.queues and ``priority`` within
    ``0..max_priority``; both are ignored on temporal."""
    if settings.ingest_admission.backend == "rabbitmq":
        # Lazy import: aio_pika is only required when this backend is
        # actually selected (default is temporal).
        from src.ingest_queue.publisher import publish_ingest

        await publish_ingest(params, queue, priority)
        return
    await _submit_to_scheduler(client, params)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_workflow/test_ingest_submit.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/ingest_submit.py tests/test_workflow/test_ingest_submit.py
git commit -m "feat(ingest): thread priority through submit_document"
```

---

### Task 4: `/ingest` priority form field + validation

**Files:**
- Modify: `src/api/routes/ingest.py` (`upload_document`: signature ~line 60-69, validation ~after line 90, call at line 193)
- Test: `tests/test_api/test_ingest_priority.py` (create)

**Interfaces:**
- Consumes: `PRIO_LIVE`; `submit_document(..., priority=...)`.
- Produces: `POST /ingest` accepts `priority: int | None` form field; `None` → `PRIO_LIVE`; out-of-range (`< 0` or `> max_priority`) → 422 on the rabbitmq backend; value forwarded to `submit_document`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_api/test_ingest_priority.py`:

```python
"""ASGI tests for the /ingest `priority` form field: out-of-range 422 (rabbitmq
backend), and the resolved value forwarded to submit_document. Mirrors the
stub-free validation path in test_ingest_group.py, plus a fully-stubbed forward
path."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


@pytest.mark.asyncio
async def test_ingest_priority_out_of_range_422(monkeypatch) -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    monkeypatch.setattr(settings.ingest_admission, "backend", "rabbitmq")
    stub_storage = MagicMock()

    with (
        patch("src.api.routes.ingest.build_minio_storage", return_value=stub_storage),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("t.txt", b"hi", "text/plain")},
                data={"priority": "99"},  # > max_priority (10)
            )

    assert resp.status_code == 422, resp.text
    assert "priority must be" in resp.text
    stub_storage.put_object.assert_not_called()
    ins.assert_not_awaited()


@pytest.mark.asyncio
async def test_ingest_forwards_priority(monkeypatch) -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    monkeypatch.setattr(settings.ingest_admission, "backend", "rabbitmq")
    stub_storage = MagicMock()
    stub_storage.put_object.return_value = "s3://bucket/x/t.txt"

    submit = AsyncMock()
    with (
        patch("src.api.routes.ingest.build_minio_storage", return_value=stub_storage),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()),
        patch("src.api.routes.ingest.get_temporal_client", new=AsyncMock(return_value=MagicMock())),
        patch("src.api.routes.ingest.submit_document", new=submit),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("t.txt", b"hi", "text/plain")},
                data={"priority": "0"},
            )

    assert resp.status_code == 202, resp.text
    assert submit.await_args.kwargs["priority"] == 0


@pytest.mark.asyncio
async def test_ingest_defaults_to_live_priority(monkeypatch) -> None:
    from src.api.main import app
    from src.ingest_queue.priorities import PRIO_LIVE
    from src.storage.postgres import AsyncPostgres

    monkeypatch.setattr(settings.ingest_admission, "backend", "rabbitmq")
    stub_storage = MagicMock()
    stub_storage.put_object.return_value = "s3://bucket/x/t.txt"

    submit = AsyncMock()
    with (
        patch("src.api.routes.ingest.build_minio_storage", return_value=stub_storage),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()),
        patch("src.api.routes.ingest.get_temporal_client", new=AsyncMock(return_value=MagicMock())),
        patch("src.api.routes.ingest.submit_document", new=submit),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("t.txt", b"hi", "text/plain")},
            )

    assert resp.status_code == 202, resp.text
    assert submit.await_args.kwargs["priority"] == PRIO_LIVE
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api/test_ingest_priority.py -v`
Expected: FAIL — no `priority` field is accepted; `submit_document` is called without a `priority` kwarg, and the out-of-range value is not rejected.

- [ ] **Step 3: Add the import**

In `src/api/routes/ingest.py`, after line 28 (`from src.retrieval.groups import GROUP_SET`), add:

```python
from src.ingest_queue.priorities import PRIO_LIVE
```

- [ ] **Step 4: Add the form parameter**

In the `upload_document` signature, add after `group: str = Form(default="")` (line 67):

```python
    priority: int | None = Form(default=None),
```

- [ ] **Step 5: Add validation + resolution**

In `src/api/routes/ingest.py`, after the group-validation block (after line 90, before the document-date block), add:

```python
    # Optional ingest priority (rabbitmq backend only). Higher = served first;
    # manual channel reingest posts PRIO_BACKFILL (0). None → live default.
    # Validate against the queue's advertised max so an out-of-range value 422s
    # before any upload/enqueue work. Ignored on the temporal backend.
    resolved_priority = PRIO_LIVE if priority is None else priority
    if settings.ingest_admission.backend == "rabbitmq" and not (
        0 <= resolved_priority <= settings.rabbitmq.max_priority
    ):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"priority must be 0..{settings.rabbitmq.max_priority}",
        )
```

- [ ] **Step 6: Forward the priority to `submit_document`**

Change the call at line 193 from:

```python
        await submit_document(client, params, queue=target_queue)
```

to:

```python
        await submit_document(client, params, queue=target_queue, priority=resolved_priority)
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_api/test_ingest_priority.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/api/routes/ingest.py tests/test_api/test_ingest_priority.py
git commit -m "feat(api): /ingest priority form field + validation"
```

---

### Task 5: `post_ingest` priority parameter

**Files:**
- Modify: `scripts/tg_ingest.py` (`post_ingest`, lines 68-94)
- Test: `tests/test_scripts/test_tg_ingest.py` (extend)

**Interfaces:**
- Produces: `post_ingest(http, api_base, api_key, filename, text, document_date, queue, group="", priority: int | None = None)` — includes `data["priority"] = str(priority)` in the POST only when `priority is not None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scripts/test_tg_ingest.py`:

```python
@pytest.mark.asyncio
async def test_post_ingest_includes_priority_when_set():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(data=data)
            return _Resp()

    ok = await post_ingest(
        _Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1",
        group="news", priority=0,
    )
    assert ok is True
    assert sent["data"]["priority"] == "0"
    assert sent["data"]["group"] == "news"


@pytest.mark.asyncio
async def test_post_ingest_omits_priority_when_none():
    sent = {}

    class _Resp:
        status_code = 202

    class _Client:
        async def post(self, url, headers=None, files=None, data=None):
            sent.update(data=data)
            return _Resp()

    await post_ingest(_Client(), "http://api", "k", "f.txt", "hi", "2024-03-01", "q1")
    assert "priority" not in sent["data"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py -k priority -v`
Expected: FAIL — `post_ingest` has no `priority` param (`TypeError`).

- [ ] **Step 3: Implement the priority field**

In `scripts/tg_ingest.py`, change the `post_ingest` signature (lines 68-77) to add `priority`:

```python
async def post_ingest(
    http: Any,
    api_base: str,
    api_key: str,
    filename: str,
    text: str,
    document_date: str,
    queue: str | None,
    group: str = "",
    priority: int | None = None,
) -> bool:
```

In its body, after the `group` block (after line 83), add:

```python
    if priority is not None:
        data["priority"] = str(priority)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_scripts/test_tg_ingest.py -v`
Expected: PASS (new priority tests + all pre-existing tg_ingest tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest.py
git commit -m "feat(tg_ingest): post_ingest optional priority field"
```

---

### Task 6: `reingest_channels` core

**Files:**
- Modify: `scripts/tg_ingest.py` (add import at line 51-area; add `reingest_channels` after `read_and_enqueue`, i.e. after line 119)
- Test: `tests/test_scripts/test_tg_ingest_reingest.py` (create)

**Interfaces:**
- Consumes: `PRIO_BACKFILL`; `dialog_slug`, `dialog_in_folders`, `_message_to_doc`, `post_ingest`.
- Produces:
  ```python
  async def reingest_channels(
      tg_client, http, *, dialogs: list[Any], channels: list[str],
      spec: dict | None, group_map: dict[int, str], limit: int,
      api_base: str, api_key: str, queue: str | None, priority: int,
  ) -> tuple[Counter, list[str]]  # (tally, errors)
  ```
  Matches each `channels` token to a dialog by `@username`/casefold or numeric id; if `spec` is not None the dialog must pass `dialog_in_folders` (else an error entry, no posts for it); posts the newest `limit` messages via `post_ingest(..., group=<folder group>, priority=priority)`. Non-empty `errors` → caller exits non-zero.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_scripts/test_tg_ingest_reingest.py`:

```python
from datetime import UTC, datetime

import pytest

from scripts.tg_ingest import reingest_channels


class _FakeMsg:
    def __init__(self, id, message, date):
        self.id = id
        self.message = message
        self.date = date


class _FakeEntity:
    def __init__(self, key, username=None):
        self._key = key
        self.username = username


class _FakeDialog:
    def __init__(self, id, key, *, username=None, is_channel=True, is_group=False):
        self.id = id
        self.entity = _FakeEntity(key, username)
        self.username = username
        self.is_channel = is_channel
        self.is_group = is_group
        self.title = username or str(id)


class _TG:
    """iter_messages(entity, limit, reverse) → prepared newest-`limit` msgs."""

    def __init__(self, per_key):
        self.per_key = per_key  # entity._key -> list[_FakeMsg]

    def iter_messages(self, entity, limit=None, reverse=False):
        msgs = sorted(self.per_key.get(entity._key, []), key=lambda m: -m.id)[:limit]
        ordered = sorted(msgs, key=lambda m: m.id) if reverse else msgs

        async def _gen():
            for m in ordered:
                yield m

        return _gen()


class _RecHTTP:
    def __init__(self):
        self.posted = []

    async def post(self, url, headers=None, files=None, data=None):
        self.posted.append((files["file"][0], dict(data)))

        class _R:
            status_code = 202

        return _R()


@pytest.mark.asyncio
async def test_reingest_success_posts_priority_and_group():
    dialog = _FakeDialog(-100111, "a", username="chan_a")
    tg = _TG({"a": [
        _FakeMsg(1, "alpha", datetime(2024, 1, 1, tzinfo=UTC)),
        _FakeMsg(2, "beta", datetime(2024, 1, 2, tzinfo=UTC)),
    ]})
    http = _RecHTTP()

    tally, errors = await reingest_channels(
        tg, http, dialogs=[dialog], channels=["@chan_a"],
        spec={"include_ids": {-100111}, "exclude_ids": set(),
              "groups": False, "broadcasts": False},
        group_map={-100111: "news"}, limit=50,
        api_base="http://a", api_key="k", queue="ingest.pending", priority=0,
    )

    assert errors == []
    assert tally["sent"] == 2
    assert [f for f, _ in http.posted] == ["tg_chan_a_1.txt", "tg_chan_a_2.txt"]
    assert all(d["priority"] == "0" and d["group"] == "news" for _, d in http.posted)


@pytest.mark.asyncio
async def test_reingest_channel_not_found_errors_no_posts():
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        _TG({}), http, dialogs=[], channels=["@nope"], spec=None,
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert http.posted == []
    assert len(errors) == 1 and "not found" in errors[0]


@pytest.mark.asyncio
async def test_reingest_channel_not_in_folder_errors_no_posts():
    dialog = _FakeDialog(-100222, "b", username="chan_b")
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        _TG({"b": [_FakeMsg(1, "x", datetime(2024, 1, 1, tzinfo=UTC))]}),
        http, dialogs=[dialog], channels=["@chan_b"],
        spec={"include_ids": set(), "exclude_ids": set(),
              "groups": False, "broadcasts": False},  # not a member
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert http.posted == []
    assert len(errors) == 1 and "not in" in errors[0]


@pytest.mark.asyncio
async def test_reingest_matches_by_numeric_id_when_no_spec():
    dialog = _FakeDialog(-100333, "c", username=None)
    tg = _TG({"c": [_FakeMsg(7, "hi", datetime(2024, 1, 1, tzinfo=UTC))]})
    http = _RecHTTP()
    tally, errors = await reingest_channels(
        tg, http, dialogs=[dialog], channels=["-100333"], spec=None,
        group_map={}, limit=10, api_base="http://a", api_key="k",
        queue=None, priority=0,
    )
    assert errors == []
    assert [f for f, _ in http.posted] == ["tg_-100333_7.txt"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_reingest.py -v`
Expected: FAIL — `ImportError: cannot import name 'reingest_channels'`.

- [ ] **Step 3: Add the `PRIO_BACKFILL` import**

In `scripts/tg_ingest.py`, after line 51 (`from src.retrieval.groups import GROUP_SET, pick_priority`), add:

```python
from src.ingest_queue.priorities import PRIO_BACKFILL
```

- [ ] **Step 4: Implement `reingest_channels`**

In `scripts/tg_ingest.py`, add after `read_and_enqueue` (after line 119):

```python
async def reingest_channels(
    tg_client: Any,
    http: Any,
    *,
    dialogs: list[Any],
    channels: list[str],
    spec: dict | None,
    group_map: dict[int, str],
    limit: int,
    api_base: str,
    api_key: str,
    queue: str | None,
    priority: int,
) -> tuple[Counter, list[str]]:
    """Reingest the newest ``limit`` messages of each requested channel at
    ``priority``. A channel token is matched to an account dialog by
    ``@username`` (case-insensitive) or numeric id; when ``spec`` is given the
    dialog must also be in those folders (else an error, and none of its
    messages are posted). Each posted doc is tagged with the channel's folder
    group. The live sync cursor (``--state``) is never read or written here.
    Returns ``(tally, errors)``; a non-empty ``errors`` means the caller should
    exit non-zero."""
    by_slug = {dialog_slug(d).lstrip("@").casefold(): d for d in dialogs}
    by_id = {str(d.id): d for d in dialogs}
    tally: Counter = Counter()
    errors: list[str] = []
    for token in channels:
        dialog = by_slug.get(token.lstrip("@").casefold()) or by_id.get(token)
        if dialog is None:
            errors.append(f"{token}: not found among account dialogs")
            continue
        if spec is not None and not dialog_in_folders(dialog, spec):
            errors.append(f"{token}: not in the configured folders")
            continue
        group = group_map.get(dialog.id, "")
        slug = dialog_slug(dialog)
        async for msg in tg_client.iter_messages(dialog.entity, limit=limit, reverse=True):
            doc = _message_to_doc(msg, slug)
            if doc is None:
                tally["skipped"] += 1
                continue
            filename, text, document_date = doc
            ok = await post_ingest(
                http, api_base, api_key, filename, text, document_date, queue,
                group=group, priority=priority,
            )
            tally["sent" if ok else "failed"] += 1
    logger.info("tg_ingest reingest tally: {t}", t=dict(tally))
    return tally, errors
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_reingest.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest_reingest.py
git commit -m "feat(tg_ingest): reingest_channels core (folder-scoped, low-priority)"
```

---

### Task 7: CLI flags, mode dispatch, `_run_reingest` wrapper

**Files:**
- Modify: `scripts/tg_ingest.py` (`main`: argparse block lines 343-361; add `select_mode` helper; add `_run_reingest`; change dispatch at line 442)
- Test: `tests/test_scripts/test_tg_ingest_reingest.py` (extend with `select_mode` tests)

**Interfaces:**
- Consumes: `reingest_channels`, `PRIO_BACKFILL`, `select_dialogs`, `resolve_folders`, `resolve_group_map`.
- Produces: module-level `select_mode(args) -> str` returning `"reingest" | "backfill" | "sync"`; CLI flags `--reingest`, `--reingest-limit`; `_run_reingest()` coroutine returning an int exit code (2 on validation errors, else 0).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scripts/test_tg_ingest_reingest.py`:

```python
from types import SimpleNamespace

from scripts.tg_ingest import select_mode


def test_select_mode_prefers_reingest():
    args = SimpleNamespace(reingest="@a", channels="@b")
    assert select_mode(args) == "reingest"


def test_select_mode_backfill_when_channels_only():
    assert select_mode(SimpleNamespace(reingest=None, channels="@b")) == "backfill"


def test_select_mode_sync_by_default():
    assert select_mode(SimpleNamespace(reingest=None, channels=None)) == "sync"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_reingest.py -k select_mode -v`
Expected: FAIL — `ImportError: cannot import name 'select_mode'`.

- [ ] **Step 3: Add the `select_mode` helper**

In `scripts/tg_ingest.py`, add a module-level function just above `def main()` (before line 334):

```python
def select_mode(args: Any) -> str:
    """Choose the run mode from parsed args: reingest wins over the legacy
    backfill, which wins over the default continuous sync."""
    if getattr(args, "reingest", None):
        return "reingest"
    if getattr(args, "channels", None):
        return "backfill"
    return "sync"
```

- [ ] **Step 4: Add the CLI flags**

In `main()`, after the `--folders` argument (line 357-361), add:

```python
    p.add_argument(
        "--reingest", default=None,
        help="reingest mode: comma-separated @channel/id to re-read (must be in "
        "a --folders folder); posts newest --reingest-limit msgs at low priority",
    )
    p.add_argument(
        "--reingest-limit", type=int, default=100,
        help="reingest: newest N messages per channel (default 100)",
    )
```

- [ ] **Step 5: Add the `_run_reingest` wrapper**

In `main()`, add this coroutine next to `_run_backfill` / `_run_sync` (e.g. after `_run_sync` ends at line 439, before the `try:` at line 441):

```python
    async def _run_reingest() -> int:
        import httpx
        from telethon import TelegramClient
        from telethon import utils as tg_utils
        from telethon.tl import functions

        channels = [c.strip() for c in args.reingest.split(",") if c.strip()]
        folder_names = [n for n in (args.folders or "").split(",") if n.strip()]
        async with (
            TelegramClient(args.session, api_id, api_hash) as tg,
            httpx.AsyncClient(timeout=30.0) as http,
        ):
            dialogs = select_dialogs([d async for d in tg.iter_dialogs()])
            res = await tg(functions.messages.GetDialogFiltersRequest())
            filters = getattr(res, "filters", res)
            group_map = resolve_group_map(filters, peer_id=tg_utils.get_peer_id)
            spec = None
            if folder_names:
                spec, missing = resolve_folders(
                    filters, folder_names, peer_id=tg_utils.get_peer_id,
                )
                if missing:
                    logger.warning(
                        "tg_ingest reingest: folders not found: {m}", m=missing,
                    )
            _tally, errors = await reingest_channels(
                tg, http,
                dialogs=dialogs, channels=channels, spec=spec,
                group_map=group_map, limit=args.reingest_limit,
                api_base=args.api_base, api_key=args.api_key,
                queue=args.queue, priority=PRIO_BACKFILL,
            )
            for e in errors:
                logger.error("tg_ingest reingest: {e}", e=e)
            return 2 if errors else 0
```

- [ ] **Step 6: Wire the dispatch**

Replace the dispatch block (lines 441-445) with:

```python
    mode = select_mode(args)
    try:
        if mode == "reingest":
            return asyncio.run(_run_reingest())
        asyncio.run(_run_backfill() if mode == "backfill" else _run_sync())
    except KeyboardInterrupt:
        logger.info("tg_ingest: stopped by user (state is saved per-message)")
    return 0
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_reingest.py -v`
Expected: PASS.

- [ ] **Step 8: Full suite + lint**

Run: `uv run pytest tests/test_scripts tests/test_ingest_queue tests/test_workflow/test_ingest_submit.py tests/test_api/test_ingest_priority.py -v && uv run ruff check scripts/tg_ingest.py src/ingest_queue src/workflow/ingest_submit.py src/api/routes/ingest.py src/config.py`
Expected: all PASS; ruff clean.

- [ ] **Step 9: Commit**

```bash
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest_reingest.py
git commit -m "feat(tg_ingest): --reingest CLI mode + mode dispatch"
```

---

### Task 8: Runbook — queue migration + reingest usage

**Files:**
- Create: `docs/runbook/reingest-and-priority.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Write the runbook**

Create `docs/runbook/reingest-and-priority.md`:

```markdown
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

Set `RABBITMQ_MAX_PRIORITY` (default 10) in `.env` if you want a different
ceiling. Restart the API + consumer so `declare_ingest_topology` recreates the
queue with the priority arg. The DLQ needs no change.

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
```

- [ ] **Step 2: Commit**

```bash
git add docs/runbook/reingest-and-priority.md
git commit -m "docs(runbook): channel reingest + queue priority migration"
```

---

## Self-Review

**Spec coverage:**
- Priority lane (x-max-priority, PRIO_LIVE/PRIO_BACKFILL, live=5/reingest=0) → Tasks 1-4. ✓
- `None` → live default at the API → Task 4 (`resolved_priority`). ✓
- Migration (delete+redeclare `ingest.pending`) → Task 1 (setting) + Task 8 (runbook). ✓
- Reingest CLI mode, folder-scoped, refuses non-folder channels, stamps group → Tasks 6-7. ✓
- Newest-N read, cursor untouched, reprocess via fresh doc_id → Tasks 6-7 (no `--state` access). ✓
- Invocation via one-off container run → Task 8. ✓
- Tests: topology / publisher / route / tg_ingest reingest → Tasks 1,2,4,6,7. ✓
- `.env.example` for `RABBITMQ_MAX_PRIORITY` → Task 1. ✓
- Out of scope (capacity reservation, temporal priority path, legacy `--channels`) → untouched. ✓

**Placeholder scan:** none — every code/test step carries full content.

**Type consistency:** `priority: int` / `PRIO_LIVE` / `PRIO_BACKFILL` used identically across publisher, submit_document, route, post_ingest, reingest_channels. `reingest_channels` returns `(Counter, list[str])` everywhere it's referenced (Task 6 def, Task 7 caller). `select_mode` returns the same three literals in def (Task 7) and tests.
```

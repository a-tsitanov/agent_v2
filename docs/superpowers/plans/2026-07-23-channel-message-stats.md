# Channel Message Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose per-channel / per-group × status counts and a daily message time series over ingested Telegram documents, via an HTTP endpoint and a CLI.

**Architecture:** Add `source_channel` / `source_group` columns to the Postgres `documents` table, populated at insert time from values the ingest caller already supplies (tg_ingest → `/ingest` Form fields → `insert_pending`). Two new `AsyncPostgres` aggregation methods hold the only statistics SQL; both a FastAPI router (`src/api/routes/stats.py`) and a CLI (`scripts/message_stats.py`) call them. A one-shot backfill recovers `source_channel` for historical rows by parsing `path`.

**Tech Stack:** Python 3.12, FastAPI, dishka DI, psycopg3 (async, pooled), Pydantic v2, pytest / pytest-asyncio, httpx `ASGITransport`.

## Global Constraints

- Channel/group are stored as `TEXT NOT NULL DEFAULT ''`; non-Telegram documents keep `''`. Every statistics query excludes `''` keys.
- Canonical groups (`src/retrieval/groups.py`): `news, analytics, digest, opinion, official, data`. The `source_group` column stores one of these or `''`.
- `documents.status` domain after this work: `pending, processing, completed, vector_only, failed, skipped`.
- Dynamic SQL identifiers (`dimension`, `date_field`, `group_by` key column) are chosen ONLY from hardcoded allowlists before f-string interpolation; all user values bind as `%s` parameters. Never interpolate a raw request value into SQL.
- Postgres integration tests skip when the DB is unreachable, following `tests/test_storage/test_ingest_metrics.py` (`_pg_reachable()` + `pytest.mark.skipif`).
- Route auth: `X-API-Key` via `require_api_key` (`src/api/auth.py`).
- Postgres DSN: `settings.postgres.dsn`. `AsyncPostgres()` uses the shared pool; `AsyncPostgres(dsn)` uses a direct connect-per-call (used by scripts/tests).

## File Structure

- `scripts/setup_db.py` — MODIFY: columns, indexes, CHECK amend, backfill constant.
- `src/storage/postgres.py` — MODIFY: `insert_pending` gains channel/group params; add `status_counts_by`, `timeline_counts`.
- `src/api/routes/ingest.py` — MODIFY: `channel` Form field → `insert_pending`.
- `src/api/routes/stats.py` — CREATE: stats router + response models.
- `src/api/main.py` — MODIFY: register stats router.
- `scripts/tg_ingest.py` — MODIFY: `post_ingest` sends `channel`; 3 call sites pass it.
- `scripts/message_stats.py` — CREATE: CLI.
- Tests under `tests/test_storage/`, `tests/test_api/`, `tests/test_scripts/`.

---

## Task 1: Schema — columns, indexes, CHECK amend, backfill

**Files:**
- Modify: `scripts/setup_db.py:33-61` (`_DOCUMENTS_DDL`), `scripts/setup_db.py:200-211` (`setup_postgres`)
- Test: `tests/test_scripts/test_setup_db_source_columns.py` (create)

**Interfaces:**
- Produces: module constant `scripts.setup_db._BACKFILL_SOURCE_CHANNEL_SQL: str`; `documents` gains columns `source_channel TEXT NOT NULL DEFAULT ''`, `source_group TEXT NOT NULL DEFAULT ''`; status CHECK includes `'skipped'`.

- [ ] **Step 1: Edit `_DOCUMENTS_DDL`**

Replace the current `_DOCUMENTS_DDL` string (`scripts/setup_db.py:33-61`) with:

```python
_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS documents (
    id           UUID PRIMARY KEY,
    path         TEXT NOT NULL,
    department   TEXT DEFAULT '',
    doc_type     TEXT DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'completed',
                                   'vector_only', 'failed', 'skipped')),
    error        TEXT DEFAULT '',
    summary      TEXT DEFAULT '',
    doc_date     DATE,
    source_channel TEXT NOT NULL DEFAULT '',
    source_group   TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent adds for pre-existing deployments (CREATE TABLE IF NOT EXISTS
-- does not add a new column to an already-created table).
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date DATE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_channel TEXT NOT NULL DEFAULT '';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_group   TEXT NOT NULL DEFAULT '';

-- Widen the status domain to include 'skipped' (classifier skip, written by
-- finalize.mark_skipped) on already-created tables. The inline CHECK above is
-- auto-named documents_status_check; drop + re-add makes this idempotent.
ALTER TABLE documents DROP CONSTRAINT IF EXISTS documents_status_check;
ALTER TABLE documents ADD CONSTRAINT documents_status_check
    CHECK (status IN ('pending', 'processing', 'completed',
                      'vector_only', 'failed', 'skipped'));

CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status);

CREATE INDEX IF NOT EXISTS documents_department_idx
    ON documents (department);

CREATE INDEX IF NOT EXISTS documents_doc_date_idx
    ON documents (doc_date);

CREATE INDEX IF NOT EXISTS documents_source_channel_idx
    ON documents (source_channel);

CREATE INDEX IF NOT EXISTS documents_source_group_idx
    ON documents (source_group);
"""


# Backfill source_channel for historical Telegram rows from the filename
# embedded in `path` ({doc_id}/tg_<channel>_<msgid>.txt). Greedy .+ so
# tg_a_b_123.txt -> channel 'a_b', msgid '123'. Only touches still-empty TG
# rows, so re-running setup is cheap and idempotent. source_group cannot be
# recovered (never recorded historically) and stays ''.
_BACKFILL_SOURCE_CHANNEL_SQL = r"""
UPDATE documents
   SET source_channel = substring(path from 'tg_(.+)_[0-9]+\.txt$')
 WHERE source_channel = ''
   AND path ~ 'tg_.+_[0-9]+\.txt$';
"""
```

- [ ] **Step 2: Run the backfill in `setup_postgres`**

In `setup_postgres` (`scripts/setup_db.py:206-210`), add the backfill execute after the two DDL executes:

```python
    with psycopg.connect(
        pg.dsn, connect_timeout=pg.connect_timeout_s, autocommit=True,
    ) as conn, conn.cursor() as cur:
        cur.execute(_DOCUMENTS_DDL)
        cur.execute(_INGEST_METRICS_DDL)
        cur.execute(_BACKFILL_SOURCE_CHANNEL_SQL)
```

- [ ] **Step 3: Write the failing test**

Create `tests/test_scripts/test_setup_db_source_columns.py`:

```python
"""Integration test for the documents source_channel/source_group columns,
the 'skipped' status CHECK, and the source_channel backfill. Skipped when
local Postgres is unreachable (mirrors test_storage/test_ingest_metrics.py).
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from src.config import settings
from scripts.setup_db import _BACKFILL_SOURCE_CHANNEL_SQL, setup_postgres


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="local Postgres unreachable"
)


def test_source_columns_backfill_and_skipped_status() -> None:
    setup_postgres()  # idempotent: creates/alters columns + constraint
    doc_id = uuid.uuid4()
    path = f"{doc_id}/tg_somechannel_4567.txt"
    try:
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                # 'skipped' must be an accepted status now.
                cur.execute(
                    "INSERT INTO documents (id, path, status, source_channel) "
                    "VALUES (%s, %s, 'skipped', '')",
                    (str(doc_id), path),
                )
                cur.execute(_BACKFILL_SOURCE_CHANNEL_SQL)
                cur.execute(
                    "SELECT source_channel, source_group FROM documents WHERE id = %s",
                    (str(doc_id),),
                )
                channel, group = cur.fetchone()
        assert channel == "somechannel"
        assert group == ""
    finally:
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents WHERE id = %s", (str(doc_id),))
```

- [ ] **Step 4: Run the test**

Run: `uv run pytest tests/test_scripts/test_setup_db_source_columns.py -v`
Expected: PASS if Postgres is up; SKIPPED otherwise. (If it fails on import of `_BACKFILL_SOURCE_CHANNEL_SQL`, Step 1 was not applied.)

- [ ] **Step 5: Commit**

```bash
git add scripts/setup_db.py tests/test_scripts/test_setup_db_source_columns.py
git commit -m "feat(db): source_channel/source_group columns + skipped-status CHECK + backfill"
```

---

## Task 2: Persist channel/group in `insert_pending`

**Files:**
- Modify: `src/storage/postgres.py:73-88` (`insert_pending`)
- Test: `tests/test_storage/test_insert_pending_source.py` (create)

**Interfaces:**
- Consumes: `documents.source_channel`, `documents.source_group` (Task 1).
- Produces: `AsyncPostgres.insert_pending(doc_id, path, department='', doc_type='', doc_date=None, source_channel='', source_group='')` writes both new columns.

- [ ] **Step 1: Write the failing test**

Create `tests/test_storage/test_insert_pending_source.py`:

```python
"""insert_pending persists source_channel/source_group. Integration —
skipped when Postgres is unreachable."""

from __future__ import annotations

import asyncio
import uuid

import psycopg
import pytest

from src.config import settings
from src.storage.postgres import AsyncPostgres


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="local Postgres unreachable"
)


def test_insert_pending_writes_source_columns() -> None:
    doc_id = uuid.uuid4()
    pg = AsyncPostgres(settings.postgres.dsn)

    async def go():
        await pg.insert_pending(
            doc_id, f"{doc_id}/tg_chan_1.txt",
            source_channel="chan", source_group="news",
        )

    try:
        asyncio.run(go())
        with psycopg.connect(settings.postgres.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_channel, source_group FROM documents WHERE id = %s",
                (str(doc_id),),
            )
            assert cur.fetchone() == ("chan", "news")
    finally:
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents WHERE id = %s", (str(doc_id),))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_storage/test_insert_pending_source.py -v`
Expected: FAIL — `insert_pending() got an unexpected keyword argument 'source_channel'` (or SKIPPED if PG down; bring PG up to drive this task).

- [ ] **Step 3: Update `insert_pending`**

Replace `insert_pending` (`src/storage/postgres.py:73-88`) with:

```python
    async def insert_pending(
        self, doc_id: uuid.UUID, path: str,
        department: str = "", doc_type: str = "",
        doc_date: str | None = None,
        source_channel: str = "", source_group: str = "",
    ) -> None:
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO documents
                        (id, path, department, doc_type, doc_date,
                         source_channel, source_group, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                    """,
                    (str(doc_id), path, department, doc_type, doc_date,
                     source_channel, source_group),
                )
            await conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_storage/test_insert_pending_source.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/storage/postgres.py tests/test_storage/test_insert_pending_source.py
git commit -m "feat(storage): insert_pending persists source_channel/source_group"
```

---

## Task 3: `/ingest` accepts a `channel` Form field

**Files:**
- Modify: `src/api/routes/ingest.py:60-71` (handler signature), `:159-162` (`insert_pending` call)
- Test: `tests/test_api/test_ingest_source_channel.py` (create)

**Interfaces:**
- Consumes: `AsyncPostgres.insert_pending(..., source_channel=, source_group=)` (Task 2); `group` Form field already validated against `GROUP_SET` (`ingest.py:88-92`).
- Produces: `POST /api/v1/ingest` accepts `channel` (multipart form, default `""`) and forwards `source_channel=channel`, `source_group=group` to `insert_pending`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_api/test_ingest_source_channel.py`:

```python
"""POST /ingest forwards the channel + group form fields into insert_pending
as source_channel / source_group. Stubs MinIO + Temporal so no infra runs."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


@pytest.mark.asyncio
async def test_ingest_forwards_channel_and_group() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = MagicMock()
    stub_storage.put_object.return_value = "s3://bucket/key"

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch("src.api.routes.ingest.get_temporal_client", new=AsyncMock()),
        patch("src.api.routes.ingest.submit_document", new=AsyncMock()),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("tg_acme_9.txt", b"hi", "text/plain")},
                data={"group": "news", "channel": "acme"},
            )

    assert resp.status_code == 202, resp.text
    _, kwargs = ins.call_args
    assert kwargs["source_channel"] == "acme"
    assert kwargs["source_group"] == "news"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_api/test_ingest_source_channel.py -v`
Expected: FAIL — `KeyError: 'source_channel'` (handler does not pass the kwarg yet).

- [ ] **Step 3: Add the `channel` param**

In `upload_document` add the Form param after `group` (`src/api/routes/ingest.py:68`):

```python
    group: str = Form(default=""),
    channel: str = Form(default=""),
```

- [ ] **Step 4: Forward it to `insert_pending`**

Replace the `insert_pending` call (`src/api/routes/ingest.py:159-162`) with:

```python
    await pg.insert_pending(
        doc_id, s3_uri, department=department, doc_type=doc_type,
        doc_date=document_date or None,
        source_channel=channel, source_group=group,
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_api/test_ingest_source_channel.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/api/routes/ingest.py tests/test_api/test_ingest_source_channel.py
git commit -m "feat(ingest): accept channel form field, persist source_channel/source_group"
```

---

## Task 4: tg_ingest sends the channel slug

**Files:**
- Modify: `scripts/tg_ingest.py:74-103` (`post_ingest`), `:124` (`read_and_enqueue` call), `:172-175` (`reingest_channels` call), `:373-375` (`sync_round` call)
- Test: `tests/test_scripts/test_tg_ingest_channel_field.py` (create)

**Interfaces:**
- Consumes: `POST /api/v1/ingest` `channel` field (Task 3).
- Produces: `post_ingest(..., channel: str = "")` adds `data["channel"]` when non-empty; all three call sites pass the channel slug (`.lstrip("@")`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_scripts/test_tg_ingest_channel_field.py`:

```python
"""post_ingest puts the channel slug into the multipart form data."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from scripts.tg_ingest import post_ingest


def test_post_ingest_sends_channel() -> None:
    http = MagicMock()
    resp = MagicMock()
    resp.status_code = 202
    http.post = AsyncMock(return_value=resp)

    asyncio.run(
        post_ingest(
            http, "http://api", "k", "tg_acme_9.txt", "body", "2026-07-23",
            None, group="news", channel="acme",
        )
    )

    _, kwargs = http.post.call_args
    assert kwargs["data"]["channel"] == "acme"
    assert kwargs["data"]["group"] == "news"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_channel_field.py -v`
Expected: FAIL — `post_ingest() got an unexpected keyword argument 'channel'`.

- [ ] **Step 3: Add `channel` to `post_ingest`**

Update the signature (`scripts/tg_ingest.py:74-83`) — add `channel` after `group`:

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
    channel: str = "",
) -> bool:
```

And in its body, after the `group` block (`scripts/tg_ingest.py:89-90`):

```python
        if group:
            data["group"] = group
        if channel:
            data["channel"] = channel
```

- [ ] **Step 4: Pass the channel at all three call sites**

`read_and_enqueue` (`scripts/tg_ingest.py:124`):

```python
            ok = await post_ingest(
                http, api_base, api_key, filename, text, document_date, queue,
                channel=channel.lstrip("@"),
            )
```

`reingest_channels` (`scripts/tg_ingest.py:172-175`):

```python
            ok = await post_ingest(
                http, api_base, api_key, filename, text, document_date, queue,
                group=group, priority=priority, channel=slug.lstrip("@"),
            )
```

`sync_round` (`scripts/tg_ingest.py:373-375`):

```python
            ok = await post_ingest(
                http, api_base, api_key, filename, text, document_date, queue,
                group=group, channel=slug.lstrip("@"),
            )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_scripts/test_tg_ingest_channel_field.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest_channel_field.py
git commit -m "feat(tg_ingest): send channel slug to /ingest for source_channel stats"
```

---

## Task 5: Aggregation methods on `AsyncPostgres`

**Files:**
- Modify: `src/storage/postgres.py` (add methods + a module status tuple)
- Test: `tests/test_storage/test_message_stats_queries.py` (create)

**Interfaces:**
- Consumes: `documents` source columns (Task 1).
- Produces:
  - `AsyncPostgres.status_counts_by(dimension: str, since=None, until=None) -> list[dict]` — `dimension ∈ {'source_channel','source_group'}`. Each dict: `{key: str, total: int, pending, processing, completed, vector_only, failed, skipped: int}`, sorted by `total` desc.
  - `AsyncPostgres.timeline_counts(date_field='created_at', group_by=None, channel=None, group=None, since=None, until=None) -> list[dict]` — `date_field ∈ {'created_at','doc_date'}`, `group_by ∈ {None,'channel','group'}`. Each dict: `{day: date, count: int}` plus `key: str` when `group_by` is set. Ordered by `day`.
  - Module constant `DOC_STATUSES: tuple[str, ...]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_storage/test_message_stats_queries.py`:

```python
"""status_counts_by + timeline_counts aggregate documents rows. Integration —
skipped when Postgres is unreachable."""

from __future__ import annotations

import asyncio
import uuid
from datetime import date

import psycopg
import pytest

from src.config import settings
from src.storage.postgres import AsyncPostgres


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="local Postgres unreachable"
)

_TAG = "stats-test-" + uuid.uuid4().hex[:8]  # unique marker for cleanup via path


def _seed() -> None:
    rows = [
        # source_channel, source_group, status, doc_date
        ("alpha", "news", "completed", "2026-07-20"),
        ("alpha", "news", "completed", "2026-07-21"),
        ("alpha", "news", "failed", "2026-07-21"),
        ("beta", "analytics", "completed", "2026-07-21"),
        ("", "", "completed", "2026-07-21"),  # non-TG: must be excluded
    ]
    with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for ch, gr, st, dd in rows:
                cur.execute(
                    "INSERT INTO documents "
                    "(id, path, status, source_channel, source_group, doc_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (str(uuid.uuid4()), f"{_TAG}/x.txt", st, ch, gr, dd),
                )


def _cleanup() -> None:
    with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM documents WHERE path LIKE %s", (f"{_TAG}/%",))


def test_status_counts_by_channel() -> None:
    _seed()
    pg = AsyncPostgres(settings.postgres.dsn)
    try:
        rows = asyncio.run(pg.status_counts_by("source_channel"))
        by_key = {r["key"]: r for r in rows}
        assert "" not in by_key  # non-TG excluded
        assert by_key["alpha"]["total"] == 3
        assert by_key["alpha"]["completed"] == 2
        assert by_key["alpha"]["failed"] == 1
        assert by_key["beta"]["completed"] == 1
        # sorted by total desc → alpha before beta
        keys = [r["key"] for r in rows if r["key"] in ("alpha", "beta")]
        assert keys.index("alpha") < keys.index("beta")
    finally:
        _cleanup()


def test_timeline_counts_by_channel_on_doc_date() -> None:
    _seed()
    pg = AsyncPostgres(settings.postgres.dsn)
    try:
        buckets = asyncio.run(
            pg.timeline_counts(date_field="doc_date", group_by="channel",
                               channel="alpha")
        )
        by_day = {b["day"]: b["count"] for b in buckets}
        assert by_day[date(2026, 7, 20)] == 1
        assert by_day[date(2026, 7, 21)] == 2
        assert all(b["key"] == "alpha" for b in buckets)
    finally:
        _cleanup()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_storage/test_message_stats_queries.py -v`
Expected: FAIL — `AttributeError: 'AsyncPostgres' object has no attribute 'status_counts_by'` (or SKIPPED if PG down).

- [ ] **Step 3: Add the module constant + methods**

Near the top of `src/storage/postgres.py` (after the imports, before `DocumentRow`):

```python
DOC_STATUSES: tuple[str, ...] = (
    "pending", "processing", "completed", "vector_only", "failed", "skipped",
)

_DIMENSIONS = {"source_channel", "source_group"}
_DATE_FIELDS = {"created_at", "doc_date"}
_GROUP_BY_COL = {"channel": "source_channel", "group": "source_group"}
```

Add these two methods to the `AsyncPostgres` class (after `get`, `src/storage/postgres.py:133`):

```python
    async def status_counts_by(
        self,
        dimension: str,
        since: object | None = None,
        until: object | None = None,
    ) -> list[dict]:
        """Per-key (channel or group) count of each pipeline status.

        `dimension` MUST be 'source_channel' or 'source_group' — validated
        against a hardcoded set before interpolation. Empty keys (non-TG
        docs) are excluded. `since`/`until` bound `created_at` (inclusive /
        exclusive). Returns dicts with a count per status in DOC_STATUSES plus
        `total`, sorted by total desc."""
        if dimension not in _DIMENSIONS:
            raise ValueError(f"bad dimension {dimension!r}")
        where = [f"{dimension} <> ''"]
        params: list[object] = []
        if since is not None:
            where.append("created_at >= %s")
            params.append(since)
        if until is not None:
            where.append("created_at < %s")
            params.append(until)
        sql = (
            f"SELECT {dimension} AS key, status, COUNT(*) AS n "
            f"FROM documents WHERE {' AND '.join(where)} "
            f"GROUP BY key, status"
        )
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            raw = await cur.fetchall()
        agg: dict[str, dict] = {}
        for key, status, n in raw:
            row = agg.setdefault(
                key,
                {"key": key, "total": 0, **{s: 0 for s in DOC_STATUSES}},
            )
            if status in row:
                row[status] = n
            row["total"] += n
        return sorted(agg.values(), key=lambda r: r["total"], reverse=True)

    async def timeline_counts(
        self,
        date_field: str = "created_at",
        group_by: str | None = None,
        channel: str | None = None,
        group: str | None = None,
        since: object | None = None,
        until: object | None = None,
    ) -> list[dict]:
        """Daily message counts. `date_field` ∈ {'created_at','doc_date'};
        `group_by` ∈ {None,'channel','group'} adds a per-key breakdown.
        Optional `channel`/`group` filter to one key. All identifiers are
        allowlisted; values bind as parameters. Ordered by day."""
        if date_field not in _DATE_FIELDS:
            raise ValueError(f"bad date_field {date_field!r}")
        keycol = None
        if group_by is not None:
            if group_by not in _GROUP_BY_COL:
                raise ValueError(f"bad group_by {group_by!r}")
            keycol = _GROUP_BY_COL[group_by]
        select = [f"date_trunc('day', {date_field})::date AS day"]
        group_cols = ["day"]
        if keycol:
            select.append(f"{keycol} AS key")
            group_cols.append("key")
        where = [f"{date_field} IS NOT NULL"]
        params: list[object] = []
        if channel is not None:
            where.append("source_channel = %s")
            params.append(channel)
        if group is not None:
            where.append("source_group = %s")
            params.append(group)
        if since is not None:
            where.append(f"{date_field} >= %s")
            params.append(since)
        if until is not None:
            where.append(f"{date_field} < %s")
            params.append(until)
        sql = (
            f"SELECT {', '.join(select)}, COUNT(*) AS n "
            f"FROM documents WHERE {' AND '.join(where)} "
            f"GROUP BY {', '.join(group_cols)} ORDER BY day"
        )
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            raw = await cur.fetchall()
        out: list[dict] = []
        for rec in raw:
            if keycol:
                day, key, n = rec
                out.append({"day": day, "key": key, "count": n})
            else:
                day, n = rec
                out.append({"day": day, "count": n})
        return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_storage/test_message_stats_queries.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/storage/postgres.py tests/test_storage/test_message_stats_queries.py
git commit -m "feat(storage): status_counts_by + timeline_counts aggregation methods"
```

---

## Task 6: Stats HTTP router

**Files:**
- Create: `src/api/routes/stats.py`
- Modify: `src/api/main.py:17-24` (imports), `:85-92` (router registration)
- Test: `tests/test_api/test_stats_routes.py` (create)

**Interfaces:**
- Consumes: `AsyncPostgres.status_counts_by`, `AsyncPostgres.timeline_counts` (Task 5); `require_api_key` (`src/api/auth.py`).
- Produces: `GET /api/v1/stats/messages`, `GET /api/v1/stats/timeline`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_api/test_stats_routes.py`:

```python
"""Stats routes: JSON shape, enum validation, auth. pg aggregation methods are
patched with AsyncMock so no DB is needed."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


@pytest.mark.asyncio
async def test_messages_stats_shape() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    fake = [{"key": "alpha", "total": 3, "pending": 0, "processing": 0,
             "completed": 2, "vector_only": 0, "failed": 1, "skipped": 0}]
    with patch.object(AsyncPostgres, "status_counts_by",
                      new=AsyncMock(return_value=fake)) as m:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/v1/stats/messages?group_by=channel",
                headers=_api_key_header(),
            )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["group_by"] == "channel"
    assert body["rows"][0]["key"] == "alpha"
    assert body["rows"][0]["failed"] == 1
    # 'channel' → source_channel dimension
    assert m.call_args.args[0] == "source_channel"


@pytest.mark.asyncio
async def test_messages_stats_bad_group_by_422() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get(
            "/api/v1/stats/messages?group_by=bogus",
            headers=_api_key_header(),
        )
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_messages_stats_requires_api_key() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/stats/messages")
    assert resp.status_code in (401, 403), resp.text


@pytest.mark.asyncio
async def test_timeline_shape() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    fake = [{"day": date(2026, 7, 21), "key": "alpha", "count": 2}]
    with patch.object(AsyncPostgres, "timeline_counts",
                      new=AsyncMock(return_value=fake)):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/v1/stats/timeline?date_field=doc_date&group_by=channel",
                headers=_api_key_header(),
            )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["date_field"] == "doc_date"
    assert body["buckets"][0]["count"] == 2
    assert body["buckets"][0]["key"] == "alpha"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_api/test_stats_routes.py -v`
Expected: FAIL — 404 on `/api/v1/stats/messages` (router not registered).

- [ ] **Step 3: Create the router**

Create `src/api/routes/stats.py`:

```python
"""Processed-message statistics over the `documents` table.

Two read-only endpoints, both API-key gated:
  * GET /stats/messages  — per-channel or per-group × status breakdown
  * GET /stats/timeline  — daily message counts (created_at or doc_date)

All aggregation SQL lives in AsyncPostgres.status_counts_by /
timeline_counts so the CLI (scripts/message_stats.py) shares it verbatim.
"""

from __future__ import annotations

from datetime import date

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from src.api.auth import require_api_key
from src.storage.postgres import AsyncPostgres

router = APIRouter(
    prefix="/stats",
    tags=["stats"],
    dependencies=[Depends(require_api_key)],
)

_GROUP_BY_DIM = {"channel": "source_channel", "group": "source_group"}
_DATE_FIELDS = {"created_at", "doc_date"}


class StatRow(BaseModel):
    key: str
    total: int
    pending: int
    processing: int
    completed: int
    vector_only: int
    failed: int
    skipped: int


class MessagesStatsResponse(BaseModel):
    group_by: str
    rows: list[StatRow]


class TimelineBucket(BaseModel):
    day: date
    key: str | None = None
    count: int


class TimelineResponse(BaseModel):
    date_field: str
    buckets: list[TimelineBucket]


@router.get("/messages", response_model=MessagesStatsResponse,
            summary="Per-channel/group message counts by status")
@inject
async def messages_stats(
    pg: FromDishka[AsyncPostgres],
    group_by: str = "channel",
    since: date | None = None,
    until: date | None = None,
) -> MessagesStatsResponse:
    dimension = _GROUP_BY_DIM.get(group_by)
    if dimension is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"group_by must be one of {sorted(_GROUP_BY_DIM)}",
        )
    rows = await pg.status_counts_by(dimension, since=since, until=until)
    return MessagesStatsResponse(group_by=group_by, rows=rows)


@router.get("/timeline", response_model=TimelineResponse,
            summary="Daily message counts")
@inject
async def timeline_stats(
    pg: FromDishka[AsyncPostgres],
    date_field: str = "created_at",
    group_by: str | None = None,
    channel: str | None = None,
    group: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> TimelineResponse:
    if date_field not in _DATE_FIELDS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"date_field must be one of {sorted(_DATE_FIELDS)}",
        )
    if group_by is not None and group_by not in _GROUP_BY_DIM:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"group_by must be one of {sorted(_GROUP_BY_DIM)}",
        )
    buckets = await pg.timeline_counts(
        date_field=date_field, group_by=group_by,
        channel=channel, group=group, since=since, until=until,
    )
    return TimelineResponse(date_field=date_field, buckets=buckets)
```

- [ ] **Step 4: Register the router**

In `src/api/main.py`, add `stats` to the routes import block (`:17-24`):

```python
from src.api.routes import (
    admin,
    documents,
    graph_admin,
    health,
    ingest,
    search_v2,
    stats,
)
```

And register it alongside the other `/api/v1` routers (after `documents`, `src/api/main.py:89`):

```python
app.include_router(stats.router, prefix="/api/v1")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_api/test_stats_routes.py -v`
Expected: PASS (all four tests)

- [ ] **Step 6: Commit**

```bash
git add src/api/routes/stats.py src/api/main.py tests/test_api/test_stats_routes.py
git commit -m "feat(api): /stats/messages + /stats/timeline endpoints"
```

---

## Task 7: CLI — `scripts/message_stats.py`

**Files:**
- Create: `scripts/message_stats.py`
- Test: `tests/test_scripts/test_message_stats_cli.py` (create)

**Interfaces:**
- Consumes: `AsyncPostgres.status_counts_by`, `AsyncPostgres.timeline_counts` (Task 5); `settings.postgres.dsn`.
- Produces: `format_status_rows(rows: list[dict]) -> str`; `main(argv: list[str] | None = None) -> None` with subcommands `channels`, `groups`, `timeline`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_scripts/test_message_stats_cli.py`:

```python
"""CLI wiring: `channels` calls status_counts_by('source_channel', ...) and
its rows render into the table. No DB — AsyncPostgres is patched."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from scripts.message_stats import format_status_rows, main


def test_format_status_rows_aligns() -> None:
    rows = [{"key": "alpha", "total": 3, "pending": 0, "processing": 0,
             "completed": 2, "vector_only": 0, "failed": 1, "skipped": 0}]
    out = format_status_rows(rows)
    assert "alpha" in out
    assert "completed" in out  # header present
    assert "3" in out


def test_channels_subcommand_uses_source_channel() -> None:
    fake = [{"key": "alpha", "total": 1, "pending": 0, "processing": 0,
             "completed": 1, "vector_only": 0, "failed": 0, "skipped": 0}]
    with patch("scripts.message_stats.AsyncPostgres") as cls:
        cls.return_value.status_counts_by = AsyncMock(return_value=fake)
        main(["channels"])
    assert cls.return_value.status_counts_by.call_args.args[0] == "source_channel"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scripts/test_message_stats_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.message_stats'`.

- [ ] **Step 3: Create the CLI**

Create `scripts/message_stats.py`:

```python
"""CLI for processed-message statistics — a thin wrapper over the same
AsyncPostgres aggregation methods the /stats endpoints use.

Usage::

    python -m scripts.message_stats channels [--since 2026-07-01] [--until 2026-07-23]
    python -m scripts.message_stats groups
    python -m scripts.message_stats timeline [--date-field doc_date]
                 [--group-by channel] [--channel acme] [--group news]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.storage.postgres import DOC_STATUSES, AsyncPostgres

_COLS = ("key", "total", *DOC_STATUSES)


def format_status_rows(rows: list[dict]) -> str:
    """Render status_counts_by output as an aligned text table."""
    widths = {c: len(c) for c in _COLS}
    for r in rows:
        for c in _COLS:
            widths[c] = max(widths[c], len(str(r.get(c, ""))))
    header = "  ".join(c.ljust(widths[c]) for c in _COLS)
    lines = [header, "  ".join("-" * widths[c] for c in _COLS)]
    for r in rows:
        lines.append("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in _COLS))
    return "\n".join(lines)


def format_timeline(buckets: list[dict]) -> str:
    if buckets and "key" in buckets[0]:
        return "\n".join(f"{b['day']}  {b['key']}  {b['count']}" for b in buckets)
    return "\n".join(f"{b['day']}  {b['count']}" for b in buckets)


async def _run(args: argparse.Namespace) -> None:
    pg = AsyncPostgres(settings.postgres.dsn)
    if args.cmd == "channels":
        rows = await pg.status_counts_by(
            "source_channel", since=args.since, until=args.until,
        )
        print(format_status_rows(rows))
    elif args.cmd == "groups":
        rows = await pg.status_counts_by(
            "source_group", since=args.since, until=args.until,
        )
        print(format_status_rows(rows))
    elif args.cmd == "timeline":
        buckets = await pg.timeline_counts(
            date_field=args.date_field, group_by=args.group_by,
            channel=args.channel, group=args.group,
            since=args.since, until=args.until,
        )
        print(format_timeline(buckets))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="message_stats")
    sub = parser.add_subparsers(dest="cmd", required=True)

    for name in ("channels", "groups"):
        p = sub.add_parser(name)
        p.add_argument("--since")
        p.add_argument("--until")

    t = sub.add_parser("timeline")
    t.add_argument("--date-field", dest="date_field", default="created_at",
                   choices=["created_at", "doc_date"])
    t.add_argument("--group-by", dest="group_by", default=None,
                   choices=["channel", "group"])
    t.add_argument("--channel", default=None)
    t.add_argument("--group", default=None)
    t.add_argument("--since")
    t.add_argument("--until")

    args = parser.parse_args(argv)
    # channels/groups have no timeline-only attrs; default them so _run is uniform.
    for attr in ("date_field", "group_by", "channel", "group"):
        if not hasattr(args, attr):
            setattr(args, attr, None)
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_scripts/test_message_stats_cli.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/message_stats.py tests/test_scripts/test_message_stats_cli.py
git commit -m "feat(cli): message_stats — channels/groups/timeline over shared SQL"
```

---

## Task 8: Docs — runbook note

**Files:**
- Modify: `docs/runbook/reingest-and-priority.md` (append a "Message statistics" section) — if the file does not exist, create `docs/runbook/message-stats.md` instead.

**Interfaces:** none (documentation only).

- [ ] **Step 1: Add a runbook section**

Append (or create) a short operator section covering:

```markdown
## Message statistics

Processed-message counts come from the `documents` table's `source_channel` /
`source_group` columns (populated by tg_ingest via the `/ingest` `channel` +
`group` form fields; historical rows backfilled from `path` by `setup_db.py`).

HTTP (needs `X-API-Key`):
- `GET /api/v1/stats/messages?group_by=channel|group[&since=YYYY-MM-DD][&until=YYYY-MM-DD]`
  → per-channel/group counts by status (`completed/failed/pending/...`).
- `GET /api/v1/stats/timeline?date_field=created_at|doc_date[&group_by=channel|group][&channel=][&group=][&since=][&until=]`
  → daily message counts.

CLI (direct DB, no API):
- `python -m scripts.message_stats channels [--since --until]`
- `python -m scripts.message_stats groups`
- `python -m scripts.message_stats timeline [--date-field doc_date] [--group-by channel]`

Note: `source_group` is empty for documents ingested before this feature
shipped (group was not persisted historically); `source_channel` is
backfilled from the filename for Telegram rows only.
```

- [ ] **Step 2: Commit**

```bash
git add docs/runbook/
git commit -m "docs(runbook): message statistics endpoints + CLI"
```

---

## Final verification

- [ ] Run the full new-test subset:

```bash
uv run pytest tests/test_storage/test_insert_pending_source.py \
  tests/test_storage/test_message_stats_queries.py \
  tests/test_api/test_stats_routes.py \
  tests/test_api/test_ingest_source_channel.py \
  tests/test_scripts/test_tg_ingest_channel_field.py \
  tests/test_scripts/test_message_stats_cli.py \
  tests/test_scripts/test_setup_db_source_columns.py -v
```

Expected: all PASS (Postgres-integration ones SKIP if no local DB — bring PG up via docker compose to exercise them).

- [ ] Sanity-check the existing ingest suite still green (signature changes):

```bash
uv run pytest tests/test_api/test_ingest.py tests/test_api/test_ingest_group.py -v
```

- [ ] Manual smoke (optional, needs running API + PG):

```bash
curl -s -H "X-API-Key: $KEY" "http://localhost:8000/api/v1/stats/messages?group_by=channel" | jq
python -m scripts.message_stats channels
```

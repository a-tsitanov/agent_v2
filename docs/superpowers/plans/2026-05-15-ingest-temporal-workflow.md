# Ingest Temporal Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monolithic taskiq `process_document` task with a Temporal workflow of 8 activities + `mark_failed`, with per-stage retry/timeout, claim-check state passing via MinIO, and best-effort graph half.

**Architecture:** Self-hosted Temporal in `docker-compose` (Postgres backend). One worker process polls workflow + activity task queues. State between activities is passed via small payloads (URIs, IDs) plus pickled blobs in MinIO bucket `kb-staging/{workflow_run_id}/{stage}.pkl`. RabbitMQ + taskiq removed at the end of migration.

**Tech Stack:** `temporalio` Python SDK, Pydantic v2 contracts, MinIO claim-check, existing FastAPI + Postgres + Milvus + Neo4j stack.

**Spec:** `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md`

**Session protocol (user preference):** Pause after each labelled **Stage** for the user to sync before starting the next one.

---

## Stage 1 — Scaffolding (additive, no behaviour change)

### Task 1: Add `temporalio` dependency + Temporal compose services

**Files:**
- Modify: `pyproject.toml`
- Modify: `docker-compose.yml`
- Modify: `src/config.py:124-300` (add `TemporalSettings`)
- Modify: `.env.example`

- [ ] **Step 1: Add `temporalio` to deps**

In `pyproject.toml`, under `dependencies`, add (keep alphabetical-ish — sits next to other clients):

```toml
    # Workflow engine
    "temporalio>=1.8,<2",
```

Leave `taskiq` / `taskiq-aio-pika` in place — they are removed in Stage 5.

- [ ] **Step 2: Add Temporal services to `docker-compose.yml`**

Append after the `rabbitmq` block:

```yaml
  # ── Workflow engine: Temporal ──────────────────────────────────────
  temporal:
    image: temporalio/auto-setup:1.25.2
    environment:
      DB: postgres12
      DB_PORT: 5432
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PWD: ${POSTGRES_PASSWORD:-postgres}
      POSTGRES_SEEDS: postgres
      DBNAME: temporal
      VISIBILITY_DBNAME: temporal_visibility
    ports:
      - "7233:7233"
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  temporal-ui:
    image: temporalio/ui:2.31.2
    environment:
      TEMPORAL_ADDRESS: temporal:7233
      TEMPORAL_CORS_ORIGINS: http://localhost:3000
    ports:
      - "8080:8080"
    depends_on:
      - temporal
    restart: unless-stopped
```

- [ ] **Step 3: Add `TemporalSettings` to `src/config.py`**

Insert next to `RabbitMQSettings` (around line 124):

```python
class TemporalSettings(BaseSettings):
    """Temporal worker / client connection settings."""

    model_config = SettingsConfigDict(env_prefix="TEMPORAL_", extra="ignore")

    host: str = "localhost"
    port: int = 7233
    namespace: str = "default"
    task_queue: str = "kb-ingest"
    activity_concurrency: int = 4
    staging_bucket: str = "kb-staging"

    @property
    def target(self) -> str:
        return f"{self.host}:{self.port}"
```

In the `Settings` aggregator (around line 280, next to `rabbitmq`), add:

```python
    @cached_property
    def temporal(self) -> TemporalSettings:
        return TemporalSettings()
```

And add `"TemporalSettings"` to the `__all__` list at the bottom.

- [ ] **Step 4: Add Temporal env vars to `.env.example`**

Append:

```env
# Temporal
TEMPORAL_HOST=localhost
TEMPORAL_PORT=7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=kb-ingest
TEMPORAL_ACTIVITY_CONCURRENCY=4
TEMPORAL_STAGING_BUCKET=kb-staging
```

- [ ] **Step 5: Install + bring up services + smoke check**

Run:
```bash
uv sync
docker compose up -d temporal temporal-ui
sleep 10
docker compose ps temporal temporal-ui
curl -fsS http://localhost:8080/ -o /dev/null && echo "ui ok"
```
Expected: both containers `running`, "ui ok" printed.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml docker-compose.yml src/config.py .env.example uv.lock
git commit -m "feat(workflow): add temporalio dep and Temporal compose services"
```

---

### Task 2: Create `src/workflow/` package + Pydantic contracts

**Files:**
- Create: `src/workflow/__init__.py`
- Create: `src/workflow/contracts.py`
- Create: `tests/test_workflow/__init__.py`
- Create: `tests/test_workflow/test_contracts.py`

- [ ] **Step 1: Write failing test for contract round-trip**

`tests/test_workflow/test_contracts.py`:

```python
"""Contracts cross the Temporal boundary, so they must JSON-roundtrip
losslessly with the default DataConverter (Pydantic v2 -> JSON)."""

from __future__ import annotations

import uuid

from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    IngestParams,
    IngestResult,
    Injected,
    Indexed,
    KGExtracted,
    MarkFailedIn,
    Merged,
    Parsed,
)


def test_ingest_params_roundtrip() -> None:
    p = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x/y.pdf")
    assert IngestParams.model_validate_json(p.model_dump_json()) == p


def test_ctx_roundtrip() -> None:
    c = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path="/tmp/x/y.pdf",
        cleanup_dir="/tmp/x",
        workflow_run_id="run-abc",
    )
    assert Ctx.model_validate_json(c.model_dump_json()) == c


def test_parsed_roundtrip() -> None:
    ctx = Ctx(doc_id="d", local_path="/tmp/f", cleanup_dir=None, workflow_run_id="r")
    p = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=42)
    assert Parsed.model_validate_json(p.model_dump_json()) == p


def test_finalize_in_carries_graph_status() -> None:
    ctx = Ctx(doc_id="d", local_path="/tmp/f", cleanup_dir=None, workflow_run_id="r")
    idx = Indexed(node_ids=["a", "b"], count=2)
    fin = FinalizeIn(ctx=ctx, indexed=idx, graph_status="vector_only")
    assert FinalizeIn.model_validate_json(fin.model_dump_json()) == fin


def test_ingest_result_shape() -> None:
    r = IngestResult(doc_id="d", chunk_count=2, graph_status="completed")
    assert IngestResult.model_validate_json(r.model_dump_json()) == r


def test_mark_failed_in_optional_ctx() -> None:
    # mark_failed runs even before ctx exists (fetch_source crashed).
    m = MarkFailedIn(
        ctx=None,
        params=IngestParams(doc_id="d", path="/tmp/x"),
        error="boom",
    )
    assert MarkFailedIn.model_validate_json(m.model_dump_json()) == m
```

- [ ] **Step 2: Run test to see ImportError**

Run: `uv run pytest tests/test_workflow/test_contracts.py -v`
Expected: `ModuleNotFoundError: No module named 'src.workflow'`.

- [ ] **Step 3: Create empty package init**

`src/workflow/__init__.py`:
```python
"""Temporal workflow + activities for document ingestion."""
```

`tests/test_workflow/__init__.py`: empty file.

- [ ] **Step 4: Write contracts**

`src/workflow/contracts.py`:
```python
"""Payloads exchanged between the workflow and its activities.

Heavy state (list[BaseNode], EntityNode lists) NEVER travels in
payloads — it is pickled to MinIO and referenced by URI.  These
contracts carry only IDs, URIs, and small counters so the Temporal
DataConverter can JSON-serialise everything safely.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

GraphStatus = Literal["completed", "vector_only"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class IngestParams(_Frozen):
    doc_id: str
    path: str


class Ctx(_Frozen):
    doc_id: str
    local_path: str
    cleanup_dir: str | None
    workflow_run_id: str


class Parsed(_Frozen):
    ctx: Ctx
    nodes_uri: str
    chunk_count: int


class Indexed(_Frozen):
    node_ids: list[str]
    count: int


class Injected(_Frozen):
    count: int


class KGExtracted(_Frozen):
    parsed: Parsed
    nodes_with_kg_uri: str


class Merged(_Frozen):
    kg: KGExtracted
    merged_entities_uri: str


class GraphBuilt(_Frozen):
    entities: int
    relations: int


class FinalizeIn(_Frozen):
    ctx: Ctx
    indexed: Indexed
    graph_status: GraphStatus


class MarkFailedIn(_Frozen):
    ctx: Ctx | None
    params: IngestParams
    error: str


class IngestResult(_Frozen):
    doc_id: str
    chunk_count: int
    graph_status: GraphStatus
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_workflow/test_contracts.py -v`
Expected: 6 passing tests.

- [ ] **Step 6: Commit**

```bash
git add src/workflow/__init__.py src/workflow/contracts.py \
        tests/test_workflow/__init__.py tests/test_workflow/test_contracts.py
git commit -m "feat(workflow): Pydantic contracts for ingest workflow payloads"
```

---

### Task 3: Staging-blob claim-check helpers

**Files:**
- Create: `src/workflow/staging.py`
- Create: `tests/test_workflow/test_staging.py`

- [ ] **Step 1: Write failing test for write+read roundtrip**

`tests/test_workflow/test_staging.py`:

```python
"""Staging helper persists pickled objects to MinIO `kb-staging` and
returns an s3:// URI.  Tests use a MagicMock MinIO client to keep
this layer unit-testable; live behaviour is exercised in
tests/test_workflow/test_workflow_local.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow.staging import StagingStore


def test_write_returns_s3_uri() -> None:
    client = MagicMock()
    store = StagingStore(client=client, bucket="kb-staging")
    uri = store.write_pickle("run-1", "parsed", {"hello": "world"})
    assert uri == "s3://kb-staging/run-1/parsed.pkl"
    client.put_object.assert_called_once()
    args, kwargs = client.put_object.call_args
    # bucket, key, stream, length, content_type=...
    assert args[0] == "kb-staging"
    assert args[1] == "run-1/parsed.pkl"


def test_read_pickle_roundtrips_object() -> None:
    import io
    import pickle

    payload = {"answer": 42, "nodes": [1, 2, 3]}
    blob = pickle.dumps(payload)

    client = MagicMock()
    response = MagicMock()
    response.read.return_value = blob
    response.close = MagicMock()
    response.release_conn = MagicMock()
    client.get_object.return_value = response

    store = StagingStore(client=client, bucket="kb-staging")
    out = store.read_pickle("s3://kb-staging/run-1/parsed.pkl")
    assert out == payload
    client.get_object.assert_called_once_with("kb-staging", "run-1/parsed.pkl")


def test_delete_prefix_lists_then_removes() -> None:
    client = MagicMock()
    obj1 = MagicMock(object_name="run-1/parsed.pkl")
    obj2 = MagicMock(object_name="run-1/kg.pkl")
    client.list_objects.return_value = [obj1, obj2]

    store = StagingStore(client=client, bucket="kb-staging")
    store.delete_prefix("run-1")

    client.list_objects.assert_called_once_with(
        "kb-staging", prefix="run-1/", recursive=True,
    )
    assert client.remove_object.call_count == 2


def test_read_pickle_rejects_wrong_bucket() -> None:
    client = MagicMock()
    store = StagingStore(client=client, bucket="kb-staging")
    with pytest.raises(ValueError, match="wrong bucket"):
        store.read_pickle("s3://kb-uploads/run-1/parsed.pkl")
```

- [ ] **Step 2: Run test, verify ImportError**

Run: `uv run pytest tests/test_workflow/test_staging.py -v`
Expected: `ModuleNotFoundError: No module named 'src.workflow.staging'`.

- [ ] **Step 3: Implement `StagingStore`**

`src/workflow/staging.py`:
```python
"""Claim-check store for workflow stage outputs.

Activities pickle their large in-memory state (parsed LlamaIndex
nodes, KG entity/relation lists) to MinIO under
``s3://{bucket}/{workflow_run_id}/{stage}.pkl`` and pass only the URI
to the next activity.  ``finalize`` (and the failure path) removes
the whole ``{workflow_run_id}/`` prefix so we don't accrete blobs.

Pickle is fine here because:
  * the producer and consumer share the same Python image,
  * blobs are short-lived (lifetime of one workflow run),
  * the on-disk format is never read by anything outside this
    package.
"""

from __future__ import annotations

import io
import pickle
from typing import Any

from loguru import logger
from minio import Minio

from src.config import settings


class StagingStore:
    """Thin wrapper around the MinIO client for stage blobs."""

    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def write_pickle(self, run_id: str, stage: str, obj: Any) -> str:
        """Pickle `obj` and upload to ``{run_id}/{stage}.pkl``.

        Returns the full ``s3://`` URI suitable for handing to the
        next activity.
        """
        blob = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        key = f"{run_id}/{stage}.pkl"
        self._client.put_object(
            self._bucket, key, io.BytesIO(blob), len(blob),
            content_type="application/octet-stream",
        )
        uri = f"s3://{self._bucket}/{key}"
        logger.info(
            "staging write  run={r}  stage={s}  uri={u}  bytes={n}",
            r=run_id, s=stage, u=uri, n=len(blob),
        )
        return uri

    def read_pickle(self, uri: str) -> Any:
        """Reverse of `write_pickle`."""
        bucket, key = _parse_uri(uri)
        if bucket != self._bucket:
            raise ValueError(
                f"wrong bucket for staging read: {bucket!r} vs {self._bucket!r}",
            )
        response = self._client.get_object(bucket, key)
        try:
            blob = response.read()
        finally:
            response.close()
            response.release_conn()
        return pickle.loads(blob)

    def delete_prefix(self, run_id: str) -> None:
        """Best-effort cleanup of every blob under ``{run_id}/``."""
        prefix = f"{run_id}/"
        for obj in self._client.list_objects(
            self._bucket, prefix=prefix, recursive=True,
        ):
            self._client.remove_object(self._bucket, obj.object_name)


def _parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 uri: {uri!r}")
    return bucket, key


def build_staging_store() -> StagingStore:
    """Construct a StagingStore from the project settings.

    Reuses the same MinIO endpoint as ``build_minio_storage`` — the
    bucket is the only difference.  Ensures the bucket exists.
    """
    cfg = settings.minio
    client = Minio(
        cfg.endpoint,
        access_key=cfg.access_key.get_secret_value(),
        secret_key=cfg.secret_key.get_secret_value(),
        secure=cfg.secure,
        region=cfg.region,
    )
    bucket = settings.temporal.staging_bucket
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("staging  bucket created  name={b}", b=bucket)
    return StagingStore(client=client, bucket=bucket)
```

- [ ] **Step 4: Run test, verify pass**

Run: `uv run pytest tests/test_workflow/test_staging.py -v`
Expected: 4 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/staging.py tests/test_workflow/test_staging.py
git commit -m "feat(workflow): MinIO claim-check store for stage payloads"
```

---

**🛑 STAGE 1 GATE — pause for user sync.** Confirm: Temporal Web UI reachable on `:8080`, `uv run pytest tests/test_workflow/ -v` green (10 tests), no behaviour change anywhere else.

---

## Stage 2 — Activities (extract logic into `src/workflow/activities/`)

Each activity wraps already-tested code from `src/ingestion/tasks.py` and friends. Tests in this stage import the activity function directly (no Temporal SDK in the test path) — the workflow harness is added in Stage 3.

### Task 4: `fetch_source` activity

**Files:**
- Create: `src/workflow/activities/__init__.py`
- Create: `src/workflow/activities/fetch_source.py`
- Create: `tests/test_workflow/test_fetch_source.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_fetch_source.py`:

```python
"""`fetch_source` resolves an s3:// path to a local file, updates PG
to `processing`, and is idempotent: re-run with the file already on
disk skips download."""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.fetch_source import fetch_source
from src.workflow.contracts import IngestParams


@pytest.fixture
def fake_minio(tmp_path: Path):
    storage = MagicMock()
    storage.parse_s3_uri.return_value = ("kb-uploads", "doc-1/file.pdf")
    storage.download_dir = tmp_path / "cache"

    def _download(uri: str, local: Path) -> Path:
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(b"PDF")
        return local

    storage.get_object_to_path.side_effect = _download
    return storage


@pytest.mark.asyncio
async def test_s3_path_downloads_and_marks_processing(fake_minio, tmp_path):
    pg = MagicMock()
    pg.update_status = AsyncMock()

    params = IngestParams(
        doc_id="11111111-1111-1111-1111-111111111111",
        path="s3://kb-uploads/doc-1/file.pdf",
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
        return_value=fake_minio,
    ), patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        info = MagicMock(workflow_run_id="run-1")
        mock_activity.info.return_value = info
        ctx = await fetch_source(params)

    assert ctx.local_path == str(tmp_path / "cache" / params.doc_id / "file.pdf")
    assert ctx.cleanup_dir == str(tmp_path / "cache" / params.doc_id)
    assert ctx.workflow_run_id == "run-1"
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(params.doc_id), status="processing",
    )


@pytest.mark.asyncio
async def test_legacy_local_path_passes_through(tmp_path):
    pg = MagicMock()
    pg.update_status = AsyncMock()

    local = tmp_path / "old.txt"
    local.write_text("legacy")
    params = IngestParams(
        doc_id="11111111-1111-1111-1111-111111111111",
        path=str(local),
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
    ) as build, patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        mock_activity.info.return_value = MagicMock(workflow_run_id="run-2")
        ctx = await fetch_source(params)

    assert ctx.local_path == str(local)
    assert ctx.cleanup_dir is None
    build.assert_not_called()


@pytest.mark.asyncio
async def test_s3_path_skips_download_when_file_present(fake_minio, tmp_path):
    """Second activity attempt after worker crash: file already on
    disk → no second MinIO GET."""
    pg = MagicMock()
    pg.update_status = AsyncMock()

    target = tmp_path / "cache" / "doc-1" / "file.pdf"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"PDF-cached")

    params = IngestParams(
        doc_id="11111111-1111-1111-1111-111111111111",
        path="s3://kb-uploads/doc-1/file.pdf",
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
        return_value=fake_minio,
    ), patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        mock_activity.info.return_value = MagicMock(workflow_run_id="run-3")
        await fetch_source(params)

    fake_minio.get_object_to_path.assert_not_called()
```

- [ ] **Step 2: Run test, verify ImportError**

Run: `uv run pytest tests/test_workflow/test_fetch_source.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement activity**

`src/workflow/activities/__init__.py`:
```python
"""Activity functions invoked by `DocumentIngestWorkflow`."""

from src.workflow.activities.fetch_source import fetch_source

__all__ = ["fetch_source"]
```

`src/workflow/activities/fetch_source.py`:
```python
"""`fetch_source` — resolve doc path to a local file + mark processing.

Idempotent: a second attempt after a worker crash finds the file on
disk and skips the MinIO GET.  Postgres `update_status('processing')`
is a no-op overwrite if already processing — safe to repeat.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from loguru import logger
from temporalio import activity

from src.storage.minio import build_minio_storage
from src.storage.postgres import AsyncPostgres
from src.workflow.contracts import Ctx, IngestParams


@activity.defn
async def fetch_source(params: IngestParams) -> Ctx:
    info = activity.info()
    pg = AsyncPostgres()
    await pg.update_status(uuid.UUID(params.doc_id), status="processing")

    if not params.path.startswith("s3://"):
        return Ctx(
            doc_id=params.doc_id,
            local_path=params.path,
            cleanup_dir=None,
            workflow_run_id=info.workflow_run_id,
        )

    storage = build_minio_storage()
    _, key = storage.parse_s3_uri(params.path)
    filename = Path(key).name
    target = storage.download_dir / params.doc_id / filename
    if not target.exists():
        await asyncio.to_thread(storage.get_object_to_path, params.path, target)
        logger.info(
            "fetch_source  download  doc={d}  s3={p}  local={t}",
            d=params.doc_id, p=params.path, t=target,
        )
    else:
        logger.info(
            "fetch_source  cache_hit  doc={d}  local={t}",
            d=params.doc_id, t=target,
        )
    return Ctx(
        doc_id=params.doc_id,
        local_path=str(target),
        cleanup_dir=str(target.parent),
        workflow_run_id=info.workflow_run_id,
    )
```

- [ ] **Step 4: Run test, verify pass**

Run: `uv run pytest tests/test_workflow/test_fetch_source.py -v`
Expected: 3 passing tests.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/activities/__init__.py \
        src/workflow/activities/fetch_source.py \
        tests/test_workflow/test_fetch_source.py
git commit -m "feat(workflow): fetch_source activity"
```

---

### Task 5: `parse_and_chunk` activity

**Files:**
- Create: `src/workflow/activities/parse_and_chunk.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_parse_and_chunk.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_parse_and_chunk.py`:

```python
"""`parse_and_chunk` runs the LlamaIndex IngestionPipeline + writes
the resulting nodes to a staging blob."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.parse_and_chunk import parse_and_chunk
from src.workflow.contracts import Ctx


@pytest.mark.asyncio
async def test_writes_nodes_blob_and_returns_uri(tmp_path: Path):
    local = tmp_path / "doc.pdf"
    local.write_bytes(b"PDF")
    ctx = Ctx(
        doc_id="d",
        local_path=str(local),
        cleanup_dir=str(tmp_path),
        workflow_run_id="run-x",
    )

    node = MagicMock()
    node.node_id = "n1"
    fake_pipeline = MagicMock()
    fake_pipeline.arun = AsyncMock(return_value=[node, node])

    staging = MagicMock()
    staging.write_pickle.return_value = "s3://kb-staging/run-x/parsed.pkl"

    doc = MagicMock()
    doc.metadata = {"file_path": str(local)}

    with patch(
        "src.workflow.activities.parse_and_chunk.read_documents",
        return_value=[doc],
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_ingestion_pipeline",
        return_value=fake_pipeline,
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.parse_and_chunk.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        out = await parse_and_chunk(ctx)

    assert out.nodes_uri == "s3://kb-staging/run-x/parsed.pkl"
    assert out.chunk_count == 2
    staging.write_pickle.assert_called_once()
    args = staging.write_pickle.call_args.args
    assert args[0] == "run-x"
    assert args[1] == "parsed"


@pytest.mark.asyncio
async def test_raises_when_reader_does_not_find_file(tmp_path: Path):
    local = tmp_path / "doc.pdf"
    local.write_bytes(b"PDF")
    ctx = Ctx(
        doc_id="d", local_path=str(local), cleanup_dir=None,
        workflow_run_id="run-y",
    )

    other = MagicMock()
    other.metadata = {"file_path": "/somewhere/else.pdf"}

    with patch(
        "src.workflow.activities.parse_and_chunk.read_documents",
        return_value=[other],
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_ingestion_pipeline",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_staging_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.activity"
    ):
        with pytest.raises(FileNotFoundError):
            await parse_and_chunk(ctx)
```

- [ ] **Step 2: Run, verify ImportError**

Run: `uv run pytest tests/test_workflow/test_parse_and_chunk.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/parse_and_chunk.py`:
```python
"""`parse_and_chunk` — read + split + identifier-canon + translate.

Mirrors the first half of `src.ingestion.tasks.process_document`'s
pipeline section.  Output is the list of LlamaIndex `BaseNode`
objects, pickled to MinIO under `{run_id}/parsed.pkl`.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from temporalio import activity

from src.ingestion.embeddings import build_embedding_model
from src.ingestion.pipeline import build_ingestion_pipeline, read_documents
from src.ingestion.translate_transform import (
    FULL_TRANSLATED_TEXT_KEY,
    ORIGINAL_DOC_LENGTH_KEY,
)
from src.retrieval.llm import build_llm
from src.workflow.contracts import Ctx, Parsed
from src.workflow.staging import build_staging_store


def _scrub(md: dict | None) -> None:
    if not md:
        return
    md.pop(FULL_TRANSLATED_TEXT_KEY, None)
    md.pop(ORIGINAL_DOC_LENGTH_KEY, None)


@activity.defn
async def parse_and_chunk(ctx: Ctx) -> Parsed:
    target = Path(ctx.local_path)
    llm = build_llm()
    embed_model = build_embedding_model()
    pipeline = build_ingestion_pipeline(
        embed_model=embed_model,
        translator_llm=llm,
    )

    docs = read_documents(target.parent, recursive=False)
    docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
    if not docs:
        raise FileNotFoundError(f"file not in reader output: {target}")

    nodes = await pipeline.arun(documents=docs)

    # Scrub doc-translation scaffolding so it never reaches downstream
    # stores.  Same logic as the legacy taskiq task.
    for n in nodes:
        _scrub(getattr(n, "metadata", None))
        for rel in (getattr(n, "relationships", {}) or {}).values():
            _scrub(getattr(rel, "metadata", None))

    activity.heartbeat({"chunks": len(nodes)})

    staging = build_staging_store()
    uri = staging.write_pickle(ctx.workflow_run_id, "parsed", nodes)
    logger.info(
        "parse_and_chunk done  doc={d}  chunks={n}  uri={u}",
        d=ctx.doc_id, n=len(nodes), u=uri,
    )
    return Parsed(ctx=ctx, nodes_uri=uri, chunk_count=len(nodes))
```

- [ ] **Step 4: Wire export**

In `src/workflow/activities/__init__.py`, append:
```python
from src.workflow.activities.parse_and_chunk import parse_and_chunk

__all__ = ["fetch_source", "parse_and_chunk"]
```
(Replace the old `__all__` line accordingly.)

- [ ] **Step 5: Run test, verify pass**

Run: `uv run pytest tests/test_workflow/test_parse_and_chunk.py -v`
Expected: 2 passing tests.

- [ ] **Step 6: Commit**

```bash
git add src/workflow/activities/parse_and_chunk.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_parse_and_chunk.py
git commit -m "feat(workflow): parse_and_chunk activity"
```

---

### Task 6: `index_vector` activity

**Files:**
- Create: `src/workflow/activities/index_vector.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_index_vector.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_index_vector.py`:

```python
"""`index_vector` loads nodes from staging, scrubs Milvus-oversized
metadata, inserts to Milvus, returns the node IDs.  Original
metadata is restored on the in-memory nodes for downstream graph
activities (they receive the same blob via staging again — this test
just verifies the snapshot/restore around insert)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.workflow.activities.index_vector import index_vector
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_indexes_and_returns_ids():
    n1 = MagicMock(node_id="a")
    n1.metadata = {"canonical_identifiers": ["x"], "translated_text": "RU"}
    n2 = MagicMock(node_id="b")
    n2.metadata = {"canonical_identifiers": ["y"]}

    staging = MagicMock()
    staging.read_pickle.return_value = [n1, n2]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=2)

    with patch(
        "src.workflow.activities.index_vector.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.index_vector.build_vector_store",
    ), patch(
        "src.workflow.activities.index_vector.build_vector_index",
    ), patch(
        "src.workflow.activities.index_vector.build_embedding_model",
    ), patch(
        "src.workflow.activities.index_vector.index_nodes",
    ) as mock_index_nodes, patch(
        "src.workflow.activities.index_vector.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        out = await index_vector(parsed)

    assert out.node_ids == ["a", "b"]
    assert out.count == 2
    mock_index_nodes.assert_called_once()


@pytest.mark.asyncio
async def test_restores_metadata_after_insert():
    n = MagicMock(node_id="a")
    n.metadata = {"canonical_identifiers": ["x"], "translated_text": "RU", "k": 1}

    staging = MagicMock()
    staging.read_pickle.return_value = [n]

    captured: dict = {}

    def _capture(idx, nodes):
        # During insert, the oversize keys should be stripped.
        captured["inside"] = dict(nodes[0].metadata)

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.index_vector.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.index_vector.build_vector_store",
    ), patch(
        "src.workflow.activities.index_vector.build_vector_index",
    ), patch(
        "src.workflow.activities.index_vector.build_embedding_model",
    ), patch(
        "src.workflow.activities.index_vector.index_nodes",
        side_effect=_capture,
    ), patch(
        "src.workflow.activities.index_vector.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        await index_vector(parsed)

    # Inside insert: oversize keys removed
    assert "canonical_identifiers" not in captured["inside"]
    assert "translated_text" not in captured["inside"]
    assert captured["inside"]["k"] == 1
    # After insert: everything restored on the in-memory node
    assert "canonical_identifiers" in n.metadata
    assert "translated_text" in n.metadata
```

- [ ] **Step 2: Run, verify ImportError**

Run: `uv run pytest tests/test_workflow/test_index_vector.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/index_vector.py`:
```python
"""`index_vector` — embed + Milvus insert.

Loads parsed nodes from staging, snapshot-strips Milvus-oversized
metadata around `index_nodes`, then writes the resulting node-id list
back to the workflow.  In-memory nodes keep their full metadata so
the same pickle (re-read by the next activity) is unaffected.
"""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.ingestion.embeddings import build_embedding_model
from src.retrieval.vector_index import (
    build_vector_index,
    build_vector_store,
    index_nodes,
)
from src.workflow.contracts import Indexed, Parsed
from src.workflow.staging import build_staging_store

_MILVUS_DROP_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
    "translated_text",
})


def _snapshot_metadata(nodes, keys: frozenset[str]) -> list[dict]:
    snaps: list[dict] = []
    for n in nodes:
        md = getattr(n, "metadata", None)
        snap: dict = {}
        if md:
            for k in list(md.keys()):
                if k in keys:
                    snap[k] = md.pop(k)
        snaps.append(snap)
    return snaps


def _restore_metadata(nodes, snaps: list[dict]) -> None:
    for n, snap in zip(nodes, snaps):
        if not snap:
            continue
        md = getattr(n, "metadata", None)
        if md is None:
            n.metadata = snap
        else:
            md.update(snap)


@activity.defn
async def index_vector(parsed: Parsed) -> Indexed:
    staging = build_staging_store()
    nodes = staging.read_pickle(parsed.nodes_uri)

    embed_model = build_embedding_model()
    store = build_vector_store()
    index = build_vector_index(store, embed_model)

    snaps = _snapshot_metadata(nodes, _MILVUS_DROP_KEYS)
    try:
        index_nodes(index, nodes)
    finally:
        _restore_metadata(nodes, snaps)

    node_ids = [getattr(n, "node_id", "") for n in nodes]
    activity.heartbeat({"indexed": len(node_ids)})
    logger.info(
        "index_vector done  doc={d}  count={n}",
        d=parsed.ctx.doc_id, n=len(node_ids),
    )
    return Indexed(node_ids=node_ids, count=len(node_ids))
```

- [ ] **Step 4: Wire export**

Update `src/workflow/activities/__init__.py`:
```python
from src.workflow.activities.fetch_source import fetch_source
from src.workflow.activities.index_vector import index_vector
from src.workflow.activities.parse_and_chunk import parse_and_chunk

__all__ = ["fetch_source", "index_vector", "parse_and_chunk"]
```

- [ ] **Step 5: Run, verify pass**

Run: `uv run pytest tests/test_workflow/test_index_vector.py -v`
Expected: 2 passing tests.

- [ ] **Step 6: Commit**

```bash
git add src/workflow/activities/index_vector.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_index_vector.py
git commit -m "feat(workflow): index_vector activity"
```

---

### Task 7: `inject_canonical` activity

**Files:**
- Create: `src/workflow/activities/inject_canonical.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_inject_canonical.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_inject_canonical.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_calls_inject_with_loaded_nodes():
    n = MagicMock(node_id="a")
    staging = MagicMock()
    staging.read_pickle.return_value = [n]

    graph_store = MagicMock()
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.inject_canonical.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.inject_canonical.build_neo4j_graph_store",
        return_value=graph_store,
    ), patch(
        "src.workflow.activities.inject_canonical.inject_canonical_entities",
    ) as mock_inject:
        out = await inject_canonical(parsed)

    mock_inject.assert_called_once_with(graph_store, [n])
    assert out.count == 1
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_inject_canonical.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/inject_canonical.py`:
```python
"""`inject_canonical` — write canonical identifier entities to Neo4j."""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.graph.store import build_neo4j_graph_store
from src.ingestion.identifier_transform import inject_canonical_entities
from src.workflow.contracts import Injected, Parsed
from src.workflow.staging import build_staging_store


@activity.defn
async def inject_canonical(parsed: Parsed) -> Injected:
    staging = build_staging_store()
    nodes = staging.read_pickle(parsed.nodes_uri)
    graph_store = build_neo4j_graph_store()
    inject_canonical_entities(graph_store, nodes)
    logger.info(
        "inject_canonical done  doc={d}  chunks={n}",
        d=parsed.ctx.doc_id, n=len(nodes),
    )
    return Injected(count=len(nodes))
```

- [ ] **Step 4: Wire export**

Update `src/workflow/activities/__init__.py` alphabetically — `from src.workflow.activities.inject_canonical import inject_canonical` plus add `"inject_canonical"` to `__all__`.

- [ ] **Step 5: Run + commit**

```bash
uv run pytest tests/test_workflow/test_inject_canonical.py -v
git add src/workflow/activities/inject_canonical.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_inject_canonical.py
git commit -m "feat(workflow): inject_canonical activity"
```

---

### Task 8: `extract_kg` activity

**Files:**
- Create: `src/workflow/activities/extract_kg.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_extract_kg.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_extract_kg.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.extract_kg import extract_kg
from src.workflow.contracts import Ctx, Parsed


@pytest.mark.asyncio
async def test_runs_extractor_and_writes_kg_blob():
    n = MagicMock(node_id="a")
    staging = MagicMock()
    staging.read_pickle.return_value = [n]
    staging.write_pickle.return_value = "s3://kb-staging/r/kg.pkl"

    extractor = MagicMock()
    extractor.acall = AsyncMock(return_value=[n])

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)

    with patch(
        "src.workflow.activities.extract_kg.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.extract_kg.build_kg_extractor",
        return_value=extractor,
    ), patch(
        "src.workflow.activities.extract_kg.build_llm",
        return_value=MagicMock(),
    ):
        out = await extract_kg(parsed)

    extractor.acall.assert_awaited_once_with([n])
    staging.write_pickle.assert_called_once_with("r", "kg", [n])
    assert out.nodes_with_kg_uri == "s3://kb-staging/r/kg.pkl"
    assert out.parsed == parsed
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_extract_kg.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/extract_kg.py`:
```python
"""`extract_kg` — LightRAG-style KG extraction (heaviest stage).

One LLM call per chunk produces KG_NODES_KEY / KG_RELATIONS_KEY
metadata on each node.  Output blob is pickled separately from the
parsed blob so a retry of merge_and_resolve can re-read it without
rerunning the extractor.
"""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.graph.index import build_kg_extractor
from src.retrieval.llm import build_llm
from src.workflow.contracts import KGExtracted, Parsed
from src.workflow.staging import build_staging_store


@activity.defn
async def extract_kg(parsed: Parsed) -> KGExtracted:
    staging = build_staging_store()
    nodes = staging.read_pickle(parsed.nodes_uri)
    llm = build_llm()
    extractor = build_kg_extractor(llm, mode="lightrag")
    nodes = await extractor.acall(nodes)
    activity.heartbeat({"extracted": len(nodes)})
    uri = staging.write_pickle(parsed.ctx.workflow_run_id, "kg", nodes)
    logger.info(
        "extract_kg done  doc={d}  chunks={n}  uri={u}",
        d=parsed.ctx.doc_id, n=len(nodes), u=uri,
    )
    return KGExtracted(parsed=parsed, nodes_with_kg_uri=uri)
```

- [ ] **Step 4: Wire export + run + commit**

```bash
# update __init__.py to add extract_kg
uv run pytest tests/test_workflow/test_extract_kg.py -v
git add src/workflow/activities/extract_kg.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_extract_kg.py
git commit -m "feat(workflow): extract_kg activity"
```

---

### Task 9: `merge_and_resolve` activity

**Files:**
- Create: `src/workflow/activities/merge_and_resolve.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_merge_and_resolve.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_merge_and_resolve.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.contracts import Ctx, KGExtracted, Parsed


@pytest.mark.asyncio
async def test_merge_consolidate_resolve_chain():
    nodes = [MagicMock(node_id="a")]
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"

    merged_entities = [MagicMock()]
    merged_relations = [MagicMock()]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = False

    with patch(
        "src.workflow.activities.merge_and_resolve.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
        new=AsyncMock(return_value=(merged_entities, merged_relations)),
    ), patch(
        "src.workflow.activities.merge_and_resolve._consolidate_phone_entities",
        return_value=(merged_entities, merged_relations, {}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.settings",
        fake_settings,
    ):
        out = await merge_and_resolve(kg)

    assert out.merged_entities_uri == "s3://kb-staging/r/merged.pkl"
    # write_pickle called with (run_id, "merged", (entities, relations, nodes))
    args = staging.write_pickle.call_args.args
    assert args[0] == "r"
    assert args[1] == "merged"


@pytest.mark.asyncio
async def test_runs_er_when_enabled():
    nodes = [MagicMock(node_id="a")]
    staging = MagicMock()
    staging.read_pickle.return_value = nodes
    staging.write_pickle.return_value = "s3://kb-staging/r/merged.pkl"

    merged_entities = [MagicMock()]
    merged_relations = [MagicMock()]

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")

    fake_settings = MagicMock()
    fake_settings.agent.er_enabled = True
    fake_settings.agent.er_judge_batch_size = 8

    er_mock = AsyncMock(return_value=(merged_entities, merged_relations, {}))

    with patch(
        "src.workflow.activities.merge_and_resolve.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.build_neo4j_graph_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.merge_and_resolve.merge_kg_extraction",
        new=AsyncMock(return_value=(merged_entities, merged_relations)),
    ), patch(
        "src.workflow.activities.merge_and_resolve._consolidate_phone_entities",
        return_value=(merged_entities, merged_relations, {}),
    ), patch(
        "src.workflow.activities.merge_and_resolve.resolve_entities",
        new=er_mock,
    ), patch(
        "src.workflow.activities.merge_and_resolve.settings",
        fake_settings,
    ):
        await merge_and_resolve(kg)

    er_mock.assert_awaited_once()
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_merge_and_resolve.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/merge_and_resolve.py`:
```python
"""`merge_and_resolve` — cross-chunk dedup + phone consolidation + ER.

Pulls the post-KG nodes from staging, runs the three dedup passes
(LightRAG merge → phone consolidation → entity resolution), then
writes a tuple ``(entities, relations, nodes)`` to a fresh staging
blob.  Nodes are written too because phone/ER passes can rewrite
chunk-level `KG_NODES_KEY` metadata in-place.
"""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.config import settings
from src.graph.entity_resolution import ERConfig, resolve_entities
from src.graph.merge import merge_kg_extraction
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.ingestion.tasks import _consolidate_phone_entities
from src.retrieval.llm import build_llm
from src.workflow.contracts import KGExtracted, Merged
from src.workflow.staging import build_staging_store


@activity.defn
async def merge_and_resolve(kg: KGExtracted) -> Merged:
    staging = build_staging_store()
    nodes = staging.read_pickle(kg.nodes_with_kg_uri)
    llm = build_llm()

    merged_entities, merged_relations = await merge_kg_extraction(
        nodes, llm, language="Russian",
    )
    merged_entities, merged_relations, _phone_map = _consolidate_phone_entities(
        merged_entities, merged_relations, nodes,
    )
    if settings.agent.er_enabled:
        embed_model = build_embedding_model()
        graph_store = build_neo4j_graph_store()
        merged_entities, merged_relations, _er_map = await resolve_entities(
            merged_entities, merged_relations, nodes,
            llm=llm, embed_model=embed_model, graph_store=graph_store,
            config=ERConfig(
                language="Russian",
                judge_batch=settings.agent.er_judge_batch_size,
                name_token_min_overlap=0.1,
            ),
        )

    uri = staging.write_pickle(
        kg.parsed.ctx.workflow_run_id, "merged",
        (merged_entities, merged_relations, nodes),
    )
    logger.info(
        "merge_and_resolve done  doc={d}  entities={e}  relations={r}",
        d=kg.parsed.ctx.doc_id,
        e=len(merged_entities), r=len(merged_relations),
    )
    return Merged(kg=kg, merged_entities_uri=uri)
```

- [ ] **Step 4: Wire export + run + commit**

```bash
# update __init__.py
uv run pytest tests/test_workflow/test_merge_and_resolve.py -v
git add src/workflow/activities/merge_and_resolve.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_merge_and_resolve.py
git commit -m "feat(workflow): merge_and_resolve activity"
```

---

### Task 10: `build_property_graph` activity

**Files:**
- Create: `src/workflow/activities/build_property_graph.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_build_property_graph.py`

- [ ] **Step 1: Write failing test**

`tests/test_workflow/test_build_property_graph.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.contracts import Ctx, KGExtracted, Merged, Parsed


@pytest.mark.asyncio
async def test_strips_metadata_builds_pg_upserts_entities():
    n = MagicMock(node_id="a")
    n.metadata = {"safe": "str", "bad": {"nested": "x"}}
    entities = [MagicMock()]
    relations = [MagicMock()]

    staging = MagicMock()
    staging.read_pickle.return_value = (entities, relations, [n])

    graph_store = MagicMock()

    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None, workflow_run_id="r")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=1)
    kg = KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/r/kg.pkl")
    merged = Merged(kg=kg, merged_entities_uri="s3://kb-staging/r/merged.pkl")

    with patch(
        "src.workflow.activities.build_property_graph.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.build_property_graph.build_neo4j_graph_store",
        return_value=graph_store,
    ), patch(
        "src.workflow.activities.build_property_graph.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.build_property_graph.build_property_graph_index",
    ) as mock_build:
        out = await build_property_graph(merged)

    mock_build.assert_called_once()
    graph_store.upsert_nodes.assert_called_once_with(entities)
    graph_store.upsert_relations.assert_called_once_with(relations)
    # nested metadata stripped
    assert "bad" not in n.metadata
    assert n.metadata.get("safe") == "str"
    assert out.entities == 1
    assert out.relations == 1
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_build_property_graph.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

`src/workflow/activities/build_property_graph.py`:
```python
"""`build_property_graph` — Chunk + MENTIONS + entity/relation upsert."""

from __future__ import annotations

import asyncio

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from loguru import logger
from temporalio import activity

from src.graph.index import NoOpKGExtractor, build_property_graph_index
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.workflow.contracts import GraphBuilt, Merged
from src.workflow.staging import build_staging_store

_NEO4J_UNSAFE_METADATA_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
})
_PRESERVE_METADATA_KEYS: frozenset[str] = frozenset({
    KG_NODES_KEY, KG_RELATIONS_KEY,
})


def _is_neo4j_safe(value) -> bool:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(
            v is None or isinstance(v, (str, int, float, bool))
            for v in value
        )
    return False


def _strip_neo4j_unsafe_metadata(nodes) -> None:
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not md:
            continue
        for key in list(md.keys()):
            if key in _PRESERVE_METADATA_KEYS:
                continue
            if key in _NEO4J_UNSAFE_METADATA_KEYS or not _is_neo4j_safe(md[key]):
                md.pop(key, None)


@activity.defn
async def build_property_graph(merged: Merged) -> GraphBuilt:
    staging = build_staging_store()
    entities, relations, nodes = staging.read_pickle(merged.merged_entities_uri)

    graph_store = build_neo4j_graph_store()
    embed_model = build_embedding_model()

    _strip_neo4j_unsafe_metadata(nodes)
    await asyncio.to_thread(
        build_property_graph_index,
        graph_store=graph_store,
        embed_model=embed_model,
        extractor=NoOpKGExtractor(),
        nodes=nodes,
    )
    if entities:
        graph_store.upsert_nodes(entities)
    if relations:
        graph_store.upsert_relations(relations)
    logger.info(
        "build_property_graph done  doc={d}  e={e}  r={r}",
        d=merged.kg.parsed.ctx.doc_id, e=len(entities), r=len(relations),
    )
    return GraphBuilt(entities=len(entities), relations=len(relations))
```

- [ ] **Step 4: Wire export + run + commit**

```bash
uv run pytest tests/test_workflow/test_build_property_graph.py -v
git add src/workflow/activities/build_property_graph.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_build_property_graph.py
git commit -m "feat(workflow): build_property_graph activity"
```

---

### Task 11: `finalize` + `mark_failed` activities

**Files:**
- Create: `src/workflow/activities/finalize.py`
- Modify: `src/workflow/activities/__init__.py`
- Create: `tests/test_workflow/test_finalize.py`

- [ ] **Step 1: Write failing tests for both**

`tests/test_workflow/test_finalize.py`:

```python
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.finalize import finalize, mark_failed
from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    IngestParams,
    Indexed,
    MarkFailedIn,
)


@pytest.mark.asyncio
async def test_finalize_writes_completed_and_cleans(tmp_path):
    cleanup_dir = tmp_path / "cache" / "doc-1"
    cleanup_dir.mkdir(parents=True)
    (cleanup_dir / "x.pdf").write_bytes(b"x")

    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()

    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path=str(cleanup_dir / "x.pdf"),
        cleanup_dir=str(cleanup_dir),
        workflow_run_id="run-x",
    )
    indexed = Indexed(node_ids=["a", "b"], count=2)
    fin = FinalizeIn(ctx=ctx, indexed=indexed, graph_status="completed")

    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ):
        out = await finalize(fin)

    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="completed",
    )
    staging.delete_prefix.assert_called_once_with("run-x")
    assert not cleanup_dir.exists()
    assert out.graph_status == "completed"
    assert out.chunk_count == 2


@pytest.mark.asyncio
async def test_finalize_writes_vector_only_status():
    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()
    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path="/tmp/x.pdf", cleanup_dir=None, workflow_run_id="run-y",
    )
    fin = FinalizeIn(
        ctx=ctx, indexed=Indexed(node_ids=[], count=0),
        graph_status="vector_only",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ):
        out = await finalize(fin)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="vector_only",
    )
    assert out.graph_status == "vector_only"


@pytest.mark.asyncio
async def test_mark_failed_with_ctx(tmp_path):
    cleanup_dir = tmp_path / "doc"
    cleanup_dir.mkdir()
    (cleanup_dir / "x.pdf").write_bytes(b"x")

    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()
    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path=str(cleanup_dir / "x.pdf"),
        cleanup_dir=str(cleanup_dir),
        workflow_run_id="run-z",
    )
    payload = MarkFailedIn(
        ctx=ctx,
        params=IngestParams(doc_id=ctx.doc_id, path="s3://kb-uploads/x"),
        error="boom",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ):
        await mark_failed(payload)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="failed", error="boom",
    )
    staging.delete_prefix.assert_called_once_with("run-z")
    assert not cleanup_dir.exists()


@pytest.mark.asyncio
async def test_mark_failed_without_ctx_still_writes_pg():
    """fetch_source crashed before producing a Ctx — mark_failed must
    still write `failed` status by using params.doc_id."""
    pg = MagicMock()
    pg.update_status = AsyncMock()
    payload = MarkFailedIn(
        ctx=None,
        params=IngestParams(
            doc_id="11111111-1111-1111-1111-111111111111",
            path="s3://kb-uploads/x",
        ),
        error="boom",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=MagicMock(),
    ):
        await mark_failed(payload)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(payload.params.doc_id), status="failed", error="boom",
    )
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_finalize.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement both**

`src/workflow/activities/finalize.py`:
```python
"""`finalize` (success path) and `mark_failed` (workflow-level
on-failure) — write Postgres terminal status + cleanup MinIO staging
+ remove local download dir.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from loguru import logger
from temporalio import activity

from src.storage.postgres import AsyncPostgres
from src.workflow.contracts import FinalizeIn, IngestResult, MarkFailedIn
from src.workflow.staging import build_staging_store


def _rmtree(path: str | None) -> None:
    if not path:
        return
    p = Path(path)
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)


@activity.defn
async def finalize(payload: FinalizeIn) -> IngestResult:
    pg = AsyncPostgres()
    await pg.update_status(
        uuid.UUID(payload.ctx.doc_id), status=payload.graph_status,
    )
    staging = build_staging_store()
    staging.delete_prefix(payload.ctx.workflow_run_id)
    _rmtree(payload.ctx.cleanup_dir)
    logger.info(
        "finalize  doc={d}  status={s}  chunks={c}",
        d=payload.ctx.doc_id, s=payload.graph_status,
        c=payload.indexed.count,
    )
    return IngestResult(
        doc_id=payload.ctx.doc_id,
        chunk_count=payload.indexed.count,
        graph_status=payload.graph_status,
    )


@activity.defn
async def mark_failed(payload: MarkFailedIn) -> None:
    doc_id = payload.ctx.doc_id if payload.ctx else payload.params.doc_id
    pg = AsyncPostgres()
    await pg.update_status(uuid.UUID(doc_id), status="failed", error=payload.error)
    staging = build_staging_store()
    if payload.ctx:
        staging.delete_prefix(payload.ctx.workflow_run_id)
        _rmtree(payload.ctx.cleanup_dir)
    logger.warning(
        "mark_failed  doc={d}  error={e}", d=doc_id, e=payload.error,
    )
```

- [ ] **Step 4: Wire exports**

Final `src/workflow/activities/__init__.py`:
```python
"""Activity functions invoked by `DocumentIngestWorkflow`."""

from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.activities.extract_kg import extract_kg
from src.workflow.activities.fetch_source import fetch_source
from src.workflow.activities.finalize import finalize, mark_failed
from src.workflow.activities.index_vector import index_vector
from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.activities.parse_and_chunk import parse_and_chunk

ALL_ACTIVITIES = [
    fetch_source,
    parse_and_chunk,
    index_vector,
    inject_canonical,
    extract_kg,
    merge_and_resolve,
    build_property_graph,
    finalize,
    mark_failed,
]

__all__ = [
    "ALL_ACTIVITIES",
    "build_property_graph",
    "extract_kg",
    "fetch_source",
    "finalize",
    "index_vector",
    "inject_canonical",
    "mark_failed",
    "merge_and_resolve",
    "parse_and_chunk",
]
```

- [ ] **Step 5: Run + commit**

```bash
uv run pytest tests/test_workflow/ -v
git add src/workflow/activities/finalize.py \
        src/workflow/activities/__init__.py \
        tests/test_workflow/test_finalize.py
git commit -m "feat(workflow): finalize + mark_failed activities"
```

---

**🛑 STAGE 2 GATE — pause for user sync.** Confirm: `uv run pytest tests/test_workflow/ -v` green (~24 tests), no behaviour change in API or worker yet.

---

## Stage 3 — Workflow + worker

### Task 12: `DocumentIngestWorkflow` + workflow-level tests

**Files:**
- Create: `src/workflow/document_ingest.py`
- Create: `tests/test_workflow/test_document_ingest_workflow.py`

- [ ] **Step 1: Write failing workflow tests using `WorkflowEnvironment.start_time_skipping()`**

`tests/test_workflow/test_document_ingest_workflow.py`:

```python
"""Workflow-level tests with mocked activities.

`start_time_skipping()` advances Temporal's clock without real
sleep, so retry policies are exercised in milliseconds.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    GraphBuilt,
    Indexed,
    IngestParams,
    IngestResult,
    Injected,
    KGExtracted,
    MarkFailedIn,
    Merged,
    Parsed,
)
from src.workflow.document_ingest import DocumentIngestWorkflow


# ── canned activity stubs ─────────────────────────────────────────


@activity.defn(name="fetch_source")
async def fetch_source_stub(params: IngestParams) -> Ctx:
    return Ctx(
        doc_id=params.doc_id, local_path="/tmp/x", cleanup_dir=None,
        workflow_run_id="run-test",
    )


@activity.defn(name="parse_and_chunk")
async def parse_and_chunk_stub(ctx: Ctx) -> Parsed:
    return Parsed(ctx=ctx, nodes_uri="s3://kb-staging/run-test/parsed.pkl",
                  chunk_count=3)


@activity.defn(name="index_vector")
async def index_vector_stub(parsed: Parsed) -> Indexed:
    return Indexed(node_ids=["a", "b", "c"], count=3)


@activity.defn(name="inject_canonical")
async def inject_canonical_stub(parsed: Parsed) -> Injected:
    return Injected(count=parsed.chunk_count)


@activity.defn(name="extract_kg")
async def extract_kg_stub(parsed: Parsed) -> KGExtracted:
    return KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/run-test/kg.pkl")


@activity.defn(name="merge_and_resolve")
async def merge_and_resolve_stub(kg: KGExtracted) -> Merged:
    return Merged(kg=kg, merged_entities_uri="s3://kb-staging/run-test/merged.pkl")


@activity.defn(name="build_property_graph")
async def build_pg_stub(merged: Merged) -> GraphBuilt:
    return GraphBuilt(entities=2, relations=1)


@activity.defn(name="finalize")
async def finalize_stub(payload: FinalizeIn) -> IngestResult:
    return IngestResult(
        doc_id=payload.ctx.doc_id, chunk_count=payload.indexed.count,
        graph_status=payload.graph_status,
    )


@activity.defn(name="mark_failed")
async def mark_failed_stub(payload: MarkFailedIn) -> None:
    return None


HAPPY_ACTIVITIES = [
    fetch_source_stub, parse_and_chunk_stub, index_vector_stub,
    inject_canonical_stub, extract_kg_stub, merge_and_resolve_stub,
    build_pg_stub, finalize_stub, mark_failed_stub,
]


# ── tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_happy_path_completed():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="t",
            workflows=[DocumentIngestWorkflow],
            activities=HAPPY_ACTIVITIES,
        ):
            params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
            result = await env.client.execute_workflow(
                DocumentIngestWorkflow.run, params,
                id=f"ingest-{params.doc_id}", task_queue="t",
            )
    assert result.graph_status == "completed"
    assert result.chunk_count == 3


@pytest.mark.asyncio
async def test_graph_failure_downgrades_to_vector_only():
    @activity.defn(name="extract_kg")
    async def boom(parsed: Parsed) -> KGExtracted:
        raise ApplicationError("LLM 503", non_retryable=True)

    activities = [
        fetch_source_stub, parse_and_chunk_stub, index_vector_stub,
        inject_canonical_stub, boom, merge_and_resolve_stub,
        build_pg_stub, finalize_stub, mark_failed_stub,
    ]

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="t",
            workflows=[DocumentIngestWorkflow], activities=activities,
        ):
            params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
            result = await env.client.execute_workflow(
                DocumentIngestWorkflow.run, params,
                id=f"ingest-{params.doc_id}", task_queue="t",
            )
    assert result.graph_status == "vector_only"


@pytest.mark.asyncio
async def test_vector_failure_runs_mark_failed_and_raises():
    mark_failed_calls: list[MarkFailedIn] = []

    @activity.defn(name="mark_failed")
    async def record_failure(payload: MarkFailedIn) -> None:
        mark_failed_calls.append(payload)

    @activity.defn(name="index_vector")
    async def boom(parsed: Parsed) -> Indexed:
        raise ApplicationError("milvus down", non_retryable=True)

    activities = [
        fetch_source_stub, parse_and_chunk_stub, boom,
        inject_canonical_stub, extract_kg_stub, merge_and_resolve_stub,
        build_pg_stub, finalize_stub, record_failure,
    ]

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="t",
            workflows=[DocumentIngestWorkflow], activities=activities,
        ):
            params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
            with pytest.raises(Exception):
                await env.client.execute_workflow(
                    DocumentIngestWorkflow.run, params,
                    id=f"ingest-{params.doc_id}", task_queue="t",
                )

    assert len(mark_failed_calls) == 1
    assert mark_failed_calls[0].params.doc_id == params.doc_id
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_document_ingest_workflow.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement workflow**

`src/workflow/document_ingest.py`:
```python
"""`DocumentIngestWorkflow` — composes 8 activities + best-effort
graph half and on-failure mark_failed.

Outer `try/except ActivityError` covers the vector half: any non-graph
activity that exhausts retries triggers `mark_failed` then re-raises
so Temporal records the workflow as failed.  The inner `try/except`
makes the four graph activities best-effort.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        Ctx,
        FinalizeIn,
        GraphBuilt,
        Indexed,
        IngestParams,
        IngestResult,
        Injected,
        KGExtracted,
        MarkFailedIn,
        Merged,
        Parsed,
    )


_DEFAULT_RETRY = RetryPolicy(maximum_attempts=3)
_GRAPH_HEAVY_RETRY = RetryPolicy(
    maximum_attempts=2,
    initial_interval=timedelta(minutes=2),
)
_FAST_RETRY = RetryPolicy(maximum_attempts=5)


@workflow.defn
class DocumentIngestWorkflow:
    @workflow.run
    async def run(self, params: IngestParams) -> IngestResult:
        ctx: Ctx | None = None
        try:
            ctx = await workflow.execute_activity(
                "fetch_source", params,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_DEFAULT_RETRY,
            )
            parsed: Parsed = await workflow.execute_activity(
                "parse_and_chunk", ctx,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_DEFAULT_RETRY,
            )
            indexed: Indexed = await workflow.execute_activity(
                "index_vector", parsed,
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=_DEFAULT_RETRY,
            )

            graph_status: str = "completed"
            try:
                injected: Injected = await workflow.execute_activity(
                    "inject_canonical", parsed,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=_FAST_RETRY,
                )
                workflow.logger.info(
                    "inject_canonical done count=%d", injected.count,
                )
                kg: KGExtracted = await workflow.execute_activity(
                    "extract_kg", parsed,
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=_GRAPH_HEAVY_RETRY,
                )
                merged: Merged = await workflow.execute_activity(
                    "merge_and_resolve", kg,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                built: GraphBuilt = await workflow.execute_activity(
                    "build_property_graph", merged,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                workflow.logger.info(
                    "graph built entities=%d relations=%d",
                    built.entities, built.relations,
                )
            except ActivityError as exc:
                workflow.logger.warning(
                    "graph stage failed, continuing: %s", exc,
                )
                graph_status = "vector_only"

            return await workflow.execute_activity(
                "finalize",
                FinalizeIn(ctx=ctx, indexed=indexed, graph_status=graph_status),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
        except ActivityError as exc:
            # Vector-half failure or other terminal failure outside the
            # graph try.  Run mark_failed compensation then re-raise.
            await workflow.execute_activity(
                "mark_failed",
                MarkFailedIn(ctx=ctx, params=params, error=str(exc)),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
            raise
```

- [ ] **Step 4: Run, verify pass**

Run: `uv run pytest tests/test_workflow/test_document_ingest_workflow.py -v`
Expected: 3 passing tests (~10-20 s with time-skipping).

- [ ] **Step 5: Commit**

```bash
git add src/workflow/document_ingest.py \
        tests/test_workflow/test_document_ingest_workflow.py
git commit -m "feat(workflow): DocumentIngestWorkflow with happy/vector_only/failed paths"
```

---

### Task 13: Worker entrypoint + Temporal client helper

**Files:**
- Create: `src/workflow/client.py`
- Create: `src/workflow/worker.py`
- Create: `tests/test_workflow/test_client.py`

- [ ] **Step 1: Write failing test for `get_temporal_client`**

`tests/test_workflow/test_client.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.client import get_temporal_client


@pytest.mark.asyncio
async def test_get_temporal_client_uses_settings():
    fake_client = MagicMock()
    fake_settings = MagicMock()
    fake_settings.temporal.target = "host:7233"
    fake_settings.temporal.namespace = "default"

    with patch(
        "src.workflow.client.Client.connect", new=AsyncMock(return_value=fake_client),
    ) as mock_connect, patch(
        "src.workflow.client.settings", fake_settings,
    ):
        client = await get_temporal_client()

    mock_connect.assert_awaited_once_with("host:7233", namespace="default")
    assert client is fake_client


@pytest.mark.asyncio
async def test_get_temporal_client_caches_instance():
    fake_client = MagicMock()
    fake_settings = MagicMock()
    fake_settings.temporal.target = "h:1"
    fake_settings.temporal.namespace = "n"

    import src.workflow.client as mod
    mod._client_singleton = None

    with patch(
        "src.workflow.client.Client.connect", new=AsyncMock(return_value=fake_client),
    ) as mock_connect, patch(
        "src.workflow.client.settings", fake_settings,
    ):
        a = await get_temporal_client()
        b = await get_temporal_client()

    assert a is b
    mock_connect.assert_awaited_once()
```

- [ ] **Step 2: Verify ImportError**

Run: `uv run pytest tests/test_workflow/test_client.py -v`
Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement client + worker**

`src/workflow/client.py`:
```python
"""Process-wide Temporal client singleton.

FastAPI handlers and CLI entry points call `get_temporal_client()`;
the connection is opened once and reused across requests.
"""

from __future__ import annotations

from temporalio.client import Client

from src.config import settings

_client_singleton: Client | None = None


async def get_temporal_client() -> Client:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = await Client.connect(
            settings.temporal.target,
            namespace=settings.temporal.namespace,
        )
    return _client_singleton
```

`src/workflow/worker.py`:
```python
"""Temporal worker entry point.

Run with:
    uv run python -m src.workflow.worker

Polls the workflow + activity task queues and registers
`DocumentIngestWorkflow` plus every activity exported from
`src.workflow.activities`.
"""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import ALL_ACTIVITIES
from src.workflow.client import get_temporal_client
from src.workflow.document_ingest import DocumentIngestWorkflow


async def _run() -> None:
    client = await get_temporal_client()
    logger.info(
        "temporal worker  target={t}  queue={q}  concurrency={c}",
        t=settings.temporal.target, q=settings.temporal.task_queue,
        c=settings.temporal.activity_concurrency,
    )
    worker = Worker(
        client,
        task_queue=settings.temporal.task_queue,
        workflows=[DocumentIngestWorkflow],
        activities=ALL_ACTIVITIES,
        max_concurrent_activities=settings.temporal.activity_concurrency,
    )
    await worker.run()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run + commit**

```bash
uv run pytest tests/test_workflow/test_client.py -v
git add src/workflow/client.py src/workflow/worker.py \
        tests/test_workflow/test_client.py
git commit -m "feat(workflow): Temporal client singleton + worker entry point"
```

---

**🛑 STAGE 3 GATE — pause for user sync.** Confirm: `uv run pytest tests/test_workflow/ -v` green (~29 tests). Manually verify the worker starts:
```bash
docker compose up -d temporal temporal-ui postgres
uv run python -m src.workflow.worker &
WORKER_PID=$!
sleep 5
curl -fsS http://localhost:8080/api/v1/cluster-info -o /dev/null && echo "temporal ok"
kill $WORKER_PID
```

---

## Stage 4 — Integration test

### Task 14: Live workflow integration test

**Files:**
- Create: `tests/test_workflow/test_workflow_local.py`

- [ ] **Step 1: Write integration test that runs the real activities against `WorkflowEnvironment.start_local()` + live infra**

`tests/test_workflow/test_workflow_local.py`:

```python
"""End-to-end test using `WorkflowEnvironment.start_local()`.

Spawns a real Temporalite server in-process and registers the real
activities against live Milvus + Neo4j + MinIO + Postgres from
`docker-compose`.  Skipped when those services aren't reachable.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from src.workflow.activities import ALL_ACTIVITIES
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not all([
        _port_open("localhost", 19530),  # milvus
        _port_open("localhost", 7687),   # neo4j
        _port_open("localhost", 9000),   # minio
        _port_open("localhost", 5432),   # postgres
    ]),
    reason="live infra (milvus/neo4j/minio/postgres) not reachable",
)


@pytest.mark.asyncio
async def test_full_pipeline_happy_path(tmp_path: Path):
    fixture = Path(__file__).parent.parent / "test_ingestion" / "fixtures"
    candidates = list(fixture.glob("*.txt")) + list(fixture.glob("*.md"))
    if not candidates:
        pytest.skip("no small fixture document available")
    source = candidates[0]

    # Insert a documents row by hand (API normally does this).
    import psycopg
    from src.config import settings as cfg
    doc_id = uuid.uuid4()
    with psycopg.connect(cfg.postgres.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO documents (id, path, status) VALUES (%s, %s, 'pending')",
                (str(doc_id), str(source)),
            )
        conn.commit()

    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client, task_queue="kb-ingest-it",
            workflows=[DocumentIngestWorkflow],
            activities=ALL_ACTIVITIES,
            max_concurrent_activities=2,
        ):
            params = IngestParams(doc_id=str(doc_id), path=str(source))
            result = await env.client.execute_workflow(
                DocumentIngestWorkflow.run, params,
                id=f"ingest-{doc_id}", task_queue="kb-ingest-it",
            )

    assert result.doc_id == str(doc_id)
    assert result.graph_status in ("completed", "vector_only")
    assert result.chunk_count > 0

    # Verify Postgres terminal state.
    with psycopg.connect(cfg.postgres.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM documents WHERE id = %s", (str(doc_id),))
            (status,) = cur.fetchone()
    assert status in ("completed", "vector_only")
```

- [ ] **Step 2: Run with infra up**

```bash
docker compose up -d
uv run pytest tests/test_workflow/test_workflow_local.py -v -s
```
Expected: 1 passing test (or skipped if a service is down) within ~3 min. Document at `tests/test_ingestion/fixtures/` is ingested end-to-end.

- [ ] **Step 3: Commit**

```bash
git add tests/test_workflow/test_workflow_local.py
git commit -m "test(workflow): live end-to-end test via WorkflowEnvironment.start_local"
```

---

**🛑 STAGE 4 GATE — pause for user sync.** Confirm live happy-path test passes locally. Spot-check Temporal Web UI at http://localhost:8080 to see the workflow + activity timeline.

---

## Stage 5 — Cutover

### Task 15: Convert `process_document` taskiq task to thin shim

**Files:**
- Modify: `src/ingestion/tasks.py`
- Create: `tests/test_workflow/test_taskiq_shim.py`

The shim preserves existing call sites (none external, but the API still imports it) while the body lives in activities.

- [ ] **Step 1: Write failing test for shim**

`tests/test_workflow/test_taskiq_shim.py`:

```python
"""The legacy `process_document` taskiq task is now a thin shim that
starts the Temporal workflow and waits for it.  Verifies the call
shape; no Temporal infra needed."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.ingestion.tasks import process_document


@pytest.mark.asyncio
async def test_process_document_starts_workflow():
    handle = MagicMock()
    handle.result = AsyncMock(return_value=MagicMock(doc_id="d"))
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)

    doc_id = str(uuid.uuid4())
    with patch(
        "src.ingestion.tasks.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        await process_document(doc_id, "s3://kb-uploads/x/y.pdf")

    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    # First positional arg is the workflow run callable name.
    assert "DocumentIngestWorkflow" in str(call.args[0]) or call.kwargs.get(
        "id", "",
    ).startswith("ingest-")
    handle.result.assert_awaited_once()
```

- [ ] **Step 2: Replace `process_document` body with shim**

In `src/ingestion/tasks.py`, replace the existing `@broker.task` block at the bottom of the file with:

```python
from src.workflow.client import get_temporal_client
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow


@broker.task
async def process_document(doc_id: str, path: str) -> None:
    """Legacy entry point — now starts the Temporal workflow.

    Kept so callers that still import `process_document` keep working
    during the cutover.  The original body has moved to activities
    under `src.workflow.activities`.
    """
    client = await get_temporal_client()
    handle = await client.start_workflow(
        DocumentIngestWorkflow.run,
        IngestParams(doc_id=doc_id, path=path),
        id=f"ingest-{doc_id}",
        task_queue=settings.temporal.task_queue,
    )
    await handle.result()
```

Keep `broker` declaration + the helper functions (`_resolve_source_path`, `_consolidate_phone_entities`, etc.) — `merge_and_resolve` imports `_consolidate_phone_entities` from here.

- [ ] **Step 3: Update imports at top of file**

Ensure `from src.config import settings` is still present. Remove now-unused imports that were only referenced by the removed body (Postgres helpers, MinIO download helper, ingestion-pipeline imports, etc.). Keep the helpers and what `merge_and_resolve` re-imports.

Specifically:
- KEEP: `from src.config import settings`, `from loguru import logger`, `from taskiq_aio_pika import AioPikaBroker`, `_consolidate_phone_entities` and its imports (phonenumbers, EntityNode/Relation), the new shim imports.
- REMOVE: `import asyncio`, `import shutil`, `import uuid`, `from pathlib import Path` if no longer used; the ingest-pipeline / vector / graph / minio / postgres builders that the old body called.

After cleanup, `src/ingestion/tasks.py` shrinks to: broker, helper `_consolidate_phone_entities` (and `_resolve_source_path` if still used by `tests/test_ingestion/test_tasks_minio.py`), shim task. About 200 lines instead of 600.

- [ ] **Step 4: Run tests + commit**

```bash
uv run pytest tests/test_ingestion/test_tasks_minio.py tests/test_workflow/ -v
git add src/ingestion/tasks.py tests/test_workflow/test_taskiq_shim.py
git commit -m "refactor(ingestion): process_document becomes thin Temporal workflow shim"
```

---

### Task 16: API switch — `/api/v1/ingest` calls Temporal directly

**Files:**
- Modify: `src/api/routes/ingest.py`
- Modify: `tests/test_api/test_ingest_endpoint.py` (existing) or create if missing

- [ ] **Step 1: Update tests**

Identify the existing endpoint test (`grep -rl "process_document" tests/test_api/`). Replace the assertion that `process_document.kiq` was awaited with one that `get_temporal_client().start_workflow` was awaited.

Sketch:

```python
@pytest.mark.asyncio
async def test_ingest_starts_temporal_workflow(client, monkeypatch):
    fake_handle = MagicMock()
    fake_handle.id = "ingest-abc"
    fake_client = MagicMock()
    fake_client.start_workflow = AsyncMock(return_value=fake_handle)

    async def _get():
        return fake_client

    monkeypatch.setattr("src.api.routes.ingest.get_temporal_client", _get)
    # ... existing MinIO + PG mocks
    response = await client.post(
        "/api/v1/ingest", files={"file": ("x.txt", b"hello", "text/plain")},
        headers={"X-API-Key": "test"},
    )
    assert response.status_code == 202
    fake_client.start_workflow.assert_awaited_once()
```

Run: `uv run pytest tests/test_api/ -v` → endpoint test should fail until step 2.

- [ ] **Step 2: Update endpoint**

In `src/api/routes/ingest.py`, replace:

```python
from src.ingestion.tasks import process_document
# ...
    await process_document.kiq(str(doc_id), s3_uri)
```

with:

```python
from src.config import settings
from src.workflow.client import get_temporal_client
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow
# ...
    client = await get_temporal_client()
    await client.start_workflow(
        DocumentIngestWorkflow.run,
        IngestParams(doc_id=str(doc_id), path=s3_uri),
        id=f"ingest-{doc_id}",
        task_queue=settings.temporal.task_queue,
    )
```

The endpoint still returns `IngestEnqueuedResponse(job_id=doc_id)` — workflow ID is derivable as `ingest-{job_id}` so we don't need to extend the response schema.

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_api/ tests/test_workflow/ -v
git add src/api/routes/ingest.py tests/test_api/
git commit -m "feat(api): /ingest starts Temporal workflow directly"
```

---

### Task 17: Remove taskiq / RabbitMQ infra

**Files:**
- Modify: `pyproject.toml`
- Modify: `docker-compose.yml`
- Modify: `src/config.py`
- Modify: `.env.example`
- Delete: `src/ingestion/tasks.py` (replace with leaner module without broker)
- Modify: tests referring to `broker` / `taskiq`

- [ ] **Step 1: Find all taskiq references**

Run:
```bash
grep -rn "taskiq\|AioPikaBroker\|RabbitMQSettings\|\.kiq(\b" src/ tests/ docker-compose.yml pyproject.toml | sort
```

Inventory everything that has to move.

- [ ] **Step 2: Move `_consolidate_phone_entities` into the graph package**

The function is graph-side logic. Create `src/graph/phone_consolidation.py` with the body of `_consolidate_phone_entities` (verbatim from `src/ingestion/tasks.py`). Update `src/workflow/activities/merge_and_resolve.py` to import from the new location:

```python
from src.graph.phone_consolidation import consolidate_phone_entities
```

(Drop the leading underscore on the public name — it is now exported.)

Update tests that import `_consolidate_phone_entities` from `src.ingestion.tasks` to import `consolidate_phone_entities` from `src.graph.phone_consolidation`.

- [ ] **Step 3: Delete the old tasks module + taskiq broker**

Delete `src/ingestion/tasks.py`. If `_resolve_source_path` had its own test (`tests/test_ingestion/test_tasks_minio.py`) and the function is now obsolete (replaced by `fetch_source` activity), delete the test file too. Otherwise move the helper into `src/workflow/activities/fetch_source.py` as a private function and update the test to import from there.

- [ ] **Step 4: Drop taskiq + RabbitMQ from config and compose**

In `src/config.py`:
- Delete `class RabbitMQSettings` and its registration in `Settings`.
- Remove `"RabbitMQSettings"` from `__all__`.

In `docker-compose.yml`:
- Delete the `rabbitmq:` service block and `rabbitmq_data:` volume.

In `pyproject.toml`:
- Delete the `taskiq` and `taskiq-aio-pika` dependency lines.

In `.env.example`:
- Delete the `RABBITMQ_*` lines.

- [ ] **Step 5: Resync lock + tests**

```bash
uv sync
docker compose down
docker compose up -d
uv run pytest -v
```
Expected: full suite passes (count should be roughly the same as before, minus any taskiq-specific tests).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: remove taskiq + RabbitMQ now that ingest runs on Temporal"
```

---

**🛑 STAGE 5 GATE — pause for user sync.** Confirm full suite green, `docker compose ps` shows no rabbitmq, Temporal Web UI shows recent workflow runs from manual smoke testing.

---

## Self-Review

**Spec coverage check:**
- Section 2 (Scope) — taskiq/rabbitmq removal: Tasks 15-17. Temporal compose: Task 1. `src/workflow/` package: Tasks 2-13. API switch: Task 16. Three test levels: unit (Tasks 4-11), workflow (Task 12), integration (Task 14). Migration plan: Tasks 15-17. ✅
- Section 3 (Architecture) — staging-blob naming, worker layout, claim-check: Tasks 2, 3, 13. ✅
- Section 4 (Workflow definition) — `try/except ActivityError` outer + inner: Task 12 implementation. ✅
- Section 5 (Activities table) — each row implemented and tested: Tasks 4-11. ✅
- Section 6 (Contracts) — Pydantic v2 frozen models: Task 2. ✅
- Section 7 (Failure semantics) — `ApplicationError(non_retryable)`, retry policies, on-failure `mark_failed`: Tasks 11, 12. ✅
- Section 8 (Idempotency) — `fetch_source` cache hit, Milvus PK reuse, Neo4j MERGE: Task 4 test + activity body in Tasks 6-10. `WorkflowIdReusePolicy` left for runtime config; not required at the task level — covered implicitly via `id=f"ingest-{doc_id}"`. Force re-ingest API surface is **deferred** — flagged in Section 12 follow-ups; not in this plan.
- Section 9 (Observability) — Temporal Web UI in Task 1, structlog/loguru log lines included throughout activities. LangFuse spans live inside `build_llm` calls already.
- Section 10 (Testing) — three levels covered as above.
- Section 11 (Migration steps 1-4) — Stages 1-3 = step 1 (additive); Task 15 = step 2 (shim); Task 16 = step 3 (API switch); Task 17 = step 4 (cleanup). ✅
- Section 12 (Open questions) — explicitly carried forward as follow-ups, none required for this plan.

**Placeholder scan:** No "TBD", "TODO", "fill in details", or "similar to Task N". Each step has the actual code, file path, or command.

**Type consistency:** Contract names (Ctx, Parsed, Indexed, Injected, KGExtracted, Merged, GraphBuilt, FinalizeIn, MarkFailedIn, IngestResult) are used identically in tests, activities, and workflow. Activity names are passed as strings (`"fetch_source"`, etc.) into `workflow.execute_activity` and registered with matching `@activity.defn(name=...)` in tests where stubs override.

One intentional asymmetry: `_consolidate_phone_entities` is imported from `src.ingestion.tasks` in Task 9 and renamed/relocated to `src.graph.phone_consolidation` as `consolidate_phone_entities` in Task 17 step 2. This is sequenced on purpose — Task 9 lands before the relocation so Stage 2 stays a pure additive lift.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-15-ingest-temporal-workflow.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**

# Source Document Download + Search Document Links — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /api/v1/documents/{doc_id}` to stream the original uploaded file from MinIO, and return `documents[]` (links to the source files used) in every search response.

**Architecture:** Originals live in MinIO at `s3://{bucket}/{doc_id}/{filename}` (Postgres `documents.path` holds the URI). A new `documents` route streams them. Search responses gain a deduped `documents[]`: local/drift derive doc_ids from `sources[].metadata["doc_id"]`; global maps surviving community_ids → source docs via a fail-open graph activity.

**Tech Stack:** FastAPI (`StreamingResponse`), MinIO Python SDK, Temporal, Pydantic v2, pytest + httpx `ASGITransport`. Spec: `docs/superpowers/specs/2026-06-02-source-document-download-design.md`.

---

## File Structure

- `src/storage/minio.py` — **modify**: add `stat_object`, `stream_object`.
- `src/api/routes/documents.py` — **create**: the download route.
- `src/api/main.py` — **modify**: mount the router.
- `src/models/search.py` — **modify**: `DocumentRef` + `SearchResponse.documents`.
- `src/workflow/contracts.py` — **modify**: `SearchOutcome.documents`, `DocumentsForCommunitiesParams`, `DocumentsForCommunitiesResult`.
- `src/api/routes/search_v2.py` — **modify**: map `outcome.documents` → response.
- `src/workflow/search/orchestrator.py` — **modify**: collect local doc_ids.
- `src/workflow/search/activities/documents.py` — **create**: `documents_for_communities` activity.
- `src/workflow/search/activities/__init__.py` — **modify**: register the activity.
- `src/workflow/search/global_wf.py` — **modify**: call the activity, attach docs.
- `src/workflow/search/router_wf.py` — **modify**: drift union.
- Tests: `tests/test_storage/test_minio_stream.py`, `tests/test_api/test_documents.py`, plus additions to existing `test_api`/`test_workflow` test files.

---

## PART A — Download endpoint

### Task 1: `MinioStorage.stat_object` + `stream_object`

**Files:**
- Modify: `src/storage/minio.py`
- Test: `tests/test_storage/test_minio_stream.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_storage/test_minio_stream.py`:
```python
"""Unit tests for MinioStorage stat/stream (stub minio client)."""

from __future__ import annotations

from types import SimpleNamespace

from src.config import settings
from src.storage.minio import MinioStorage


class _FakeResp:
    def __init__(self, chunks):
        self._chunks = chunks
        self.closed = False
        self.released = False

    def stream(self, n):
        yield from self._chunks

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class _FakeClient:
    def __init__(self, resp):
        self._resp = resp
        self.stat_args = None
        self.get_args = None

    def stat_object(self, bucket, key):
        self.stat_args = (bucket, key)
        return SimpleNamespace(size=11, content_type="application/pdf")

    def get_object(self, bucket, key):
        self.get_args = (bucket, key)
        return self._resp


def _storage(client):
    s = MinioStorage(settings.minio)
    s._client = client
    return s


def test_stat_object_returns_name_size_type():
    s = _storage(_FakeClient(_FakeResp([])))
    name, size, ctype = s.stat_object("s3://b/doc-1/report.pdf")
    assert name == "report.pdf"
    assert size == 11
    assert ctype == "application/pdf"
    assert s._client.stat_args == ("b", "doc-1/report.pdf")


def test_stream_object_yields_and_releases():
    resp = _FakeResp([b"hello ", b"world"])
    s = _storage(_FakeClient(resp))
    out = b"".join(s.stream_object("s3://b/doc-1/report.pdf"))
    assert out == b"hello world"
    assert resp.closed and resp.released  # connection released in finally
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_storage/test_minio_stream.py -q`
Expected: FAIL — `AttributeError: 'MinioStorage' object has no attribute 'stat_object'`.

- [ ] **Step 3: Implement the methods**

In `src/storage/minio.py`, add `from typing import Iterator` to the imports, then add these methods to `MinioStorage` (after `get_object_to_path`):
```python
    def stat_object(self, s3_uri: str) -> tuple[str, int, str]:
        """Return (filename, size_bytes, content_type) for an s3:// object.

        filename is the last path segment of the key.  Raises ``S3Error``
        (NoSuchKey) when the object is missing — the caller maps that to 404."""
        bucket, key = self.parse_s3_uri(s3_uri)
        info = self._client.stat_object(bucket, key)
        filename = key.rsplit("/", 1)[-1] or key
        content_type = info.content_type or "application/octet-stream"
        return filename, info.size, content_type

    def stream_object(
        self, s3_uri: str, *, chunk_size: int = 1 << 20,
    ) -> Iterator[bytes]:
        """Yield the object's bytes in chunks, releasing the HTTP
        connection in a finally block (minio's get_object response must
        be closed + released or the pool leaks)."""
        bucket, key = self.parse_s3_uri(s3_uri)
        resp = self._client.get_object(bucket, key)
        try:
            yield from resp.stream(chunk_size)
        finally:
            resp.close()
            resp.release_conn()
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_storage/test_minio_stream.py -q`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/storage/minio.py tests/test_storage/test_minio_stream.py
git commit -m "feat(minio): add stat_object + stream_object for document download"
```

---

### Task 2: `GET /api/v1/documents/{doc_id}` route

**Files:**
- Create: `src/api/routes/documents.py`
- Modify: `src/api/main.py`
- Test: `tests/test_api/test_documents.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_api/test_documents.py`:
```python
"""ASGI tests for GET /api/v1/documents/{doc_id}."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _key() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


async def _get(path, *, headers=None):
    from src.api.main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        return await ac.get(path, headers=headers)


_DOC_ID = str(uuid.uuid4())


def _row(path):
    return SimpleNamespace(path=path)


def _storage():
    s = MagicMock()
    s.stat_object.return_value = ("report.pdf", 5, "application/pdf")
    s.stream_object.return_value = iter([b"hello"])
    return s


@pytest.mark.asyncio
async def test_download_streams_original():
    with patch("src.storage.postgres.AsyncPostgres.get",
               new=AsyncMock(return_value=_row(f"s3://b/{_DOC_ID}/report.pdf"))), \
         patch("src.api.routes.documents.build_minio_storage",
               return_value=_storage()):
        resp = await _get(f"/api/v1/documents/{_DOC_ID}", headers=_key())
    assert resp.status_code == 200, resp.text
    assert resp.content == b"hello"
    assert 'filename="report.pdf"' in resp.headers["content-disposition"]
    assert resp.headers["content-type"].startswith("application/pdf")


@pytest.mark.asyncio
async def test_download_unknown_doc_404():
    with patch("src.storage.postgres.AsyncPostgres.get",
               new=AsyncMock(return_value=None)):
        resp = await _get(f"/api/v1/documents/{_DOC_ID}", headers=_key())
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_download_requires_api_key():
    resp = await _get(f"/api/v1/documents/{_DOC_ID}")
    assert resp.status_code == 401
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_api/test_documents.py -q`
Expected: FAIL — 404/route-not-found (route not mounted yet).

- [ ] **Step 3: Create the route**

Create `src/api/routes/documents.py`:
```python
"""`GET /api/v1/documents/{doc_id}` — download the original source file.

Streams the original uploaded file from MinIO (the URI is stored in
Postgres `documents.path`).  doc_id is the value search responses expose
in `sources[]` / `documents[]`.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse, StreamingResponse
from loguru import logger

from src.api.auth import require_api_key
from src.storage.minio import S3Error, build_minio_storage
from src.storage.postgres import AsyncPostgres

router = APIRouter(tags=["documents"])


@router.get(
    "/documents/{doc_id}",
    dependencies=[Depends(require_api_key)],
    summary="Download the original source file of an ingested document",
)
@inject
async def download_document(doc_id: str, pg: FromDishka[AsyncPostgres]):
    try:
        row = await pg.get(uuid.UUID(doc_id))
    except (ValueError, TypeError):
        row = None
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "document not found")

    path = row.path
    # Legacy local-path docs (pre-MinIO): stream from disk if present.
    if not path.startswith("s3://"):
        local = Path(path)
        if local.is_file():
            return FileResponse(local, filename=local.name)
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "document source not available")

    try:
        storage = build_minio_storage()
        filename, size, content_type = storage.stat_object(path)
    except S3Error as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "document source not available") from exc
    except Exception as exc:  # noqa: BLE001 — MinIO unreachable etc.
        logger.exception("document download storage error doc_id={d}", d=doc_id)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "storage unavailable") from exc

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(size),
    }
    return StreamingResponse(
        storage.stream_object(path), media_type=content_type, headers=headers)
```

- [ ] **Step 4: Mount the router**

In `src/api/main.py`, add `documents` to the route imports (alongside `health, search_v2, ingest`) and mount it after the search router:
```python
app.include_router(documents.router, prefix="/api/v1")
```

- [ ] **Step 5: Run to verify it passes**

Run: `uv run pytest tests/test_api/test_documents.py -q`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/api/routes/documents.py src/api/main.py tests/test_api/test_documents.py
git commit -m "feat(api): GET /api/v1/documents/{doc_id} streams original file from MinIO"
```

---

## PART B — Document links in the search response

### Task 3: response/contract shapes + route mapping

**Files:**
- Modify: `src/models/search.py`, `src/workflow/contracts.py`, `src/api/routes/search_v2.py`
- Test: `tests/test_api/test_documents_in_response.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_api/test_documents_in_response.py`:
```python
"""The search response carries documents[] (links) built from outcome.documents."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings
from src.workflow.contracts import SearchOutcome


def _key():
    return {"X-API-Key": settings.api.keys_list[0]}


def _stub_client(outcome):
    handle = MagicMock()
    handle.result = AsyncMock(return_value=outcome)
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    return client


@pytest.mark.asyncio
async def test_to_document_refs_builds_urls():
    from src.api.routes.search_v2 import to_document_refs
    refs = to_document_refs(["d1", "d2"])
    assert [r.doc_id for r in refs] == ["d1", "d2"]
    assert refs[0].url == "/api/v1/documents/d1"


@pytest.mark.asyncio
async def test_local_response_has_documents():
    outcome = SearchOutcome(
        query="q", mode="local", answer="a", documents=["d1", "d2"], latency_ms=1)
    with patch("src.api.routes.search_v2.get_temporal_client",
               new=AsyncMock(return_value=_stub_client(outcome))):
        from src.api.main import app
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://test") as ac:
            resp = await ac.post("/api/v1/search/local",
                                 json={"query": "q"}, headers=_key())
    assert resp.status_code == 200, resp.text
    docs = resp.json()["documents"]
    assert {d["doc_id"] for d in docs} == {"d1", "d2"}
    assert docs[0]["url"] == f"/api/v1/documents/{docs[0]['doc_id']}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_api/test_documents_in_response.py -q`
Expected: FAIL — `ImportError: cannot import name 'to_document_refs'` / `SearchOutcome` has no `documents`.

- [ ] **Step 3: Add the contract + response fields**

In `src/workflow/contracts.py`, add to `SearchOutcome` (after `sources`):
```python
    documents: list[str] = Field(default_factory=list)
```

In `src/models/search.py`, add the `DocumentRef` class (before `SearchResponse`) and the field:
```python
class DocumentRef(BaseModel):
    doc_id: str
    url: str


# inside SearchResponse, after `sources`:
    documents: list[DocumentRef] = Field(default_factory=list)
```
Add `"DocumentRef"` to `__all__`.

- [ ] **Step 4: Add the helper + map in the route**

In `src/api/routes/search_v2.py`, import `DocumentRef` (add to the `from src.models.search import ...` line) and add the pure helper + wire it into `_outcome_to_response`:
```python
def to_document_refs(doc_ids: list[str]) -> list[DocumentRef]:
    """doc_id list → relative download links (preserves order)."""
    return [DocumentRef(doc_id=d, url=f"/api/v1/documents/{d}") for d in doc_ids]
```
In `_outcome_to_response`, add `documents=to_document_refs(outcome.documents)` to the `SearchResponse(...)` construction.

- [ ] **Step 5: Run to verify it passes**

Run: `uv run pytest tests/test_api/test_documents_in_response.py -q`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/workflow/contracts.py src/models/search.py src/api/routes/search_v2.py tests/test_api/test_documents_in_response.py
git commit -m "feat(search): documents[] links in SearchResponse (mapped from SearchOutcome.documents)"
```

---

### Task 4: local mode — collect doc_ids from the merged pool

**Files:**
- Modify: `src/workflow/search/orchestrator.py`
- Test: `tests/test_workflow/test_search_orchestrator_helpers.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_workflow/test_search_orchestrator_helpers.py`:
```python
def test_distinct_doc_ids_dedups_in_order():
    from src.workflow.search.orchestrator import distinct_doc_ids

    pool = [
        SerializedNode(chunk_id="c1", text="t", metadata={"doc_id": "d1"}),
        SerializedNode(chunk_id="c2", text="t", metadata={"doc_id": "d2"}),
        SerializedNode(chunk_id="c3", text="t", metadata={"doc_id": "d1"}),
        SerializedNode(chunk_id="c4", text="t", metadata={}),  # no doc_id
    ]
    assert distinct_doc_ids(pool) == ["d1", "d2"]
```
(The file already imports `SerializedNode` from `src.workflow.contracts`.)

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_search_orchestrator_helpers.py::test_distinct_doc_ids_dedups_in_order -q`
Expected: FAIL — `ImportError: cannot import name 'distinct_doc_ids'`.

- [ ] **Step 3: Implement + wire**

In `src/workflow/search/orchestrator.py`, add the pure helper near `cap_synth_sources`:
```python
def distinct_doc_ids(sources: list[SerializedNode]) -> list[str]:
    """Distinct, order-preserving doc_ids from a source pool's metadata.
    Skips sources without a doc_id (e.g. community partials)."""
    seen: list[str] = []
    for s in sources:
        d = s.metadata.get("doc_id")
        if d and d not in seen:
            seen.append(str(d))
    return seen
```
In `run`, in the final `return SearchOutcome(...)`, add:
```python
            documents=distinct_doc_ids(merged),
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_search_orchestrator_helpers.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/orchestrator.py tests/test_workflow/test_search_orchestrator_helpers.py
git commit -m "feat(search): local mode attaches distinct source doc_ids to SearchOutcome.documents"
```

---

### Task 5: global mode — documents_for_communities activity

**Files:**
- Modify: `src/workflow/contracts.py`
- Create: `src/workflow/search/activities/documents.py`
- Modify: `src/workflow/search/activities/__init__.py`, `src/workflow/search/global_wf.py`
- Test: `tests/test_workflow/test_search_documents_activity.py` (create), extend `tests/test_workflow/test_search_global.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_workflow/test_search_documents_activity.py`:
```python
"""documents_for_communities activity + global community_id helper."""

from __future__ import annotations

import pytest

from src.workflow.contracts import DocumentsForCommunitiesParams, MapPartialResult


@pytest.mark.asyncio
async def test_documents_for_communities_returns_doc_ids(monkeypatch):
    import src.workflow.search.activities.documents as mod

    class _Store:
        def structured_query(self, cypher, params):
            assert params == {"ids": [1, 2]}
            return [{"doc_id": "d1"}, {"doc_id": "d2"}, {"doc_id": None}]

    monkeypatch.setattr(mod, "_get_store", lambda: _Store())
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=[1, 2]))
    assert res.doc_ids == ["d1", "d2"]


@pytest.mark.asyncio
async def test_documents_for_communities_failopen(monkeypatch):
    import src.workflow.search.activities.documents as mod
    monkeypatch.setattr(mod, "_get_store", lambda: None)
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=[1]))
    assert res.doc_ids == []


def test_surviving_community_ids():
    from src.workflow.search.global_wf import surviving_community_ids
    partials = [
        MapPartialResult(community_id=1, partial="x", score=0.9),
        MapPartialResult(community_id=2, partial="", score=0.0),   # dropped
        MapPartialResult(community_id=3, partial="y", score=0.5),
    ]
    assert surviving_community_ids(partials) == [1, 3]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_search_documents_activity.py -q`
Expected: FAIL — import errors (`DocumentsForCommunitiesParams`, module, helper missing).

- [ ] **Step 3: Add contracts**

In `src/workflow/contracts.py`, add (near the other search params):
```python
class DocumentsForCommunitiesParams(_Frozen):
    """Input to ``documents_for_communities`` — community ids to resolve
    back to their source documents."""

    community_ids: list[int] = Field(default_factory=list)


class DocumentsForCommunitiesResult(_Frozen):
    """Output — distinct source doc_ids behind the given communities."""

    doc_ids: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Create the activity**

Create `src/workflow/search/activities/documents.py`:
```python
"""``documents_for_communities`` activity — map community ids to the
source documents their member entities were extracted from.

Graph path: (:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(:Community).
Fail-open: a missing store or any Cypher error → empty list (the answer
is never blocked on document provenance).
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import activity

from src.workflow.contracts import (
    DocumentsForCommunitiesParams,
    DocumentsForCommunitiesResult,
)

# NOTE: `c.doc_id` and the MENTIONS/IN_COMMUNITY traversal are written per
# the project graph model but UNVERIFIED against a live Neo4j store (same
# caution as the GDS Cypher in src/graph/communities.py). Verify the chunk
# doc_id property name on the live store before relying on it.
_DOCS_FOR_COMMUNITIES_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(comm:Community)
WHERE comm.id IN $ids
RETURN DISTINCT c.doc_id AS doc_id
"""


def _get_store() -> Any | None:
    """Neo4j store or None when unreachable (indirected for monkeypatch)."""
    try:
        from src.graph.store import build_neo4j_graph_store
        return build_neo4j_graph_store()
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning("documents_for_communities: store unavailable: %s", exc)
        return None


@activity.defn
async def documents_for_communities(
    params: DocumentsForCommunitiesParams,
) -> DocumentsForCommunitiesResult:
    activity.heartbeat({"stage": "documents_for_communities",
                        "n_communities": len(params.community_ids)})
    if not params.community_ids:
        return DocumentsForCommunitiesResult(doc_ids=[])
    store = _get_store()
    if store is None:
        return DocumentsForCommunitiesResult(doc_ids=[])
    try:
        rows = await asyncio.to_thread(
            store.structured_query,
            _DOCS_FOR_COMMUNITIES_CYPHER,
            {"ids": list(params.community_ids)},
        )
    except Exception as exc:  # noqa: BLE001 — fail-open
        activity.logger.warning("documents_for_communities  err=%s", exc)
        return DocumentsForCommunitiesResult(doc_ids=[])

    doc_ids: list[str] = []
    for r in rows or []:
        d = (r or {}).get("doc_id")
        if d and d not in doc_ids:
            doc_ids.append(str(d))
    return DocumentsForCommunitiesResult(doc_ids=doc_ids)
```

- [ ] **Step 5: Register the activity**

In `src/workflow/search/activities/__init__.py`: add
`from src.workflow.search.activities.documents import documents_for_communities`,
append `documents_for_communities` to the `SEARCH_V2_ACTIVITIES` list and to `__all__`.

- [ ] **Step 6: Wire global_wf**

In `src/workflow/search/global_wf.py`:
- add to the contracts import block: `DocumentsForCommunitiesParams`, `DocumentsForCommunitiesResult`.
- add the pure helper near `build_map_specs`:
```python
def surviving_community_ids(partials: list[MapPartialResult]) -> list[int]:
    """Community ids of partials that contributed to REDUCE (non-empty,
    score>0) — the communities behind the answer."""
    return [p.community_id for p in partials if p.partial and p.score > 0.0]
```
- after `partials` are gathered (before REDUCE), call the activity:
```python
        docs_res: DocumentsForCommunitiesResult = await workflow.execute_activity(
            "documents_for_communities",
            DocumentsForCommunitiesParams(
                community_ids=surviving_community_ids(partials)),
            result_type=DocumentsForCommunitiesResult,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10),
            retry_policy=FAST_RETRY,
        )
```
- in the final `return SearchOutcome(...)`, add `documents=list(docs_res.doc_ids),`.

(`timedelta` is already imported in global_wf; this is a graph read, not an LLM call, so it keeps the tight 2/10 timeout — do NOT use `LLM_*`.)

- [ ] **Step 7: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_search_documents_activity.py -q`
Expected: PASS (3 tests).

- [ ] **Step 8: Commit**

```bash
git add src/workflow/contracts.py src/workflow/search/activities/documents.py src/workflow/search/activities/__init__.py src/workflow/search/global_wf.py tests/test_workflow/test_search_documents_activity.py
git commit -m "feat(search): global mode resolves contributing communities to source docs (fail-open)"
```

---

### Task 6: drift mode — union local + global documents

**Files:**
- Modify: `src/workflow/search/router_wf.py`
- Test: `tests/test_workflow/test_search_router.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_workflow/test_search_router.py`:
```python
def test_merge_doc_ids_unions_in_order():
    from src.workflow.search.router_wf import merge_doc_ids
    assert merge_doc_ids(["d1", "d2"], ["d2", "d3"]) == ["d1", "d2", "d3"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_search_router.py::test_merge_doc_ids_unions_in_order -q`
Expected: FAIL — `ImportError: cannot import name 'merge_doc_ids'`.

- [ ] **Step 3: Implement + wire**

In `src/workflow/search/router_wf.py`, add the pure helper near `dispatch_for_route`:
```python
def merge_doc_ids(local: list[str], glob: list[str]) -> list[str]:
    """Union of two doc_id lists, order-preserving, deduped."""
    out = list(local)
    for d in glob:
        if d not in out:
            out.append(d)
    return out
```
In `DriftSearchWorkflow.run`, the global child returns `outcome` (a frozen `SearchOutcome`). Replace the final `return outcome` with a copy that unions in the local pass's docs:
```python
        return outcome.model_copy(update={
            "documents": merge_doc_ids(list(local.documents), list(outcome.documents)),
        })
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_search_router.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/router_wf.py tests/test_workflow/test_search_router.py
git commit -m "feat(search): drift mode unions local + global source documents"
```

---

## Final verification

- [ ] **Run the touched suites + import smoke**

```bash
uv run pytest tests/test_storage/test_minio_stream.py tests/test_api tests/test_workflow/test_search_orchestrator_helpers.py tests/test_workflow/test_search_global.py tests/test_workflow/test_search_documents_activity.py tests/test_workflow/test_search_router.py -q
uv run python -c "import src.workflow.worker, src.api.main; print('imports ok')"
```
Expected: all pass; `imports ok`.

- [ ] **Manual smoke (optional, needs live stack + worker redeploy)**

```bash
# after a search, pick a doc_id from sources[]/documents[]:
curl -s -X POST localhost:8000/api/v1/search/local -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{"query":"...","top_k":5}' | jq '.documents'
curl -s -L localhost:8000/api/v1/documents/<doc_id> -H "X-API-Key: $KEY" -o out.bin
```

> **Worker redeploy required** for the new `documents_for_communities`
> activity (it must be registered on the running search worker) and for
> the global/drift document wiring to take effect.

---

## Self-Review

**Spec coverage:** download endpoint (Task 1–2 ↔ spec "Surface"/"Data flow"/"Error handling"); `documents[]` shape (Task 3 ↔ "Response shape"); local (Task 4), global activity (Task 5), drift union (Task 6) ↔ "Derivation per mode"; live-Cypher caveat carried into the activity (Task 5). Tests per spec "Testing".

**Placeholder scan:** none — every code step is complete; the only "verify on live store" note is the intentional Cypher caveat from the spec.

**Type consistency:** `SearchOutcome.documents: list[str]` (workflow) → `to_document_refs` → `DocumentRef{doc_id,url}` (response). `documents_for_communities` returns `DocumentsForCommunitiesResult.doc_ids`. `surviving_community_ids`/`distinct_doc_ids`/`merge_doc_ids` signatures match their call sites. `SearchOutcome` is frozen → constructed-with-documents at return (orchestrator/global) and `model_copy(update=...)` for drift.

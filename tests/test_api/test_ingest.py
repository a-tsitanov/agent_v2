"""ASGI tests for `POST /api/v1/ingest`.

The MinIO client, Postgres, and the Temporal client are all mocked
so the route can be exercised end-to-end against the real FastAPI
app without any live infrastructure.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings
from src.storage.minio import S3Error


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


def _stub_minio(return_uri: str = "s3://test/abc/test.txt") -> MagicMock:
    storage = MagicMock()
    storage.put_object.return_value = return_uri
    return storage


def _stub_temporal_client() -> MagicMock:
    fake_handle = MagicMock()
    fake_client = MagicMock()
    fake_client.start_workflow = AsyncMock(return_value=fake_handle)
    return fake_client


@pytest.mark.asyncio
async def test_ingest_uploads_to_minio_and_inserts_s3_uri() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = _stub_minio("s3://kb-uploads/abc/file.txt")
    fake_client = _stub_temporal_client()

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
        patch(
            "src.api.routes.ingest.get_temporal_client",
            new=AsyncMock(return_value=fake_client),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("file.txt", b"hello world", "text/plain")},
                data={"department": "qa"},
            )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert "job_id" in body
    job_id = body["job_id"]

    # Storage was called with (object_key, BytesIO, length, content_type).
    stub_storage.put_object.assert_called_once()
    (key_arg, _stream, length_arg, content_arg) = (
        stub_storage.put_object.call_args.args
    )
    assert key_arg.endswith("/file.txt")
    assert length_arg == len(b"hello world")
    assert content_arg == "text/plain"

    # Postgres saw the S3 URI as `path`.
    ins.assert_awaited_once()
    pos_or_kw_path = (
        ins.call_args.args[1] if len(ins.call_args.args) >= 2
        else ins.call_args.kwargs.get("path")
    )
    assert pos_or_kw_path == "s3://kb-uploads/abc/file.txt"

    # Temporal: always the scheduler workflow, never the direct-start path.
    fake_client.start_workflow.assert_awaited_once()
    call = fake_client.start_workflow.call_args
    assert call.kwargs.get("id") == "ingest-scheduler"
    assert call.kwargs.get("start_signal") == "submit"
    assert call.kwargs.get("task_queue") == settings.temporal.task_queue
    # The submitted IngestParams is in start_signal_args[0].
    params = call.kwargs["start_signal_args"][0]
    assert params.doc_id == job_id
    assert params.path == "s3://kb-uploads/abc/file.txt"
    # Wiki flag is snapshotted here (outside the Temporal sandbox) so the
    # workflow branches on params.wiki_enabled instead of reading .env.
    assert params.wiki_enabled == settings.wiki.enabled


@pytest.mark.asyncio
async def test_ingest_returns_503_when_minio_fails() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = MagicMock()
    stub_storage.put_object.side_effect = S3Error(
        code="InternalError",
        message="minio down",
        resource="/test",
        request_id="r",
        host_id="h",
        response=None,
    )
    fake_client = _stub_temporal_client()

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
        patch(
            "src.api.routes.ingest.get_temporal_client",
            new=AsyncMock(return_value=fake_client),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("file.txt", b"hello", "text/plain")},
            )

    assert resp.status_code == 503, resp.text
    # When the upload fails we must NOT touch Postgres or Temporal.
    ins.assert_not_awaited()
    fake_client.start_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_ingest_returns_503_when_minio_unreachable() -> None:
    """MinIO container fully down: the SDK raises a urllib3
    `MaxRetryError`, not an `S3Error`.  The endpoint must catch that
    transport-level failure too and return a clean 503."""
    from urllib3.exceptions import MaxRetryError

    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = MagicMock()
    stub_storage.put_object.side_effect = MaxRetryError(
        pool=None, url="/kb-uploads",
        reason=ConnectionRefusedError("[Errno 61] Connection refused"),
    )
    fake_client = _stub_temporal_client()

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
        patch(
            "src.api.routes.ingest.get_temporal_client",
            new=AsyncMock(return_value=fake_client),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("file.txt", b"hello", "text/plain")},
            )

    assert resp.status_code == 503, resp.text
    ins.assert_not_awaited()
    fake_client.start_workflow.assert_not_awaited()


@pytest.mark.asyncio
async def test_ingest_returns_409_when_workflow_already_started() -> None:
    """Reuse policy on the Temporal side raises
    ``WorkflowAlreadyStartedError`` when a workflow with the same
    id already exists.  API should map that to 409 Conflict instead
    of letting it propagate as a 500."""
    from temporalio.exceptions import WorkflowAlreadyStartedError

    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = _stub_minio("s3://kb-uploads/abc/file.txt")
    fake_client = MagicMock()
    fake_client.start_workflow = AsyncMock(
        side_effect=WorkflowAlreadyStartedError(
            workflow_id="ingest-deadbeef",
            workflow_type="DocumentIngestWorkflow",
            run_id="run-abc",
        ),
    )

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()),
        patch(
            "src.api.routes.ingest.get_temporal_client",
            new=AsyncMock(return_value=fake_client),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("file.txt", b"hello", "text/plain")},
            )

    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["detail"]["workflow_id"] == "ingest-deadbeef"
    assert body["detail"]["run_id"] == "run-abc"


@pytest.mark.asyncio
async def test_ingest_requires_filename() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/ingest",
            headers=_api_key_header(),
            # Empty filename triggers the explicit 400 in upload_document.
            files={"file": ("", b"hello", "text/plain")},
        )

    assert resp.status_code in (400, 422)


@pytest.mark.asyncio
async def test_ingest_always_starts_scheduler() -> None:
    """Admission is always-on: every upload MUST route through the singleton
    IngestSchedulerWorkflow (id='ingest-scheduler', signal='submit').
    There is no fallback direct-start path any more."""
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = _stub_minio("s3://kb-uploads/abc/report.pdf")
    fake_client = _stub_temporal_client()

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()),
        patch(
            "src.api.routes.ingest.get_temporal_client",
            new=AsyncMock(return_value=fake_client),
        ),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("report.pdf", b"%PDF-test", "application/pdf")},
                data={"department": "finance"},
            )

    assert resp.status_code == 202, resp.text

    fake_client.start_workflow.assert_awaited_once()
    call = fake_client.start_workflow.call_args

    # Must always be the scheduler — never a per-doc id.
    assert call.kwargs.get("id") == "ingest-scheduler"
    assert call.kwargs.get("start_signal") == "submit"

    # The submitted document params arrive via start_signal_args.
    signal_args = call.kwargs.get("start_signal_args", [])
    assert signal_args, "start_signal_args must not be empty"
    params = signal_args[0]
    assert params.path == "s3://kb-uploads/abc/report.pdf"

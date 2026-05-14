"""ASGI tests for `POST /api/v1/ingest`.

The MinIO client, Postgres, and the taskiq enqueue are all mocked
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


@pytest.mark.asyncio
async def test_ingest_uploads_to_minio_and_inserts_s3_uri() -> None:
    from src.api.main import app
    from src.storage.postgres import AsyncPostgres

    stub_storage = _stub_minio("s3://kb-uploads/abc/file.txt")

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
        patch(
            "src.api.routes.ingest.process_document.kiq",
            new=AsyncMock(),
        ) as kiq,
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
    _self, _pos_doc_id, pos_path, *_ = (
        (None, *ins.call_args.args)
        if ins.call_args.args else (None,)
    )
    # `path` may be the 2nd positional or kwarg, accept either.
    pos_or_kw_path = (
        ins.call_args.args[1] if len(ins.call_args.args) >= 2
        else ins.call_args.kwargs.get("path")
    )
    assert pos_or_kw_path == "s3://kb-uploads/abc/file.txt"

    # Worker was enqueued with the same S3 URI.
    kiq.assert_awaited_once()
    enqueue_args = kiq.call_args.args
    assert enqueue_args[1] == "s3://kb-uploads/abc/file.txt"


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

    with (
        patch(
            "src.api.routes.ingest.build_minio_storage",
            return_value=stub_storage,
        ),
        patch.object(AsyncPostgres, "insert_pending", new=AsyncMock()) as ins,
        patch(
            "src.api.routes.ingest.process_document.kiq",
            new=AsyncMock(),
        ) as kiq,
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/v1/ingest",
                headers=_api_key_header(),
                files={"file": ("file.txt", b"hello", "text/plain")},
            )

    assert resp.status_code == 503, resp.text
    # When the upload fails we must NOT touch Postgres or RabbitMQ.
    ins.assert_not_awaited()
    kiq.assert_not_awaited()


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

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

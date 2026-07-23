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

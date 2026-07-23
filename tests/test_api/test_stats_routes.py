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

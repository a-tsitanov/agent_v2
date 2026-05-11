"""Route skeleton tests — confirm the three search endpoints exist
and the unfilled ones (agent, selfrag) return 503 with a clear
message."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


@pytest.mark.asyncio
async def test_search_route_registered() -> None:
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/search", json={"query": "hello"},
        )
    assert resp.status_code in (401, 422)


@pytest.mark.asyncio
async def test_agent_route_returns_503_until_r7() -> None:
    from src.api.main import app

    key = settings.api.keys_list[0]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/agent",
            headers={"X-API-Key": key},
            json={"query": "hello"},
        )
    assert resp.status_code == 503
    assert "R7" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_selfrag_route_returns_503_until_r8() -> None:
    from src.api.main import app

    key = settings.api.keys_list[0]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/selfrag",
            headers={"X-API-Key": key},
            json={"query": "hello"},
        )
    assert resp.status_code == 503
    assert "R8" in resp.json()["detail"]

"""Route skeleton tests — confirm the search endpoints are registered.

R7b cutover: the sole search surface is now
``/api/v1/search/{local,global,drift,auto}``.  The legacy ReAct
endpoints (``/api/v1/search``, ``/agent``, ``/selfrag``) and the
judge-based ``/api/v1/legacy/agent`` baseline were removed and now
return 404."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/search/local",
        "/api/v1/search/global",
        "/api/v1/search/drift",
        "/api/v1/search/auto",
    ],
)
async def test_new_search_routes_registered(path: str) -> None:
    """The plan-execute / GraphRAG endpoints are wired; without an API
    key (or with a bad body) they short-circuit at auth / validation —
    never 404."""
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(path, json={"query": "hello"})
    assert resp.status_code in (401, 422)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/search",
        "/api/v1/agent",
        "/api/v1/selfrag",
        "/api/v1/legacy/agent",
    ],
)
async def test_legacy_search_routes_removed(path: str) -> None:
    """R7b: the legacy ReAct + judge-based routes are gone → 404."""
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post(path, json={"query": "hello"})
    assert resp.status_code == 404

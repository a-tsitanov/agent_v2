"""ASGI test for POST /admin/wiki/rebuild.

The WIKI_ENABLED default is False, so the route returns {"status": "disabled"}
without touching Temporal or Neo4j — exercises the disabled-path only.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_wiki_rebuild_returns_disabled_when_off():
    """WIKI_ENABLED defaults False → route returns disabled without infra."""
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/admin/wiki/rebuild")
    assert r.status_code == 200
    assert r.json()["status"] == "disabled"

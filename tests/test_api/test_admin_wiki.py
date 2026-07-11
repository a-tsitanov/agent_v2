"""ASGI tests for POST /admin/wiki/rebuild.

The WIKI_ENABLED default is False, so the route returns {"status": "disabled"}
without touching Temporal or the graph store — exercises the disabled-path.
A second test flips WIKI_ENABLED on and exercises the `all=true` branch,
asserting the mark-all-dirty call routes through the WikiGraphOps seam.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.mark.asyncio
async def test_wiki_rebuild_all_routes_mark_all_dirty_through_seam(monkeypatch):
    """`?all=true` (when enabled) marks every entity dirty via
    build_wiki_graph_ops(store).mark_all_dirty() — not a raw Cypher call."""
    from src.api.main import app
    from src.api.routes import admin
    from src.config import settings

    monkeypatch.setattr(settings.wiki, "enabled", True)

    store = MagicMock()
    ops = MagicMock()
    build_ops = MagicMock(return_value=ops)

    fake_handle = MagicMock(id="wiki-sweep-manual")
    fake_client = MagicMock()
    fake_client.start_workflow = AsyncMock(return_value=fake_handle)

    with (
        patch.object(admin, "build_graph_store", return_value=store),
        patch.object(admin, "build_wiki_graph_ops", build_ops),
        patch.object(admin, "get_temporal_client",
                      new=AsyncMock(return_value=fake_client)),
    ):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            r = await ac.post("/admin/wiki/rebuild", params={"all": "true"})

    assert r.status_code == 200
    assert r.json()["status"] == "started"
    build_ops.assert_called_once_with(store)
    ops.mark_all_dirty.assert_called_once_with()
    store.structured_query.assert_not_called()

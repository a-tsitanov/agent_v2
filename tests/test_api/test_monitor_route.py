"""ASGI tests for POST /admin/monitor/sweep and /admin/monitor/watch.

The MONITOR_ENABLED default is False, so sweep returns {"status": "disabled"}
without touching Temporal.  The watch route is tested via monkeypatching
mark_watched so no live Neo4j is needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_monitor_sweep_returns_disabled_when_off():
    """MONITOR_ENABLED defaults False → returns disabled without touching Temporal."""
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/admin/monitor/sweep")
    assert r.status_code == 200
    assert r.json()["status"] == "disabled"


@pytest.mark.asyncio
async def test_monitor_watch_calls_mark_watched():
    """POST /admin/monitor/watch patches mark_watched and build_neo4j_graph_store."""
    from src.api.main import app

    recorded: list = []

    def _fake_mark_watched(store, names, watched=True):
        recorded.append((names, watched))

    transport = ASGITransport(app=app)
    with (
        patch("src.api.routes.admin.mark_watched", _fake_mark_watched),
        patch("src.api.routes.admin.build_graph_store", return_value=MagicMock()),
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            r = await ac.post(
                "/admin/monitor/watch",
                json=["Alpha", "Beta"],
                params={"watched": "true"},
            )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["count"] == 2
    assert body["watched"] is True
    assert recorded == [(["Alpha", "Beta"], True)]

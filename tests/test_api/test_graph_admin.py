"""ASGI tests for the /admin/graph/* analysis endpoints (Track 7b)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings


def _key() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


async def _post(path, *, headers=None):
    from src.api.main import app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        return await ac.post(path, headers=headers)


@pytest.mark.asyncio
async def test_graph_stats_endpoint_returns_analysis():
    fake = {"entities": 50000, "relationships": 120000, "communities": 191}
    with patch("src.api.routes.graph_admin._store", return_value=object()), patch(
        "src.api.routes.graph_admin.analysis.graph_stats",
        new=AsyncMock(return_value=fake),
    ):
        resp = await _post("/admin/graph/stats", headers=_key())
    assert resp.status_code == 200
    assert resp.json()["entities"] == 50000


@pytest.mark.asyncio
async def test_graph_stats_requires_api_key():
    resp = await _post("/admin/graph/stats")
    assert resp.status_code in (401, 403)


@pytest.mark.asyncio
async def test_pagerank_endpoint_wraps_top():
    with patch("src.api.routes.graph_admin._store", return_value=object()), patch(
        "src.api.routes.graph_admin.analysis.pagerank",
        new=AsyncMock(return_value=[{"name": "Иванов", "score": 9.1}]),
    ):
        resp = await _post("/admin/graph/pagerank?top_n=1", headers=_key())
    assert resp.status_code == 200
    assert resp.json()["top"][0]["name"] == "Иванов"


# ── temporal: snapshot / diff (analytics layer v1) ───────────────────


@pytest.mark.asyncio
async def test_snapshot_endpoint_returns_temporal():
    fake = {
        "as_of": "2024-01-01", "edges": [], "nodes": [],
        "coverage": {"dated": 0, "fallback": 0, "untimed": 0, "total": 0},
    }
    with patch("src.api.routes.graph_admin._store", return_value=object()), patch(
        "src.api.routes.graph_admin.temporal.snapshot",
        new=AsyncMock(return_value=fake),
    ):
        resp = await _post("/admin/graph/snapshot?as_of=2024-01-01", headers=_key())
    assert resp.status_code == 200
    assert resp.json()["as_of"] == "2024-01-01"


@pytest.mark.asyncio
async def test_diff_endpoint_returns_temporal():
    fake = {
        "t1": "2023-01-01", "t2": "2025-01-01",
        "added": [{"source": "A", "target": "NewCo", "label": "WORKS_AT"}],
        "removed": [], "persisted": [],
        "t1_coverage": {}, "t2_coverage": {},
    }
    with patch("src.api.routes.graph_admin._store", return_value=object()), patch(
        "src.api.routes.graph_admin.temporal.diff",
        new=AsyncMock(return_value=fake),
    ):
        resp = await _post(
            "/admin/graph/diff?t1=2023-01-01&t2=2025-01-01", headers=_key(),
        )
    assert resp.status_code == 200
    assert resp.json()["added"][0]["target"] == "NewCo"


@pytest.mark.asyncio
async def test_snapshot_requires_api_key():
    resp = await _post("/admin/graph/snapshot?as_of=2024-01-01")
    assert resp.status_code in (401, 403)

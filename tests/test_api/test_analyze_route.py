"""Tests for POST /api/v1/analyze endpoint.

Mirrors the pattern from test_search_v2_routes.py:
- Unit-test _to_params (no infra needed)
- HTTP round-trip via ASGI + stubbed Temporal client
- 401 without API key
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.analytics.contracts import AnalyticsOutcome, Provenance
from src.api.routes.analyze import _to_params
from src.config import settings
from src.models.analyze import AnalyzeRequest
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow

# ---------------------------------------------------------------------------
# Unit: _to_params
# ---------------------------------------------------------------------------


def test_request_to_params_maps_query_and_top_n():
    req = AnalyzeRequest(query="trend analysis", top_n=10)
    p = _to_params(req)
    assert p.query == "trend analysis"
    assert p.top_n == 10
    assert p.date_from_epoch is None
    assert p.date_to_epoch is None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


def _stub_client(outcome: AnalyticsOutcome) -> MagicMock:
    handle = MagicMock()
    handle.result = AsyncMock(return_value=outcome)
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    return client


def _outcome() -> AnalyticsOutcome:
    return AnalyticsOutcome(
        query="q",
        answer="The trend is up.",
        provenance=Provenance(route="catalog", plan_reason="test", steps=[], elapsed_ms=50),
        latency_ms=50,
    )


async def _post_analyze(body: dict, *, headers: dict | None = None):
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        return await ac.post("/api/v1/analyze", json=body, headers=headers)


# ---------------------------------------------------------------------------
# HTTP: happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_analyze_returns_200_with_valid_request():
    client = _stub_client(_outcome())
    with patch(
        "src.api.routes.analyze.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        resp = await _post_analyze({"query": "q"}, headers=_api_key_header())

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["query"] == "q"
    assert body["answer"] == "The trend is up."
    assert "provenance" in body

    # Started AnalyticalQueryWorkflow on the correct queue.
    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    assert call.args[0] is AnalyticalQueryWorkflow.run
    assert call.kwargs.get("task_queue") == settings.temporal.search_task_queue


# ---------------------------------------------------------------------------
# HTTP: auth + validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_analyze_requires_api_key():
    resp = await _post_analyze({"query": "q"})  # no header
    assert resp.status_code == 401

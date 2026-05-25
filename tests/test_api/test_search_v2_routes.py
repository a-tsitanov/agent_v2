"""ASGI tests for the R7a search endpoints (/search/global|drift|auto).

The Temporal client is mocked so each route is exercised end-to-end
against the real FastAPI app without live infra.  We assert each endpoint
STARTS the right workflow on the right queue and maps the SearchOutcome
onto the shared SearchResponse.  Auth (401 without key) is covered too.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings
from src.workflow.contracts import SearchOutcome, SerializedNode
from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.router_wf import AutoSearchWorkflow, DriftSearchWorkflow


def _api_key_header() -> dict[str, str]:
    return {"X-API-Key": settings.api.keys_list[0]}


def _stub_client(outcome: SearchOutcome) -> MagicMock:
    handle = MagicMock()
    handle.result = AsyncMock(return_value=outcome)
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    return client


def _outcome(mode: str) -> SearchOutcome:
    return SearchOutcome(
        query="q", mode=mode, answer=f"{mode} answer",
        sources=[SerializedNode(chunk_id="community:1", text="t", score=1.0,
                                metadata={"doc_id": "d1"})],
        latency_ms=12,
    )


async def _post(path: str, *, headers=None):
    from src.api.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        return await ac.post(path, json={"query": "q"}, headers=headers)


@pytest.mark.parametrize(
    ("path", "wf", "expected_mode"),
    [
        ("/api/v1/search/global", GlobalSearchWorkflow.run, "global"),
        ("/api/v1/search/drift", DriftSearchWorkflow.run, "drift"),
        ("/api/v1/search/auto", AutoSearchWorkflow.run, "local"),
    ],
)
@pytest.mark.asyncio
async def test_route_starts_expected_workflow(path, wf, expected_mode):
    client = _stub_client(_outcome(expected_mode))
    with patch(
        "src.api.routes.search_v2.get_temporal_client",
        new=AsyncMock(return_value=client),
    ):
        resp = await _post(path, headers=_api_key_header())

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["mode"] == expected_mode
    assert body["answer"] == f"{expected_mode} answer"
    # Started the expected workflow on the small search queue.
    client.start_workflow.assert_awaited_once()
    call = client.start_workflow.call_args
    assert call.args[0] is wf
    assert call.kwargs.get("task_queue") == settings.temporal.search_task_queue


@pytest.mark.parametrize(
    "path",
    ["/api/v1/search/global", "/api/v1/search/drift", "/api/v1/search/auto"],
)
@pytest.mark.asyncio
async def test_route_requires_api_key(path):
    resp = await _post(path)  # no header
    assert resp.status_code == 401

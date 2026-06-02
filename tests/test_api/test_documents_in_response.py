"""The search response carries documents[] (links) built from outcome.documents."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import settings
from src.workflow.contracts import SearchOutcome


def _key():
    return {"X-API-Key": settings.api.keys_list[0]}


def _stub_client(outcome):
    handle = MagicMock()
    handle.result = AsyncMock(return_value=outcome)
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=handle)
    return client


@pytest.mark.asyncio
async def test_to_document_refs_builds_urls():
    from src.api.routes.search_v2 import to_document_refs
    refs = to_document_refs(["d1", "d2"])
    assert [r.doc_id for r in refs] == ["d1", "d2"]
    assert refs[0].url == "/api/v1/documents/d1"


@pytest.mark.asyncio
async def test_local_response_has_documents():
    outcome = SearchOutcome(
        query="q", mode="local", answer="a", documents=["d1", "d2"], latency_ms=1)
    with patch("src.api.routes.search_v2.get_temporal_client",
               new=AsyncMock(return_value=_stub_client(outcome))):
        from src.api.main import app
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://test") as ac:
            resp = await ac.post("/api/v1/search/local",
                                 json={"query": "q"}, headers=_key())
    assert resp.status_code == 200, resp.text
    docs = resp.json()["documents"]
    assert {d["doc_id"] for d in docs} == {"d1", "d2"}
    assert docs[0]["url"] == f"/api/v1/documents/{docs[0]['doc_id']}"

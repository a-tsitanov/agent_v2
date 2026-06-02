"""documents_for_communities activity + global community_id helper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import src.workflow.search.activities.documents as documents_mod
from src.workflow.contracts import DocumentsForCommunitiesParams, MapPartialResult


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    """Outside a Temporal env ``activity.heartbeat``/``activity.logger``
    have no context — stub the module-level ``activity`` (same pattern as
    the other search-activity unit tests)."""
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(documents_mod, "activity", mock)


@pytest.mark.asyncio
async def test_documents_for_communities_returns_doc_ids(monkeypatch):
    import src.workflow.search.activities.documents as mod

    class _Store:
        def structured_query(self, cypher, params):
            assert params == {"ids": [1, 2]}
            return [{"doc_id": "d1"}, {"doc_id": "d2"}, {"doc_id": None}]

    monkeypatch.setattr(mod, "_get_store", lambda: _Store())
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=[1, 2]))
    assert res.doc_ids == ["d1", "d2"]


@pytest.mark.asyncio
async def test_documents_for_communities_failopen(monkeypatch):
    import src.workflow.search.activities.documents as mod
    monkeypatch.setattr(mod, "_get_store", lambda: None)
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=[1]))
    assert res.doc_ids == []


def test_surviving_community_ids():
    from src.workflow.search.global_wf import surviving_community_ids
    partials = [
        MapPartialResult(community_id=1, partial="x", score=0.9),
        MapPartialResult(community_id=2, partial="", score=0.0),   # dropped
        MapPartialResult(community_id=3, partial="y", score=0.5),
    ]
    assert surviving_community_ids(partials) == [1, 3]

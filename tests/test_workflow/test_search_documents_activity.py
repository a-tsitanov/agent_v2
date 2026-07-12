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
            assert params == {"ids": ["1", "2"]}
            return [{"doc_id": "d1"}, {"doc_id": "d2"}, {"doc_id": None}]

    monkeypatch.setattr(mod, "_get_store", lambda: _Store())
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=["1", "2"]))
    assert res.doc_ids == ["d1", "d2"]


@pytest.mark.asyncio
async def test_documents_for_communities_failopen(monkeypatch):
    import src.workflow.search.activities.documents as mod
    monkeypatch.setattr(mod, "_get_store", lambda: None)
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=["1"]))
    assert res.doc_ids == []


def test_surviving_community_ids():
    from src.workflow.search.global_wf import surviving_community_ids
    partials = [
        MapPartialResult(community_id="1", partial="x", score=0.9),
        MapPartialResult(community_id="2", partial="", score=0.0),   # dropped
        MapPartialResult(community_id="3", partial="y", score=0.5),
    ]
    assert surviving_community_ids(partials) == ["1", "3"]


def test_docs_for_communities_cypher_scoped_to_level0():
    """После линковки членов на всех уровнях адресация по comm.id
    коллизит между уровнями; траверс должен быть заскоуплен на level 0."""
    from src.workflow.search.activities import documents

    cy = documents._DOCS_FOR_COMMUNITIES_CYPHER
    assert "-[:IN_COMMUNITY]->(comm:Community {level: 0})" in cy
    assert "comm.id IN $ids" in cy


# ── nebula doc↔community: member first_doc_id approximation ───────────


class _FakeNebulaStore:
    def __init__(self, canned):
        self.calls = []
        self._canned = list(canned)

    def structured_query(self, stmt, param_map=None):
        self.calls.append(stmt)
        for sub, rows in self._canned:
            if sub in stmt:
                return rows
        return []


@pytest.mark.asyncio
async def test_documents_for_communities_nebula_member_first_doc(monkeypatch):
    import src.workflow.search.activities.documents as mod
    from src.config import settings

    monkeypatch.setattr(settings.graph, "backend", "nebula")
    store = _FakeNebulaStore(canned=[
        ("OVER `IN_COMMUNITY` REVERSELY", [{"ent": "e1"}, {"ent": "e2"}]),
        ("FETCH PROP ON `Entity`", [{"doc_id": "dA"}, {"doc_id": "dB"}, {"doc_id": None}]),
    ])
    monkeypatch.setattr(mod, "_get_store", lambda: store)
    res = await mod.documents_for_communities(
        DocumentsForCommunitiesParams(community_ids=["1"]))
    # member first_doc_ids surfaced (None dropped by the downstream de-dup)
    assert set(res.doc_ids) == {"dA", "dB"}
    assert "IN_COMMUNITY` REVERSELY" in store.calls[0]

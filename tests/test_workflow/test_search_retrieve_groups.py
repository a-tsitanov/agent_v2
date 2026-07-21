import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.workflow.search.activities.retrieve as R
from src.retrieval.date_filters import DateBounds
from src.retrieval.group_filter import GroupFilter, combined_metadata_filters
from src.workflow.contracts import RetrieveParams


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    """No live Temporal — stub the activity heartbeat/logger context (same
    pattern as test_search_retrieve.py's fixture; this file is a separate
    module so pytest doesn't auto-share that fixture across files)."""
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(R, "activity", mock)


def test_group_filter_pushed_into_vector_retriever(monkeypatch):
    seen = {}

    async def _vret(top_k, filters=None):
        seen["top_k"] = top_k
        seen["filters"] = filters
        async def _retrieve(q): return []
        return SimpleNamespace(aretrieve=_retrieve)

    async def _greter():  # graph retriever unused here
        return None

    monkeypatch.setattr(R, "get_vector_retriever", _vret)
    monkeypatch.setattr(R, "get_graph_retriever", _greter)
    # Make the tool pipeline a no-op so only retriever construction matters.
    async def _dispatch(*a, **k):
        return SimpleNamespace(observation="{}", sources=[])
    monkeypatch.setattr(R.atomic_tools, "dispatch", _dispatch)

    params = RetrieveParams(subquestion="q", top_k=10, groups=["official"])
    asyncio.run(R.retrieve_subquestion(params))

    expected = combined_metadata_filters(DateBounds(), GroupFilter(include=("official",)))
    assert seen["filters"] == expected
    assert seen["top_k"] > 10  # over-fetched because a filter is set

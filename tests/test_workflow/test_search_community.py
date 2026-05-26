"""Unit tests for the offline community-build activities + workflow
helpers (Search R6).

The community activities are plain async fns over stubbed deps (no live
Temporal) — same pattern as the other search-activity tests.  We cover:

  * ``summarize_community_activity`` produces a summary via a mock small
    LLM and issues the ``:Community.summary`` MERGE; fail-safe on error,
  * the pure ``build_summarize_specs`` workflow helper turns detected
    communities into batchable summarize params (idempotent re-run).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow.contracts import (
    CommunityRef,
    DetectCommunitiesResult,
    SummarizeCommunityParams,
)
from src.workflow.search.activities import community as community_mod
from src.workflow.search.activities.community import summarize_community_activity
from src.workflow.search.community_wf import build_summarize_specs


class _FakeStore:
    def __init__(self, *, member_rows=None, raise_on=None):
        self._member_rows = member_rows or []
        self._raise_on = raise_on
        self.calls: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        if self._raise_on and self._raise_on in cypher:
            raise RuntimeError("boom")
        if "RETURN" in cypher and "description" in cypher:
            return self._member_rows
        return []


class _FakeLLM:
    """Records the prompt; returns a canned summary."""

    def __init__(self, text="Community of construction firms."):
        self.text = text
        self.seen_prompt: str | None = None

    async def acomplete(self, prompt, **_kw):
        self.seen_prompt = prompt
        return MagicMock(text=self.text)


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(community_mod, "activity", mock)


@pytest.mark.asyncio
async def test_summarize_produces_summary_and_persists(monkeypatch):
    store = _FakeStore(member_rows=[
        {"name": "Ромашка", "description": "Поставщик материалов."},
        {"name": "СтройИнвест", "description": "Подрядчик."},
    ])
    llm = _FakeLLM("Группа строительных компаний.")
    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: llm)

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id=2, level=0, members=["Ромашка", "СтройИнвест"],
    ))

    assert out.community_id == 2
    assert out.summary == "Группа строительных компаний."
    assert out.persisted is True
    # The member names made it into the prompt.
    assert "Ромашка" in (llm.seen_prompt or "")
    # The summary MERGE was issued.
    joined = "\n".join(c for c, _ in store.calls)
    assert "MERGE (c:Community" in joined
    assert "summary" in joined


@pytest.mark.asyncio
async def test_summarize_failsafe_on_llm_error(monkeypatch):
    store = _FakeStore(member_rows=[{"name": "A", "description": "x"}])

    class _BoomLLM:
        async def acomplete(self, *_a, **_k):
            raise RuntimeError("llm down")

    monkeypatch.setattr(community_mod, "_get_store", lambda: store)
    monkeypatch.setattr(community_mod, "_get_summary_llm", lambda: _BoomLLM())

    out = await summarize_community_activity(SummarizeCommunityParams(
        community_id=1, members=["A", "B"],
    ))
    assert out.summary == ""
    assert out.persisted is False


@pytest.mark.asyncio
async def test_summarize_empty_members_skips_llm(monkeypatch):
    called = {"llm": False}

    def _llm():
        called["llm"] = True
        raise AssertionError("must not build LLM for empty community")

    monkeypatch.setattr(community_mod, "_get_store", lambda: _FakeStore())
    monkeypatch.setattr(community_mod, "_get_summary_llm", _llm)

    out = await summarize_community_activity(
        SummarizeCommunityParams(community_id=1, members=[]),
    )
    assert out.persisted is False
    assert called["llm"] is False


# ── pure workflow helper: detected → batchable summarize specs ──────


def test_build_summarize_specs_maps_each_community():
    detect = DetectCommunitiesResult(communities=[
        CommunityRef(community_id=1, level=0, members=["A", "B", "C"]),
        CommunityRef(community_id=2, level=0, members=["D", "E", "F"]),
    ])
    specs = build_summarize_specs(detect)
    assert [s.community_id for s in specs] == [1, 2]
    assert all(isinstance(s, SummarizeCommunityParams) for s in specs)
    assert specs[0].members == ["A", "B", "C"]


def test_build_summarize_specs_empty():
    assert build_summarize_specs(DetectCommunitiesResult(communities=[])) == []

"""GraphRAG global map-reduce tests (Search R7a).

The global activities are plain async fns over stubbed deps (no live
Temporal) and the workflow's map-reduce wiring lives in pure helpers, so
we cover the contract WITHOUT a Temporal env or a real LLM/store:

  * ``rank_summaries`` ranks community summaries by query overlap + caps,
  * ``map_communities`` reads summaries and is fail-safe on store errors,
  * ``map_community_partial`` produces a partial / self-drops 'НЕТ',
  * ``build_map_specs`` / ``partials_to_sources`` assemble the fan-out
    and the reduce context,
  * ``build_reduce_call`` pins REDUCE to the LARGE queue + synthesis tier
    (the R5 pattern) — this is the "reduce on large, pinned queue" assert.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.config import settings
from src.workflow.contracts import (
    CommunitySummaryRef,
    MapCommunitiesParams,
    MapPartialParams,
    MapPartialResult,
    SerializedNode,
)
from src.workflow.search.activities import global_search as gs_mod
from src.workflow.search.activities.global_search import (
    is_relevant_partial,
    map_communities,
    map_community_partial,
    rank_summaries,
)
from src.workflow.search.global_wf import (
    build_map_specs,
    build_reduce_call,
    partials_to_sources,
)


# ── rank_summaries (pure) ───────────────────────────────────────────


def test_rank_summaries_orders_by_query_overlap_and_caps():
    rows = [
        {"community_id": 1, "level": 0, "summary": "строительные фирмы города"},
        {"community_id": 2, "level": 0, "summary": "поставщики продуктов питания"},
        {"community_id": 3, "level": 0, "summary": ""},  # blank → dropped
    ]
    refs = rank_summaries(rows, query="строительные фирмы", limit=10)
    # blank dropped; the строительные community ranks first on overlap.
    assert [r.community_id for r in refs] == [1, 2]
    assert all(isinstance(r, CommunitySummaryRef) for r in refs)


def test_rank_summaries_caps_to_limit():
    rows = [
        {"community_id": i, "level": 0, "summary": f"summary {i}"}
        for i in range(5)
    ]
    refs = rank_summaries(rows, query="q", limit=2)
    assert len(refs) == 2


def test_is_relevant_partial():
    assert is_relevant_partial("Сообщество описывает строителей.") is True
    assert is_relevant_partial("НЕТ") is False
    assert is_relevant_partial("нет.") is False
    assert is_relevant_partial("") is False


# ── map_communities activity (stubbed store) ────────────────────────


class _FakeStore:
    def __init__(self, rows=None, raise_=False):
        self._rows = rows or []
        self._raise = raise_

    def structured_query(self, cypher, param_map=None):
        if self._raise:
            raise RuntimeError("neo4j down")
        return self._rows


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(gs_mod, "activity", mock)


@pytest.mark.asyncio
async def test_map_communities_reads_summaries(monkeypatch):
    store = _FakeStore(rows=[
        {"community_id": 7, "level": 0, "summary": "строители", "member_count": 5},
    ])
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    out = await map_communities(MapCommunitiesParams(query="строители", limit=10))
    assert [c.community_id for c in out.communities] == [7]


@pytest.mark.asyncio
async def test_map_communities_failsafe_on_store_error(monkeypatch):
    monkeypatch.setattr(gs_mod, "_get_store", lambda: _FakeStore(raise_=True))
    out = await map_communities(MapCommunitiesParams(query="q"))
    assert out.communities == []


@pytest.mark.asyncio
async def test_map_communities_no_store(monkeypatch):
    monkeypatch.setattr(gs_mod, "_get_store", lambda: None)
    out = await map_communities(MapCommunitiesParams(query="q"))
    assert out.communities == []


# ── map_community_partial activity (stubbed LLM) ────────────────────


class _FakeLLM:
    def __init__(self, text):
        self._text = text

    async def acomplete(self, prompt, **_kw):
        return MagicMock(text=self._text)


@pytest.mark.asyncio
async def test_map_partial_produces_partial(monkeypatch):
    monkeypatch.setattr(
        gs_mod, "_get_map_llm", lambda: _FakeLLM("Сообщество о строителях."),
    )
    out = await map_community_partial(MapPartialParams(
        query="строители", community_id=3, summary="строительные фирмы",
    ))
    assert out.community_id == 3
    assert out.score == 1.0
    assert "строител" in out.partial.lower()


@pytest.mark.asyncio
async def test_map_partial_self_drops_irrelevant(monkeypatch):
    monkeypatch.setattr(gs_mod, "_get_map_llm", lambda: _FakeLLM("НЕТ"))
    out = await map_community_partial(MapPartialParams(
        query="строители", community_id=4, summary="поставщики еды",
    ))
    assert out.score == 0.0
    assert out.partial == ""


@pytest.mark.asyncio
async def test_map_partial_failsafe_on_llm_error(monkeypatch):
    class _Boom:
        async def acomplete(self, *_a, **_k):
            raise RuntimeError("llm down")

    monkeypatch.setattr(gs_mod, "_get_map_llm", lambda: _Boom())
    out = await map_community_partial(MapPartialParams(
        query="q", community_id=5, summary="x",
    ))
    assert out.score == 0.0


# ── workflow pure helpers ───────────────────────────────────────────


def test_build_map_specs_one_per_community():
    comms = [
        CommunitySummaryRef(community_id=1, summary="a"),
        CommunitySummaryRef(community_id=2, summary="b"),
    ]
    specs = build_map_specs(comms, query="q")
    assert [s.community_id for s in specs] == [1, 2]
    assert all(isinstance(s, MapPartialParams) for s in specs)
    assert specs[0].query == "q"


def test_partials_to_sources_drops_empty_and_zero_score():
    partials = [
        MapPartialResult(community_id=1, partial="relevant", score=1.0),
        MapPartialResult(community_id=2, partial="", score=0.0),
        MapPartialResult(community_id=3, partial="dropped", score=0.0),
    ]
    sources = partials_to_sources(partials)
    assert [s.chunk_id for s in sources] == ["community:1"]
    assert sources[0].metadata["community_id"] == 1


def test_build_reduce_call_pins_large_queue_and_tier():
    sources = [SerializedNode(chunk_id="community:1", text="t", score=1.0)]
    queue, params = build_reduce_call(
        query="каковы темы?", sources=sources, max_refinements=3,
    )
    # REDUCE runs on the dedicated large-tier queue (R5 pattern).
    assert queue == settings.temporal.large_task_queue
    assert queue == "kb-search-large"
    assert params.use_synthesis_llm is True
    assert params.mode == "simple"
    assert params.query == "каковы темы?"
    assert [n.chunk_id for n in params.accumulated] == ["community:1"]


def test_coerce_params_accepts_dict():
    from src.workflow.search.global_wf import _coerce_global_params
    from src.workflow.contracts import GlobalSearchParams

    out = _coerce_global_params({"query": "q", "drift_mode": True})
    assert isinstance(out, GlobalSearchParams)
    assert out.drift_mode is True
    # passthrough for already-typed input
    typed = GlobalSearchParams(query="q")
    assert _coerce_global_params(typed) is typed

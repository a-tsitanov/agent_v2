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
    _cosine,
    is_relevant_partial,
    map_communities,
    map_community_partial,
    rank_summaries,
    select_communities_descent,
    select_communities_semantic,
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
    assert [r.community_id for r in refs] == ["1", "2"]
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
    assert [c.community_id for c in out.communities] == ["7"]


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


# ── select_communities_semantic (stubbed store) ─────────────────────


class _FakeVecStore:
    """Records the queries it received; returns canned queryNodes rows."""

    def __init__(self, rows=None, raise_=False):
        self._rows = rows if rows is not None else []
        self._raise = raise_
        self.queries: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        self.queries.append((cypher, param_map or {}))
        if self._raise:
            raise RuntimeError("vector index down")
        return self._rows


class _FakeEmbed:
    def __init__(self, vec=None, raise_=False):
        self._vec = vec or [0.1, 0.2, 0.3]
        self._raise = raise_

    async def aget_text_embedding(self, text):
        if self._raise:
            raise RuntimeError("embed down")
        return self._vec


@pytest.mark.asyncio
async def test_select_communities_semantic_builds_refs():
    rows = [
        {"community_id": 7, "level": 0, "summary": "строители"},
        {"community_id": 8, "level": 0, "summary": "поставщики"},
    ]
    store = _FakeVecStore(rows=rows)
    refs = await select_communities_semantic(
        store, [0.1, 0.2], level=0, limit=5,
    )
    assert [r.community_id for r in refs] == ["7", "8"]
    assert all(isinstance(r, CommunitySummaryRef) for r in refs)
    assert refs[0].summary == "строители"
    # passes the vector + limit through to queryNodes
    _, params = store.queries[0]
    assert params["vec"] == [0.1, 0.2]
    assert params["limit"] == 5
    assert params["level"] == 0


@pytest.mark.asyncio
async def test_select_communities_semantic_failsafe_on_error():
    store = _FakeVecStore(raise_=True)
    refs = await select_communities_semantic(
        store, [0.1], level=0, limit=5,
    )
    assert refs == []


# ── _cosine (pure) ──────────────────────────────────────────────────


def test_cosine_orthogonal_identical_empty():
    assert _cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert _cosine([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)
    assert _cosine([], [1.0]) == 0.0
    assert _cosine([0.0, 0.0], [1.0, 1.0]) == 0.0      # zero norm → 0
    assert _cosine([1.0, 0.0], [1.0, 0.0, 0.0]) == 0.0  # length mismatch → 0


# ── select_communities_descent (stubbed tree store) ─────────────────


class _FakeTreeStore:
    """Serves a fixed community tree for descent traversal.

    ``roots`` are the level-0 rows; ``children`` maps a community_id to its
    finer child rows.  Each row carries a ``report_vec``."""

    def __init__(self, roots, children=None, raise_=False):
        self._roots = roots
        self._children = children or {}
        self._raise = raise_
        self.queries: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        self.queries.append((cypher, param_map or {}))
        if self._raise:
            raise RuntimeError("neo4j down")
        if "PARENT_OF" in cypher:
            return self._children.get(param_map.get("community_id"), [])
        return self._roots


def _row(cid, level, vec, summary=None):
    return {
        "community_id": cid,
        "level": level,
        "summary": summary or f"community {cid}",
        "report_vec": vec,
    }


@pytest.mark.asyncio
async def test_descent_returns_finest_relevant_child():
    # Two roots: 1 is query-relevant (aligned vec) with level-1 children,
    # 2 is irrelevant (orthogonal).  Child 11 aligns with the query; 12 not.
    roots = [
        _row(1, 0, [1.0, 0.0]),   # relevant root, has children
        _row(2, 0, [0.0, 1.0]),   # irrelevant root, leaf
    ]
    children = {
        1: [_row(11, 1, [1.0, 0.0]), _row(12, 1, [0.0, 1.0])],
    }
    store = _FakeTreeStore(roots=roots, children=children)
    refs = await select_communities_descent(store, [1.0, 0.0], budget=2)
    ids = {r.community_id for r in refs}
    # the finest relevant child (11) is selected, NOT its parent (1).
    assert "11" in ids
    assert "1" not in ids
    # all selected are leaves (level 1 child or childless root 2).
    assert all(r.level in (0, 1) for r in refs)
    levels = {r.community_id: r.level for r in refs}
    assert levels["11"] == 1


@pytest.mark.asyncio
async def test_descent_respects_budget():
    roots = [_row(i, 0, [1.0, 0.0]) for i in range(5)]  # all leaves
    store = _FakeTreeStore(roots=roots, children={})
    refs = await select_communities_descent(store, [1.0, 0.0], budget=2)
    assert len(refs) == 2


@pytest.mark.asyncio
async def test_descent_no_children_fallback_top_root_by_cosine():
    # Only level 0 exists → no leaf via PARENT_OF; childless roots are
    # selected directly, ranked by cosine, capped at budget.
    roots = [
        _row(1, 0, [0.0, 1.0]),   # less relevant
        _row(2, 0, [1.0, 0.0]),   # most relevant
        _row(3, 0, [0.7, 0.7]),
    ]
    store = _FakeTreeStore(roots=roots, children={})
    refs = await select_communities_descent(store, [1.0, 0.0], budget=2)
    # childless roots become leaves; the two most query-relevant kept.
    assert {r.community_id for r in refs} == {"2", "3"}


@pytest.mark.asyncio
async def test_descent_failsafe_on_store_error():
    store = _FakeTreeStore(roots=[], raise_=True)
    refs = await select_communities_descent(store, [1.0, 0.0], budget=3)
    assert refs == []


# ── map_communities selection switch ────────────────────────────────


@pytest.mark.asyncio
async def test_map_communities_semantic_uses_knn(monkeypatch):
    rows = [{"community_id": 9, "level": 0, "summary": "строители"}]
    store = _FakeVecStore(rows=rows)
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    monkeypatch.setattr(gs_mod, "_get_embed_model", lambda: _FakeEmbed())
    out = await map_communities(MapCommunitiesParams(
        query="строители", selection="semantic",
    ))
    assert [c.community_id for c in out.communities] == ["9"]
    # used the vector index, never read the lexical summaries cypher
    assert all("queryNodes" in q for q, _ in store.queries)


@pytest.mark.asyncio
async def test_map_communities_semantic_empty_falls_back_to_lexical(monkeypatch):
    # semantic returns no rows → fall back to lexical read.
    rows_for_semantic: list = []

    class _SwitchStore:
        def __init__(self):
            self.queries: list[tuple[str, dict]] = []

        def structured_query(self, cypher, param_map=None):
            self.queries.append((cypher, param_map or {}))
            if "queryNodes" in cypher:
                return rows_for_semantic
            # lexical path
            return [{"community_id": 1, "level": 0,
                     "summary": "строители", "member_count": 3}]

    store = _SwitchStore()
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    monkeypatch.setattr(gs_mod, "_get_embed_model", lambda: _FakeEmbed())
    out = await map_communities(MapCommunitiesParams(
        query="строители", selection="semantic",
    ))
    assert [c.community_id for c in out.communities] == ["1"]
    # the lexical cypher was read after the empty semantic result
    assert any("MATCH (c:Community" in q for q, _ in store.queries)


@pytest.mark.asyncio
async def test_map_communities_semantic_embed_error_falls_back(monkeypatch):
    store = _FakeStore(rows=[
        {"community_id": 2, "level": 0, "summary": "строители", "member_count": 1},
    ])
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    monkeypatch.setattr(
        gs_mod, "_get_embed_model", lambda: _FakeEmbed(raise_=True),
    )
    out = await map_communities(MapCommunitiesParams(
        query="строители", selection="semantic",
    ))
    assert [c.community_id for c in out.communities] == ["2"]


@pytest.mark.asyncio
async def test_map_communities_descent_uses_descent(monkeypatch):
    roots = [_row(1, 0, [1.0, 0.0]), _row(2, 0, [0.0, 1.0])]
    children = {1: [_row(11, 1, [1.0, 0.0])]}
    store = _FakeTreeStore(roots=roots, children=children)
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    monkeypatch.setattr(gs_mod, "_get_embed_model", lambda: _FakeEmbed([1.0, 0.0]))
    out = await map_communities(MapCommunitiesParams(
        query="строители", selection="descent", limit=5,
    ))
    ids = {c.community_id for c in out.communities}
    # descended to the finest relevant child, never read the lexical cypher.
    assert "11" in ids
    assert not any("c.summary IS NOT NULL" in q for q, _ in store.queries)


@pytest.mark.asyncio
async def test_map_communities_descent_empty_falls_back_to_lexical(monkeypatch):
    class _SwitchStore:
        def __init__(self):
            self.queries: list[tuple[str, dict]] = []

        def structured_query(self, cypher, param_map=None):
            self.queries.append((cypher, param_map or {}))
            if "PARENT_OF" in cypher:
                return []
            if "report_vec" in cypher:   # descent root read → empty
                return []
            # lexical summaries path
            return [{"community_id": 1, "level": 0,
                     "summary": "строители", "member_count": 3}]

    store = _SwitchStore()
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)
    monkeypatch.setattr(gs_mod, "_get_embed_model", lambda: _FakeEmbed([1.0, 0.0]))
    out = await map_communities(MapCommunitiesParams(
        query="строители", selection="descent",
    ))
    assert [c.community_id for c in out.communities] == ["1"]
    assert any("c.summary IS NOT NULL" in q for q, _ in store.queries)


@pytest.mark.asyncio
async def test_map_communities_lexical_default_unchanged(monkeypatch):
    # default selection="lexical" never embeds / queries the vector index.
    store = _FakeStore(rows=[
        {"community_id": 7, "level": 0, "summary": "строители", "member_count": 5},
    ])
    monkeypatch.setattr(gs_mod, "_get_store", lambda: store)

    def _boom_embed():
        raise AssertionError("lexical path must not embed")

    monkeypatch.setattr(gs_mod, "_get_embed_model", _boom_embed)
    out = await map_communities(MapCommunitiesParams(query="строители"))
    assert [c.community_id for c in out.communities] == ["7"]


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
        query="строители", community_id="3", summary="строительные фирмы",
    ))
    assert out.community_id == "3"
    assert out.score == 1.0
    assert "строител" in out.partial.lower()


@pytest.mark.asyncio
async def test_map_partial_self_drops_irrelevant(monkeypatch):
    monkeypatch.setattr(gs_mod, "_get_map_llm", lambda: _FakeLLM("НЕТ"))
    out = await map_community_partial(MapPartialParams(
        query="строители", community_id="4", summary="поставщики еды",
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
        query="q", community_id="5", summary="x",
    ))
    assert out.score == 0.0


# ── workflow pure helpers ───────────────────────────────────────────


def test_build_map_specs_one_per_community():
    comms = [
        CommunitySummaryRef(community_id="1", summary="a"),
        CommunitySummaryRef(community_id="2", summary="b"),
    ]
    specs = build_map_specs(comms, query="q")
    assert [s.community_id for s in specs] == ["1", "2"]
    assert all(isinstance(s, MapPartialParams) for s in specs)
    assert specs[0].query == "q"


def test_partials_to_sources_drops_empty_and_zero_score():
    partials = [
        MapPartialResult(community_id="1", partial="relevant", score=1.0),
        MapPartialResult(community_id="2", partial="", score=0.0),
        MapPartialResult(community_id="3", partial="dropped", score=0.0),
    ]
    sources = partials_to_sources(partials)
    assert [s.chunk_id for s in sources] == ["community:1"]
    assert sources[0].metadata["community_id"] == "1"


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

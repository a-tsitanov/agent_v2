"""Unit tests for the unified graph+vector rerank step (Search R5).

The heavy bge cross-encoder is never loaded here: we cover the pure
pool-prep helper directly and exercise the ``rerank_sources`` activity
with a stub reranker injected via the ``_search_deps`` cache. No
Temporal env needed — the activity is a plain async fn over stubbed
deps, mirroring the test pattern for the other search activities.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow import _search_deps
from src.workflow.contracts import RerankParams, SerializedNode
from src.workflow.search.activities import rerank as rerank_mod
from src.workflow.search.activities.rerank import (
    append_unranked_remainder,
    prepare_rerank_pool,
    rerank_sources,
)


def _n(cid: str, text: str = "t", score: float = 0.5) -> SerializedNode:
    return SerializedNode(chunk_id=cid, text=text, score=score)


# ── pure helper: unified pool merge + dedup-before-rerank ───────────


def test_prepare_pool_dedups_across_graph_and_vector():
    """A chunk surfacing from BOTH graph and vector retrieval collapses
    to one entry before reranking (first occurrence wins)."""
    pool = [_n("g1"), _n("shared"), _n("g2"), _n("shared"), _n("v1")]
    out = prepare_rerank_pool(pool)
    assert [n.chunk_id for n in out] == ["g1", "shared", "g2", "v1"]


def test_prepare_pool_empty():
    assert prepare_rerank_pool([]) == []


# ── pure helper: nothing-lost safety net for the model's fixed cap ──
#
# ``get_reranker`` (src/workflow/_search_deps.py) builds the cached
# cross-encoder as a scorer fixed at ``_RERANK_SCORE_CAP`` (256) — its
# docstring says the ``top_n`` passed in no longer sizes the build.  So
# ``postprocess_nodes`` never returns more than that cap, regardless of
# ``params.top_n``.  For a pool bigger than the cap, the model-scored
# list is missing pool members it never touched.  ``append_unranked_
# remainder`` restores them, unranked, after the ranked head.


def test_append_remainder_appends_missing_pool_members_in_pool_order():
    """Members the model never scored come back AFTER the ranked ones,
    in their ORIGINAL pool order (not reranked, not reversed)."""
    pool = [_n("a"), _n("b"), _n("c"), _n("d")]
    ranked = [pool[2], pool[0]]  # only c, a were scored; b, d were not
    out = append_unranked_remainder(ranked, pool)
    assert [n.chunk_id for n in out] == ["c", "a", "b", "d"]


def test_append_remainder_is_noop_when_ranked_has_every_pool_member():
    """Pool at/under the cap: ranked already contains everything, so the
    remainder is empty and ranked comes back untouched (existing
    behaviour for pools under the cap)."""
    pool = [_n("a"), _n("b"), _n("c")]
    ranked = [pool[1], pool[2], pool[0]]  # full pool, just reordered
    out = append_unranked_remainder(ranked, pool)
    assert out == ranked


def test_append_remainder_all_missing_when_ranked_is_empty():
    pool = [_n("a"), _n("b")]
    out = append_unranked_remainder([], pool)
    assert [n.chunk_id for n in out] == ["a", "b"]


# ── activity: reranks over the COMBINED pool, respects top_n ────────


class _StubReranker:
    """Records the node-set + query it was asked to rerank; returns the
    first ``top_n`` nodes in descending score so the activity's
    top-N truncation is observable."""

    def __init__(self, top_n: int) -> None:
        self.top_n = top_n
        self.seen_node_ids: list[str] = []
        self.seen_query: str | None = None

    def postprocess_nodes(self, nodes, query_str=None):
        self.seen_node_ids = [n.node.node_id for n in nodes]
        self.seen_query = query_str
        ordered = sorted(nodes, key=lambda n: n.score or 0.0, reverse=True)
        return ordered[: self.top_n]


@pytest.fixture
def _reset_deps():
    _search_deps.reset_for_tests()
    yield
    _search_deps.reset_for_tests()


@pytest.fixture(autouse=True)
def _stub_activity_ctx(monkeypatch):
    """No live Temporal — stub the activity heartbeat/logger context."""
    mock = MagicMock()
    mock.heartbeat = MagicMock()
    mock.logger = MagicMock()
    monkeypatch.setattr(rerank_mod, "activity", mock)


@pytest.mark.asyncio
async def test_rerank_runs_over_unified_pool_and_truncates(monkeypatch, _reset_deps):
    """Graph + vector chunks (with a cross-source duplicate) are reranked
    in ONE pass over the deduped union, then truncated to top_n."""
    stub = _StubReranker(top_n=2)

    async def _get_reranker(top_n):
        # top_n threaded through to the model factory.
        stub.top_n = top_n
        return stub

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)

    # graph pool: g1, shared ; vector pool: shared (dup), v1
    params = RerankParams(
        query="кто такой Иванов?",
        sources=[
            _n("g1", score=0.1),
            _n("shared", score=0.9),
            _n("shared", score=0.9),
            _n("v1", score=0.5),
        ],
        top_n=2,
    )
    out = await rerank_sources(params)

    # Reranker saw the UNIFIED, deduped pool (3 unique chunks).
    assert stub.seen_node_ids == ["g1", "shared", "v1"]
    assert stub.seen_query == "кто такой Иванов?"
    # top_n=2 honoured; highest-score chunks survive (shared, v1).
    assert [n.chunk_id for n in out.sources] == ["shared", "v1"]


@pytest.mark.asyncio
async def test_rerank_empty_pool_skips_model(monkeypatch, _reset_deps):
    """No sources → no model load, returns empty (fail-safe / cheap)."""
    called = False

    async def _get_reranker(top_n):
        nonlocal called
        called = True
        raise AssertionError("must not build reranker for empty pool")

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)
    out = await rerank_sources(RerankParams(query="q", sources=[], top_n=5))
    assert out.sources == []
    assert called is False


# ── activity: nothing lost when the pool exceeds the model's fixed cap ──


class _CappedStubReranker:
    """Mimics the cached cross-encoder's fixed ``_RERANK_SCORE_CAP``: its
    OWN cap never changes no matter what ``top_n`` the caller asks
    ``get_reranker`` for — matching the REAL ``get_reranker``'s current
    behaviour (see its docstring: "top_n ... no longer used to size the
    build"). ``postprocess_nodes`` therefore returns at most ``cap``
    nodes even when the pool is bigger."""

    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.seen_node_ids: list[str] = []

    def postprocess_nodes(self, nodes, query_str=None):
        self.seen_node_ids = [n.node.node_id for n in nodes]
        ordered = sorted(nodes, key=lambda n: n.score or 0.0, reverse=True)
        return ordered[: self.cap]


@pytest.mark.asyncio
async def test_rerank_pool_over_cap_returns_full_pool_ranked_then_remainder(
    monkeypatch, _reset_deps,
):
    """A pool bigger than the model's fixed cap must not lose chunks:
    the model-scored head comes back ranked best-first, the never-scored
    tail comes back appended in its ORIGINAL pool order — same chunk_id
    set, same count as the pool (Task 1's "nothing is lost" constraint,
    now also true when the pool exceeds ``_RERANK_SCORE_CAP``)."""
    stub = _CappedStubReranker(cap=3)

    async def _get_reranker(top_n):
        return stub

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)

    pool_sources = [_n(f"c{i}", score=float(i)) for i in range(6)]  # c0..c5
    params = RerankParams(query="q", sources=pool_sources, top_n=len(pool_sources))
    out = await rerank_sources(params)

    # nothing lost: same set, same count as the pool.
    assert len(out.sources) == 6
    assert {n.chunk_id for n in out.sources} == {f"c{i}" for i in range(6)}
    # model-scored head, best-first (highest raw score first, cap=3) ...
    assert [n.chunk_id for n in out.sources[:3]] == ["c5", "c4", "c3"]
    # ... then the never-scored remainder, in ORIGINAL pool order.
    assert [n.chunk_id for n in out.sources[3:]] == ["c0", "c1", "c2"]


@pytest.mark.asyncio
async def test_rerank_pool_under_cap_unaffected_by_remainder_append(
    monkeypatch, _reset_deps,
):
    """Pool comfortably under the cap: the remainder is always empty, so
    a caller-requested top_n < pool size still truncates exactly as
    before (existing behaviour unchanged)."""
    stub = _CappedStubReranker(cap=256)

    async def _get_reranker(top_n):
        return stub

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)

    pool_sources = [_n("a", score=0.1), _n("b", score=0.9), _n("c", score=0.5)]
    out = await rerank_sources(
        RerankParams(query="q", sources=pool_sources, top_n=2),
    )
    assert [n.chunk_id for n in out.sources] == ["b", "c"]

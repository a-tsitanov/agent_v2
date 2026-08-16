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
    select_rerank_candidates,
    split_into_blocks,
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


# ── pure helper: bounding what the cross-encoder actually scores ────
#
# Cost is linear in chunks scored and the pool grows with the corpus, so
# "score everything" is a promise the activity's timeout cannot keep —
# in production it stopped keeping it, at a 139-chunk pool.
#
# The subtlety is WHICH chunks to score. The merged pool is a plain
# concatenation of one block per sub-question (plus one per coverage
# round); each block is sorted internally, but the scores are not
# comparable ACROSS blocks — resolving that is the cross-encoder's whole
# job. So the pool's leading slice is not its best chunks, it is its
# first sub-question, and a naive `pool[:n]` drops whole facets.


def _block(prefix: str, n: int, hi: float, lo: float) -> list[SerializedNode]:
    """One sub-question's hits: descending scores, its own range."""
    step = (hi - lo) / max(n - 1, 1)
    return [_n(f"{prefix}{i}", score=hi - step * i) for i in range(n)]


def test_split_into_blocks_cuts_the_pool_back_up():
    pool = [_n(f"c{i}") for i in range(6)]
    assert [
        [n.chunk_id for n in b] for b in split_into_blocks(pool, [2, 3, 1])
    ] == [["c0", "c1"], ["c2", "c3", "c4"], ["c5"]]


def test_split_into_blocks_falls_back_when_sizes_do_not_fit():
    """Sizes that do not describe THIS pool would silently mis-slice it.
    Treat any inconsistency as "structure unknown" — one block — rather
    than splitting on wrong boundaries."""
    pool = [_n(f"c{i}") for i in range(6)]
    for bad in ([], [2, 2], [7], [3, -1, 4]):
        assert split_into_blocks(pool, bad) == [pool]


def test_select_candidates_returns_pool_when_unbounded():
    """``max_candidates<=0`` is the pre-existing "score the whole pool"
    behaviour and must stay a no-op."""
    pool = [_n(f"c{i}") for i in range(5)]
    assert select_rerank_candidates(pool, [2, 3], 0) == pool
    assert select_rerank_candidates(pool, [2, 3], -1) == pool


def test_select_candidates_returns_pool_when_it_already_fits():
    pool = [_n(f"c{i}") for i in range(5)]
    assert select_rerank_candidates(pool, [2, 3], 5) == pool
    assert select_rerank_candidates(pool, [2, 3], 99) == pool


def test_select_candidates_draws_round_robin_by_rank():
    """Each block's best first, then each block's second-best, and so on
    — never one block's whole head before another block's best."""
    pool = _block("a", 3, 0.9, 0.7) + _block("b", 3, 0.5, 0.3)
    out = select_rerank_candidates(pool, [3, 3], 4)
    assert [n.chunk_id for n in out] == ["a0", "b0", "a1", "b1"]


def test_select_candidates_keeps_drawing_from_blocks_that_still_have_chunks():
    """A short block runs out; the rest must keep contributing rather
    than the round stopping at the first exhausted block."""
    pool = _block("a", 1, 0.9, 0.9) + _block("b", 4, 0.5, 0.2)
    out = select_rerank_candidates(pool, [1, 4], 4)
    assert [n.chunk_id for n in out] == ["a0", "b0", "b1", "b2"]


def test_select_candidates_without_block_sizes_degrades_to_the_head():
    """No structure known → the leading slice is all we can justify."""
    pool = [_n(f"c{i}") for i in range(6)]
    out = select_rerank_candidates(pool, [], 3)
    assert [n.chunk_id for n in out] == ["c0", "c1", "c2"]


def test_select_candidates_beats_a_leading_slice_on_a_production_pool():
    """Regression for the shape that actually broke: a 139-chunk pool of
    6 blocks whose LAST block (the coverage round, issued precisely
    because the first five left a gap) carries the highest scores in the
    whole pool.

    `pool[:40]` there reached 2 of 6 blocks and contained none of the
    pool's ten highest-scoring chunks. Round-robin reaches every block
    and recovers most of them. Sizes and score ranges are taken from the
    measured production run."""
    spec = [
        ("b1", 30, 0.475, 0.425),
        ("b2", 26, 0.458, 0.411),
        ("b3", 25, 0.438, 0.387),
        ("b4", 21, 0.441, 0.387),
        ("b5", 13, 0.432, 0.399),
        ("b6", 24, 0.516, 0.478),  # coverage round — best of the pool
    ]
    blocks = [_block(p, n, hi, lo) for p, n, hi, lo in spec]
    pool = [n for b in blocks for n in b]
    sizes = [len(b) for b in blocks]
    assert len(pool) == 139

    best_10 = {
        n.chunk_id
        for n in sorted(pool, key=lambda s: s.score, reverse=True)[:10]
    }
    picked = select_rerank_candidates(pool, sizes, 40)
    picked_ids = {n.chunk_id for n in picked}
    head_ids = {n.chunk_id for n in pool[:40]}

    assert len(picked) == 40
    # every facet represented ...
    assert {i.chunk_id[:2] for i in picked} == {"b1", "b2", "b3", "b4", "b5", "b6"}
    # ... and the pool's strongest chunks recovered, which the leading
    # slice misses entirely.
    assert len(head_ids & best_10) == 0
    assert len(picked_ids & best_10) >= 6


# ── activity: the bound applies, and nothing is lost ────────────────


@pytest.mark.asyncio
async def test_rerank_scores_only_the_candidates_but_returns_the_whole_pool(
    monkeypatch, _reset_deps,
):
    """The model sees `max_candidates` chunks drawn across blocks; the
    unscored rest still comes back, after the ranked head, in pool
    order. Bounding the model's work must not shrink the result."""
    stub = _CappedStubReranker(cap=256)

    async def _get_reranker(top_n):
        return stub

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)

    pool = _block("a", 4, 0.9, 0.6) + _block("b", 4, 0.5, 0.2)
    out = await rerank_sources(RerankParams(
        query="q",
        sources=pool,
        top_n=len(pool),
        block_sizes=[4, 4],
        max_candidates=4,
    ))

    # Exactly 4 chunks reached the model, one rank at a time per block.
    assert stub.seen_node_ids == ["a0", "b0", "a1", "b1"]
    # Nothing lost: the full pool comes back.
    assert len(out.sources) == 8
    assert {n.chunk_id for n in out.sources} == {n.chunk_id for n in pool}
    # Scored head first (best-first), then the untouched tail in pool order.
    assert [n.chunk_id for n in out.sources[:4]] == ["a0", "a1", "b0", "b1"]
    assert [n.chunk_id for n in out.sources[4:]] == ["a2", "a3", "b2", "b3"]


@pytest.mark.asyncio
async def test_rerank_unbounded_params_score_the_whole_pool(
    monkeypatch, _reset_deps,
):
    """Defaults (no bound, no block sizes) keep the old behaviour, so
    existing callers are unaffected."""
    stub = _CappedStubReranker(cap=256)

    async def _get_reranker(top_n):
        return stub

    monkeypatch.setattr(rerank_mod, "get_reranker", _get_reranker)

    pool = [_n(f"c{i}", score=float(i)) for i in range(5)]
    await rerank_sources(RerankParams(query="q", sources=pool, top_n=5))
    assert stub.seen_node_ids == [f"c{i}" for i in range(5)]

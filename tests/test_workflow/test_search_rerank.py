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

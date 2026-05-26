"""Synthesize-call-spec tests (Search R5).

The orchestrator pins the final ``synthesize_answer`` to the large-tier
queue and sets ``use_synthesis_llm=True``.  Direct workflow execution is
Temporal-gated (see test_search_orchestrator.py), so we assert the
*call spec* via a tiny pure helper instead — no live Temporal env.
"""

from __future__ import annotations

from src.config import settings
from src.workflow.contracts import SerializedNode
from src.workflow.search.orchestrator import build_synthesize_call


def _n(cid: str) -> SerializedNode:
    return SerializedNode(chunk_id=cid, text="t", score=0.5)


def test_synthesize_pinned_to_large_queue_and_large_tier():
    sources = [_n("a"), _n("b")]
    queue, params = build_synthesize_call(
        query="кто Иванов?", sources=sources, max_refinements=3,
    )
    # Scheduled on the dedicated large-tier queue.
    assert queue == settings.temporal.large_task_queue
    assert queue == "kb-search-large"
    # Large synthesis tier (build_synthesis_llm) + simple mode.
    assert params.use_synthesis_llm is True
    assert params.mode == "simple"
    assert params.query == "кто Иванов?"
    assert [n.chunk_id for n in params.accumulated] == ["a", "b"]
    assert params.max_refinements == 3

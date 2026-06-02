"""Pure-helper tests for SearchOrchestratorWorkflow (no Temporal needed).

Kept OUT of ``test_search_orchestrator.py`` because that module is
gated behind a live-Temporal skipif; these helpers are Temporal-free
and must always run.
"""

from __future__ import annotations

from src.workflow.contracts import SerializedNode


def test_cap_synth_sources_bounds_pool():
    from src.workflow.search.orchestrator import cap_synth_sources

    pool = [SerializedNode(chunk_id=str(i), text="t") for i in range(20)]
    out = cap_synth_sources(pool, 5)
    assert len(out) == 5
    assert [n.chunk_id for n in out] == ["0", "1", "2", "3", "4"]
    # top_n<=0 → return as-is (defensive: never silently empty the context)
    assert cap_synth_sources(pool, 0) == pool


def test_distinct_doc_ids_dedups_in_order():
    from src.workflow.search.orchestrator import distinct_doc_ids

    pool = [
        SerializedNode(chunk_id="c1", text="t", metadata={"doc_id": "d1"}),
        SerializedNode(chunk_id="c2", text="t", metadata={"doc_id": "d2"}),
        SerializedNode(chunk_id="c3", text="t", metadata={"doc_id": "d1"}),
        SerializedNode(chunk_id="c4", text="t", metadata={}),  # no doc_id
    ]
    assert distinct_doc_ids(pool) == ["d1", "d2"]

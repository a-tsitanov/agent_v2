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


# ── rerank sizing: candidate bound + the timeout derived from it ────
#
# `rerank_sources` was timing out in production on a fixed 3-minute
# `start_to_close`: cross-encoder cost is linear in chunks scored, the
# merged pool grew from 30 to 139 chunks as the corpus grew, and the
# constant did not move. Both numbers are now derived, so neither a
# bigger pool nor a bigger `rerank_top_n` can silently re-create that.


def test_rerank_candidate_count_scales_with_synthesis_cap():
    from src.workflow.search.orchestrator import rerank_candidate_count

    # The shortlist is a multiple of what actually reaches synthesis.
    assert rerank_candidate_count(20) == 40
    assert rerank_candidate_count(30) == 60


def test_rerank_candidate_count_treats_uncapped_synthesis_as_unbounded():
    """``rerank_top_n<=0`` means "no synthesis cap" for
    ``cap_synth_sources``; the candidate bound reads it the same way, so
    the two cannot disagree about what a non-positive cap means."""
    from src.workflow.search.orchestrator import rerank_candidate_count

    assert rerank_candidate_count(0) == 0
    assert rerank_candidate_count(-1) == 0


def test_rerank_timeouts_grow_with_the_candidate_bound():
    from src.workflow.search.orchestrator import rerank_timeouts

    start_40, _ = rerank_timeouts(40)
    start_60, _ = rerank_timeouts(60)
    # 60s overhead + 4s per candidate.
    assert start_40.total_seconds() == 220
    assert start_60.total_seconds() == 300
    assert start_60 > start_40


def test_rerank_timeouts_never_drop_below_the_old_constant():
    """A small bound must not make the budget *tighter* than the 3
    minutes that were there before — model load alone can eat most of
    a short budget on a cold worker."""
    from src.workflow.search.orchestrator import rerank_timeouts

    start, _ = rerank_timeouts(1)
    assert start.total_seconds() == 180


def test_rerank_timeouts_cover_every_retry_attempt():
    """``schedule_to_close`` must outlast all three FAST_RETRY attempts
    plus backoff, or the last attempt is cut short by the outer bound
    and the retry budget is a lie."""
    from src.workflow.search.orchestrator import rerank_timeouts

    for bound in (0, 1, 40, 60):
        start, schedule = rerank_timeouts(bound)
        assert schedule.total_seconds() > 3 * start.total_seconds()


def test_rerank_timeouts_unbounded_candidates_keep_a_flat_ceiling():
    """No candidate bound → no derivable budget; fall back to a generous
    constant rather than computing one from a number that means
    "unbounded"."""
    from src.workflow.search.orchestrator import rerank_timeouts

    start, _ = rerank_timeouts(0)
    assert start.total_seconds() == 600

"""Unit tests for the pure admission state machine (Track 5).

The Temporal scheduler shell is thin; all the scheduling LOGIC lives in
``AdmissionState`` so it's deterministic + testable without infra:
  * at most K documents in flight,
  * FIFO order,
  * a document runs to completion before the next is admitted (K=1),
  * a completed document frees exactly one slot,
  * duplicate submits are ignored.
"""

from __future__ import annotations

from src.workflow.admission import AdmissionState


def test_k1_one_document_at_a_time_fifo():
    s = AdmissionState(max_inflight=1)
    for d in ("a", "b", "c"):
        s.submit(d)
    assert s.admit_ready() == ["a"]   # only the first starts
    assert s.admit_ready() == []      # slot is full — b/c wait
    s.complete("a")
    assert s.admit_ready() == ["b"]   # a finished → b admitted
    s.complete("b")
    assert s.admit_ready() == ["c"]


def test_k2_admits_two_then_blocks():
    s = AdmissionState(max_inflight=2)
    for d in ("a", "b", "c"):
        s.submit(d)
    assert s.admit_ready() == ["a", "b"]
    assert s.admit_ready() == []
    s.complete("a")
    assert s.admit_ready() == ["c"]


def test_duplicate_submit_ignored():
    s = AdmissionState(max_inflight=1)
    s.submit("a")
    s.submit("a")           # already pending
    assert s.admit_ready() == ["a"]
    assert s.pending == []
    s.submit("a")           # already in flight
    assert s.admit_ready() == []


def test_complete_unknown_is_noop():
    s = AdmissionState(max_inflight=1)
    s.complete("ghost")     # no raise, no negative slots
    assert s.inflight == set()
    s.submit("a")
    assert s.admit_ready() == ["a"]


def test_pending_ids_preserves_order_for_carryover():
    s = AdmissionState(max_inflight=1)
    for d in ("a", "b", "c"):
        s.submit(d)
    s.admit_ready()         # a → inflight
    assert s.pending == ["b", "c"]   # carryover order for continue_as_new


def test_scheduler_set_max_inflight_signal_updates_state():
    """The set_max_inflight signal live-updates K on the running singleton."""
    from src.workflow.ingest_scheduler import IngestSchedulerWorkflow
    wf = IngestSchedulerWorkflow()           # __init__ sets plain attrs, no Temporal ctx
    assert wf._state.max_inflight == 1       # initial
    wf.set_max_inflight(5)
    assert wf._state.max_inflight == 5
    wf.set_max_inflight(0)                    # clamped to >= 1
    assert wf._state.max_inflight == 1


# ── history-bounding recycle (drain-then-continue_as_new) ─────────────
# Regression: the singleton scheduler must recycle (continue_as_new) on a
# HISTORY threshold even while documents are in flight / queued.  The old
# guard required full quiescence (`not running and not pending`), which
# under sustained ingest never arrives → history grows unbounded → replay
# eventually exceeds the workflow-task timeout → the workflow wedges and
# "everything stalls".

def test_should_recycle_fires_at_threshold_despite_active_work():
    from src.workflow.ingest_scheduler import (
        _HISTORY_RECYCLE_THRESHOLD,
        IngestSchedulerWorkflow,
    )
    wf = IngestSchedulerWorkflow()
    # Simulate a busy scheduler: one doc in flight, one queued.
    wf._state.submit("a")
    wf._state.admit_ready()                   # a → inflight
    wf._state.submit("b")                     # b pending
    wf._running["a"] = object()               # pretend child task is live

    # Below threshold: keep running, no recycle.
    assert wf._should_recycle(_HISTORY_RECYCLE_THRESHOLD - 1) is False
    # At/over threshold: recycle even though work is active — the OLD
    # `not running and not pending` guard would have blocked forever here.
    assert wf._should_recycle(_HISTORY_RECYCLE_THRESHOLD) is True
    assert wf._should_recycle(_HISTORY_RECYCLE_THRESHOLD + 10_000) is True


def test_carry_forward_preserves_pending_params_in_fifo_order():
    """continue_as_new must hand the still-queued docs to the next run
    so nothing is dropped on recycle."""
    from src.workflow.contracts import IngestParams
    from src.workflow.ingest_scheduler import IngestSchedulerWorkflow
    wf = IngestSchedulerWorkflow()
    for d in ("a", "b", "c"):
        wf._params[d] = IngestParams(doc_id=d, path=f"/{d}")
        wf._state.submit(d)
    wf._state.admit_ready()                   # a → inflight; b, c pending
    carried = wf._carry_forward()
    assert [p.doc_id for p in carried] == ["b", "c"]
    # in-flight doc is NOT re-queued (it finishes during drain before recycle)
    assert "a" not in [p.doc_id for p in carried]

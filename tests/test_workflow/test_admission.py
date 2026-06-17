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

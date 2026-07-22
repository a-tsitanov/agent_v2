"""Routing dispatch tests (Search R7a).

The route→workflow mapping lives in the pure ``dispatch_for_route``
helper so it's unit-testable without a live Temporal env (the
AutoSearchWorkflow body that uses it is Temporal-gated)."""

from __future__ import annotations

from src.workflow.search.router_wf import (
    DRIFT_WF,
    GLOBAL_WF,
    LOCAL_WF,
    dispatch_for_route,
)


def test_dispatch_known_routes():
    assert dispatch_for_route("local") == LOCAL_WF
    assert dispatch_for_route("global") == GLOBAL_WF
    assert dispatch_for_route("drift") == DRIFT_WF


def test_dispatch_unknown_falls_back_to_local():
    assert dispatch_for_route("???") == LOCAL_WF  # type: ignore[arg-type]


def test_dispatch_date_scoped_downgrades_global_to_local():
    # GLOBAL cannot honour a date bound → a date-scoped question the router
    # classified as global must run LOCAL instead (which applies the bound).
    assert dispatch_for_route("global", date_scoped=True) == LOCAL_WF


def test_dispatch_date_scoped_leaves_local_and_drift_intact():
    assert dispatch_for_route("local", date_scoped=True) == LOCAL_WF
    assert dispatch_for_route("drift", date_scoped=True) == DRIFT_WF


def test_dispatch_global_kept_when_not_date_scoped():
    assert dispatch_for_route("global", date_scoped=False) == GLOBAL_WF
    assert dispatch_for_route("global") == GLOBAL_WF


def test_merge_doc_ids_unions_in_order():
    from src.workflow.search.router_wf import merge_doc_ids

    assert merge_doc_ids(["d1", "d2"], ["d2", "d3"]) == ["d1", "d2", "d3"]

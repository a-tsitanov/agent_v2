"""Routing + drift entry workflows (Search R7a).

Two thin coordinator workflows that compose the existing local
(``SearchOrchestratorWorkflow``) and global (``GlobalSearchWorkflow``)
flows WITHOUT modifying either of them:

  * ``DriftSearchWorkflow`` — the "drift" mode: run the local plan-execute
    flow FIRST (concrete chunk evidence), then EXPAND it with corpus-level
    community context by running the global map-reduce with ``drift_mode``
    and the local sources as the drift seed.  Local evidence leads; the
    community partials broaden it; one large-tier REDUCE produces the
    final answer.  A bounded mechanism — exactly one local pass + one
    global pass, no open-ended loop.
  * ``AutoSearchWorkflow`` — the "auto" mode: ``route_query`` (small tier)
    classifies the question, then dispatches to the matching flow as a
    CHILD workflow (local → orchestrator, global → global, drift → drift).
    Fail-safe routing (the activity defaults to "local") means a flaky
    router degrades to the safe local flow.

The dispatch decision is extracted into the pure ``dispatch_for_route``
helper so the route→workflow mapping is unit-testable outside Temporal.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        GlobalSearchParams,
        OrchestratorParams,
        RouteLabel,
        RouteParams,
        RouteResult,
        SearchOutcome,
    )
    from src.workflow.search._retry import (
        FAST_RETRY, LLM_SCHEDULE_TO_CLOSE, LLM_START_TO_CLOSE,
    )

from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow

# Workflow names dispatched per route — string handles so the pure helper
# carries no Temporal import (and the route handler can assert the mapping).
LOCAL_WF = "SearchOrchestratorWorkflow"
GLOBAL_WF = "GlobalSearchWorkflow"
DRIFT_WF = "DriftSearchWorkflow"


def dispatch_for_route(route: RouteLabel) -> str:
    """Pure: map a route label to the workflow that serves it.

    Unknown / unexpected labels fall back to the LOCAL flow (the safe
    default — mirrors ``route_query``'s own fail-safe)."""
    return {
        "local": LOCAL_WF,
        "global": GLOBAL_WF,
        "drift": DRIFT_WF,
    }.get(route, LOCAL_WF)


def merge_doc_ids(local: list[str], glob: list[str]) -> list[str]:
    """Union of two doc_id lists, order-preserving, deduped."""
    out = list(local)
    for d in glob:
        if d not in out:
            out.append(d)
    return out


@workflow.defn
class DriftSearchWorkflow:
    """Drift mode — local pass, then global community expansion."""

    @workflow.run
    async def run(
        self,
        local_params: OrchestratorParams,
        global_params: GlobalSearchParams,
    ) -> SearchOutcome:
        log = workflow.logger
        log.info("drift_search start  query=%s", local_params.query[:80])

        # 1. Local plan-execute pass (concrete chunk evidence) — REUSE the
        #    existing orchestrator unchanged as a child workflow.
        local: SearchOutcome = await workflow.execute_child_workflow(
            SearchOrchestratorWorkflow.run,
            local_params,
            id=f"{workflow.info().workflow_id}-local",
            result_type=SearchOutcome,
        )

        # 2. Global expansion seeded with the local sources.  drift_mode
        #    merges the local sources AHEAD of the community partials in the
        #    REDUCE context and labels the outcome "drift".
        drift_global = global_params.model_copy(update={"drift_mode": True})
        outcome: SearchOutcome = await workflow.execute_child_workflow(
            GlobalSearchWorkflow.run,
            args=[drift_global, list(local.sources)],
            id=f"{workflow.info().workflow_id}-global",
            result_type=SearchOutcome,
        )
        return outcome.model_copy(update={
            "documents": merge_doc_ids(list(local.documents), list(outcome.documents)),
        })


@workflow.defn
class AutoSearchWorkflow:
    """Auto mode — route the question, then dispatch to local/global/drift."""

    def __init__(self) -> None:
        self._state: dict = {"phase": "init", "route": ""}

    @workflow.query
    def get_state(self) -> dict:
        return dict(self._state)

    @workflow.run
    async def run(
        self,
        local_params: OrchestratorParams,
        global_params: GlobalSearchParams,
    ) -> SearchOutcome:
        log = workflow.logger
        self._state["phase"] = "route"
        route_res: RouteResult = await workflow.execute_activity(
            "route_query",
            RouteParams(query=local_params.query),
            result_type=RouteResult,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )
        target = dispatch_for_route(route_res.route)
        self._state["route"] = route_res.route
        self._state["phase"] = "dispatch"
        log.info("auto_search  route=%s → %s", route_res.route, target)

        wf_id = workflow.info().workflow_id
        if target == GLOBAL_WF:
            return await workflow.execute_child_workflow(
                GlobalSearchWorkflow.run,
                global_params,
                id=f"{wf_id}-global",
                result_type=SearchOutcome,
            )
        if target == DRIFT_WF:
            return await workflow.execute_child_workflow(
                DriftSearchWorkflow.run,
                args=[local_params, global_params],
                id=f"{wf_id}-drift",
                result_type=SearchOutcome,
            )
        # local (default) — the existing R2–R5 orchestrator unchanged.
        return await workflow.execute_child_workflow(
            SearchOrchestratorWorkflow.run,
            local_params,
            id=f"{wf_id}-local",
            result_type=SearchOutcome,
        )

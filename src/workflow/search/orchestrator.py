"""``SearchOrchestratorWorkflow`` — plan-execute-synthesize (R2).

Thin coordinator replacing the open-ended ReAct loop with a fixed,
parallel pipeline:

  1. ``plan_subquestions``  — split the question into atomic sub-Qs
     (small planner model; ``[query]`` for atomic questions).
  2. fan-out — one ``SubQueryRetrievalWorkflow`` child per sub-question,
     run in PARALLEL via ``asyncio.gather`` over
     ``workflow.execute_child_workflow``.
  3. merge — union all children's sources, dedup by chunk_id.
  4. ``synthesize_answer`` — ONE final synthesis (large model) over the
     merged sources, returning the SAME ``SearchOutcome`` shape the
     legacy ``SearchWorkflow`` returns (so route handlers are reusable).

No "LLM picks next tool" step anywhere — the only LLM calls are the
up-front planner and the final synthesizer.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        AgenticStepStatDict,
        OrchestratorParams,
        PlanParams,
        PlanResult,
        SearchOutcome,
        SubQueryParams,
        SubQueryResult,
        SynthesizeParams,
        SynthesizeResult,
    )
    from src.workflow.search._merge import merge_subquery_sources

from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow

_FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)


@workflow.defn
class SearchOrchestratorWorkflow:
    """Plan-execute-synthesize search session (local mode)."""

    def __init__(self) -> None:
        self._state: dict = {
            "phase": "init",
            "n_subqueries": 0,
            "n_sources": 0,
        }

    @workflow.query
    def get_state(self) -> dict:
        """Progress snapshot (mirrors SearchWorkflow.get_state)."""
        return dict(self._state)

    @workflow.run
    async def run(self, params: OrchestratorParams) -> SearchOutcome:
        log = workflow.logger
        t_start = workflow.now()
        log.info(
            "orchestrator start  query=%s  max_sub=%d",
            params.query[:80], params.max_subqueries,
        )

        # ── 1. plan ─────────────────────────────────────────────────
        self._state["phase"] = "plan"
        plan: PlanResult = await workflow.execute_activity(
            "plan_subquestions",
            PlanParams(query=params.query, max_subqueries=params.max_subqueries),
            result_type=PlanResult,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10),
            retry_policy=_FAST_RETRY,
        )
        subquestions = plan.subquestions or [params.query]
        self._state["n_subqueries"] = len(subquestions)
        log.info("orchestrator  planned %d sub-questions", len(subquestions))

        # ── 2. fan-out: one child workflow per sub-question, parallel ─
        self._state["phase"] = "retrieve"
        child_coros = [
            workflow.execute_child_workflow(
                SubQueryRetrievalWorkflow.run,
                SubQueryParams(
                    subquestion=sub,
                    top_k=params.top_k,
                    distill_enabled=params.distill_enabled,
                    distill_min_chars=params.distill_min_chars,
                ),
                id=f"{workflow.info().workflow_id}-sub-{i}",
                result_type=SubQueryResult,
            )
            for i, sub in enumerate(subquestions)
        ]
        child_results: list[SubQueryResult] = await asyncio.gather(*child_coros)

        # ── 3. merge + dedup by chunk_id across all sub-questions ────
        merged = merge_subquery_sources([r.sources for r in child_results])
        self._state["n_sources"] = len(merged)
        log.info(
            "orchestrator  merged %d sources from %d children",
            len(merged), len(child_results),
        )

        # Per-sub-question telemetry — reuse the legacy step-stats shape
        # so the response model maps unchanged (one entry per child).
        step_stats = [
            AgenticStepStatDict(
                step=i + 1,
                tool_name="subquery_retrieval",
                tool_args={"subquestion": r.subquestion},
                observation_summary=f"{len(r.sources)} sources",
            )
            for i, r in enumerate(child_results)
        ]

        # ── 4. synthesize once (large model) ────────────────────────
        self._state["phase"] = "synthesize"
        synth: SynthesizeResult = await workflow.execute_activity(
            "synthesize_answer",
            SynthesizeParams(
                query=params.query,
                mode="simple",
                accumulated=merged,
                max_refinements=params.max_refinements,
                use_synthesis_llm=True,
            ),
            result_type=SynthesizeResult,
            start_to_close_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(minutes=15),
            retry_policy=_FAST_RETRY,
        )

        self._state["phase"] = "done"
        latency_ms = int((workflow.now() - t_start).total_seconds() * 1000)
        return SearchOutcome(
            query=params.query,
            mode="simple",
            answer=synth.text,
            sources=merged,
            step_stats=step_stats,
            citations=list(synth.citations),
            uncertainties=list(synth.uncertainties),
            refinement_rounds=synth.refinement_rounds,
            latency_ms=latency_ms,
        )

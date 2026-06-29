"""AnalyticalQueryWorkflow — plan → execute primitives → synthesize + provenance."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.analytics.contracts import (
        AnalysisPlan,
        AnalyticsOutcome,
        AnalyzeParams,
        ExecInput,
        PlanInput,
        StepResult,
        SynthInput,
        SynthResult,
    )
    from src.analytics.provenance import assemble_provenance
    from src.config import settings
    from src.workflow.search._retry import (
        FAST_RETRY,
        LLM_SCHEDULE_TO_CLOSE,
        LLM_START_TO_CLOSE,
    )

# DB/graph step is cheaper than LLM — tighter start_to_close, same schedule budget.
_DB_START = timedelta(minutes=5)


@workflow.defn
class AnalyticalQueryWorkflow:
    """Plan-execute-synthesize analytical session."""

    def __init__(self) -> None:
        self._state: dict = {"phase": "init"}

    @workflow.query
    def get_state(self) -> dict:
        """Progress snapshot (mirrors SearchOrchestratorWorkflow.get_state)."""
        return dict(self._state)

    @workflow.run
    async def run(self, params: AnalyzeParams) -> AnalyticsOutcome:
        t_start = workflow.now()

        # ── 1. plan ─────────────────────────────────────────────────
        self._state["phase"] = "plan"
        plan: AnalysisPlan = await workflow.execute_activity(
            "analytical_plan",
            PlanInput(query=params.query, max_steps=settings.analytics.max_steps),
            result_type=AnalysisPlan,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )

        # ── 2. execute each plan step sequentially ───────────────────
        self._state["phase"] = "execute"
        steps: list[StepResult] = []
        for call in plan.steps[: settings.analytics.max_steps]:
            sr: StepResult = await workflow.execute_activity(
                "execute_step",
                ExecInput(
                    call=call,
                    top_n=params.top_n,
                    date_from_epoch=params.date_from_epoch,
                    date_to_epoch=params.date_to_epoch,
                ),
                result_type=StepResult,
                start_to_close_timeout=_DB_START,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                retry_policy=FAST_RETRY,
            )
            steps.append(sr)

        # ── 3. synthesize (large model, dedicated large queue) ───────
        self._state["phase"] = "synthesize"
        synth: SynthResult = await workflow.execute_activity(
            "synthesize_analytical",
            SynthInput(query=params.query, steps=steps),
            result_type=SynthResult,
            task_queue=settings.temporal.large_task_queue,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )

        elapsed = int((workflow.now() - t_start).total_seconds() * 1000)
        self._state["phase"] = "done"
        return AnalyticsOutcome(
            query=params.query,
            answer=synth.text,
            provenance=assemble_provenance(plan, steps, elapsed),
            latency_ms=elapsed,
        )

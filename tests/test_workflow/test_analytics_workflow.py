"""AnalyticalQueryWorkflow integration test.

Uses the bundled time-skipping Temporal test server (NOT docker-compose
localhost:7233), matching the idiom in test_search_drift_roundtrip.py.
Skips gracefully if the test-server binary is unavailable.

Activities are stubbed by name — no Milvus / Neo4j / LLM required.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from src.analytics.contracts import (
    AnalysisPlan,
    AnalyzeParams,
    PrimitiveCall,
    StepResult,
    SynthResult,
)
from src.config import settings
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow


@activity.defn(name="analytical_plan")
async def _plan(p) -> AnalysisPlan:
    return AnalysisPlan(
        route="catalog",
        steps=[PrimitiveCall(primitive="count_entities", params={})],
        reason="r",
    )


@activity.defn(name="execute_step")
async def _exec(p) -> StepResult:
    return StepResult(
        primitive="count_entities",
        rows=[{"n": 7}],
        row_count=1,
        cypher="MATCH ...",
    )


@activity.defn(name="synthesize_analytical")
async def _synth(p) -> SynthResult:
    return SynthResult(text="There are 7 entities.")


@pytest.mark.asyncio
async def test_analytical_workflow_plan_execute_synthesize(monkeypatch):
    # Pin the large task queue to the test queue so one Worker handles all activities.
    test_queue = f"test-analytics-{uuid.uuid4().hex}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", test_queue)

    try:
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover — infra-dependent
        pytest.skip(f"time-skipping test server unavailable: {exc}")

    async with (
        env,
        Worker(
            env.client,
            task_queue=test_queue,
            workflows=[AnalyticalQueryWorkflow],
            activities=[_plan, _exec, _synth],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ),
    ):
        out = await env.client.execute_workflow(
            AnalyticalQueryWorkflow.run,
            AnalyzeParams(query="how many entities"),
            id=f"t-{uuid.uuid4().hex}",
            task_queue=test_queue,
        )

    assert out.answer == "There are 7 entities."
    assert out.provenance.steps[0].rows == [{"n": 7}]
    assert out.provenance.plan_reason == "r"

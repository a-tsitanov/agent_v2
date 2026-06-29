"""AnalyticsMaterializeWorkflow integration test.

Uses the bundled time-skipping Temporal test server.
Skips gracefully if the test-server binary is unavailable.

Activities are stubbed by name — no GDS / Neo4j required.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from src.analytics.contracts import MaterializeParams, StageResult
from src.workflow.analytics.materialize_workflow import AnalyticsMaterializeWorkflow


@activity.defn(name="materialize_centrality")
async def _c(p) -> StageResult:
    return StageResult(written=10)


@activity.defn(name="materialize_link_prediction")
async def _l(p) -> StageResult:
    return StageResult(written=3)


@activity.defn(name="materialize_risk")
async def _r(p) -> StageResult:
    return StageResult(written=7)


@pytest.mark.asyncio
async def test_materialize_workflow_aggregates():
    try:
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover — infra-dependent
        pytest.skip(f"temporal test server unavailable: {exc}")

    async with (
        env,
        Worker(
            env.client,
            task_queue="t-mat",
            workflows=[AnalyticsMaterializeWorkflow],
            activities=[_c, _l, _r],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ),
    ):
        out = await env.client.execute_workflow(
            AnalyticsMaterializeWorkflow.run,
            MaterializeParams(),
            id=f"mat-{uuid.uuid4().hex}",
            task_queue="t-mat",
        )

    assert out.centrality_written == 10
    assert out.links_written == 3
    assert out.risk_written == 7
    assert out.errors == []


@pytest.mark.asyncio
async def test_materialize_workflow_skips_link_and_risk():
    """When link_prediction=False and risk=False, only centrality runs."""
    try:
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover — infra-dependent
        pytest.skip(f"temporal test server unavailable: {exc}")

    async with (
        env,
        Worker(
            env.client,
            task_queue="t-mat-skip",
            workflows=[AnalyticsMaterializeWorkflow],
            activities=[_c, _l, _r],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ),
    ):
        out = await env.client.execute_workflow(
            AnalyticsMaterializeWorkflow.run,
            MaterializeParams(link_prediction=False, risk=False),
            id=f"mat-{uuid.uuid4().hex}",
            task_queue="t-mat-skip",
        )

    assert out.centrality_written == 10
    assert out.links_written == 0
    assert out.risk_written == 0
    assert out.errors == []


@activity.defn(name="materialize_centrality_err")
async def _c_err(p) -> StageResult:
    return StageResult(written=0, error="gds timeout")


@pytest.mark.asyncio
async def test_materialize_workflow_collects_errors():
    """Errors from StageResult.error are collected into MaterializeResult.errors."""

    @activity.defn(name="materialize_centrality")
    async def _c_with_err(p) -> StageResult:
        return StageResult(written=0, error="gds timeout")

    try:
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover — infra-dependent
        pytest.skip(f"temporal test server unavailable: {exc}")

    async with (
        env,
        Worker(
            env.client,
            task_queue="t-mat-err",
            workflows=[AnalyticsMaterializeWorkflow],
            activities=[_c_with_err, _l, _r],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ),
    ):
        out = await env.client.execute_workflow(
            AnalyticsMaterializeWorkflow.run,
            MaterializeParams(),
            id=f"mat-{uuid.uuid4().hex}",
            task_queue="t-mat-err",
        )

    assert out.centrality_written == 0
    assert "centrality: gds timeout" in out.errors

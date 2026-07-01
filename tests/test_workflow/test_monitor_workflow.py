"""MonitorSweepWorkflow integration test.

Uses the bundled time-skipping Temporal test server.
Skips gracefully if the test-server binary is unavailable.

The detect_alerts activity is stubbed — no Neo4j required.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from src.analytics.contracts import DeliverIn, DeliverResult, MonitorIn, MonitorResult
from src.workflow.monitor.workflow import MonitorSweepWorkflow


@activity.defn(name="detect_alerts")
async def _detect_stub(p: MonitorIn) -> MonitorResult:
    return MonitorResult(new_connection_alerts=2, risk_rise_alerts=1, burst_alerts=3)


@activity.defn(name="deliver_alerts")
async def _deliver_stub(p: DeliverIn) -> DeliverResult:
    return DeliverResult(delivered=4, failed=1)


@pytest.mark.asyncio
async def test_monitor_sweep_workflow_returns_tally() -> None:
    """MonitorSweepWorkflow runs detect_alerts then deliver_alerts and rolls up both."""
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
            task_queue="t-mon",
            workflows=[MonitorSweepWorkflow],
            activities=[_detect_stub, _deliver_stub],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ),
    ):
        out = await env.client.execute_workflow(
            MonitorSweepWorkflow.run,
            id=f"mon-{uuid.uuid4().hex}",
            task_queue="t-mon",
        )

    assert out.new_connection_alerts == 2
    assert out.risk_rise_alerts == 1
    assert out.burst_alerts == 3
    assert out.delivered == 4
    assert out.failed == 1
    assert out.error == ""

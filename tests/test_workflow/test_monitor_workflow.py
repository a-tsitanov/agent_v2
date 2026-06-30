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

from src.analytics.contracts import MonitorIn, MonitorResult
from src.workflow.monitor.workflow import MonitorSweepWorkflow


@activity.defn(name="detect_alerts")
async def _detect_stub(p: MonitorIn) -> MonitorResult:
    return MonitorResult(new_connection_alerts=2, risk_rise_alerts=1)


@pytest.mark.asyncio
async def test_monitor_sweep_workflow_returns_tally() -> None:
    """MonitorSweepWorkflow routes detect_alerts and returns its tally."""
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
            activities=[_detect_stub],
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
    assert out.error == ""

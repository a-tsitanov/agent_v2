"""Arc 2 monitoring sweep — one-shot MonitorSweepWorkflow.

Builds MonitorIn from settings, delegates to the detect_alerts activity,
and returns the alert tally.  Designed to be scheduled via a Temporal
Schedule (see settings.monitor.sweep_interval_minutes).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.analytics.contracts import MonitorIn, MonitorResult
    from src.config import settings

_RETRY = RetryPolicy(
    maximum_attempts=2,
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
)


@workflow.defn
class MonitorSweepWorkflow:
    """One-shot sweep: detect new-connection + risk-rise alerts for watched entities."""

    @workflow.run
    async def run(self) -> MonitorResult:
        monitor_in = MonitorIn(
            window_days=settings.monitor.new_window_days,
            risk_rise_delta=settings.monitor.risk_rise_delta,
        )
        return await workflow.execute_activity(
            "detect_alerts",
            monitor_in,
            result_type=MonitorResult,
            start_to_close_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_RETRY,
        )

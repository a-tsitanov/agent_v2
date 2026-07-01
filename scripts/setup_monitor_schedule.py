"""Idempotently create/update the Temporal Schedule that runs
MonitorSweepWorkflow every MONITOR_SWEEP_INTERVAL_MINUTES. No-op if disabled.

Run: uv run python -m scripts.setup_monitor_schedule
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from loguru import logger
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
    ScheduleUpdate,
)

from src.config import settings
from src.workflow.client import get_temporal_client
from src.workflow.monitor.workflow import MonitorSweepWorkflow

_SCHEDULE_ID = "monitor-sweep"


async def _main() -> None:
    if not settings.monitor.enabled:
        logger.info("MONITOR_ENABLED=false — skipping monitor schedule")
        return
    client = await get_temporal_client()
    interval = timedelta(minutes=settings.monitor.sweep_interval_minutes)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            MonitorSweepWorkflow.run,
            id="monitor-sweep-scheduled",
            task_queue=settings.monitor.task_queue,
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=interval)]),
    )
    try:
        await client.create_schedule(_SCHEDULE_ID, schedule)
        logger.info(
            "created monitor schedule every {m}m", m=settings.monitor.sweep_interval_minutes
        )
    except Exception:  # already exists -> update
        handle = client.get_schedule_handle(_SCHEDULE_ID)
        await handle.update(lambda _i: ScheduleUpdate(schedule=schedule))
        logger.info("updated existing monitor schedule")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()

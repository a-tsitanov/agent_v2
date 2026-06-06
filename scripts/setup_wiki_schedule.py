"""Idempotently create/update the Temporal Schedule that runs
WikiSweepWorkflow every WIKI_SWEEP_INTERVAL_MINUTES. No-op if disabled.

Run: uv run python -m scripts.setup_wiki_schedule
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
from src.workflow.wiki.wiki_sweep import WikiSweepWorkflow

_SCHEDULE_ID = "wiki-sweep"


async def _main() -> None:
    if not settings.wiki.enabled:
        logger.info("WIKI_ENABLED=false — skipping wiki schedule")
        return
    client = await get_temporal_client()
    interval = timedelta(minutes=settings.wiki.sweep_interval_minutes)
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            WikiSweepWorkflow.run, id="wiki-sweep-scheduled",
            task_queue=settings.wiki.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=interval)]),
    )
    try:
        await client.create_schedule(_SCHEDULE_ID, schedule)
        logger.info("created wiki schedule every {m}m",
                    m=settings.wiki.sweep_interval_minutes)
    except Exception:  # already exists -> update
        handle = client.get_schedule_handle(_SCHEDULE_ID)
        await handle.update(lambda _i: ScheduleUpdate(schedule=schedule))
        logger.info("updated existing wiki schedule")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()

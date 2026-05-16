"""Temporal worker entry point.

Run with:
    uv run python -m src.workflow.worker

Polls the workflow + activity task queues and registers
`DocumentIngestWorkflow` plus every activity exported from
`src.workflow.activities`.
"""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import ALL_ACTIVITIES
from src.workflow.client import get_temporal_client
from src.workflow.document_ingest import DocumentIngestWorkflow


async def _run() -> None:
    client = await get_temporal_client()
    logger.info(
        "temporal worker  target={t}  queue={q}  concurrency={c}",
        t=settings.temporal.target, q=settings.temporal.task_queue,
        c=settings.temporal.activity_concurrency,
    )
    worker = Worker(
        client,
        task_queue=settings.temporal.task_queue,
        workflows=[DocumentIngestWorkflow],
        activities=ALL_ACTIVITIES,
        max_concurrent_activities=settings.temporal.activity_concurrency,
    )
    await worker.run()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

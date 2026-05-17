"""Temporal worker entry point.

Run with::

    uv run python -m src.workflow.worker

Starts two worker pools in the same process against the same
Temporal client:

* **main** — polls ``settings.temporal.task_queue`` with normal
  concurrency.  Hosts the workflow definition plus IO / embedding
  activities (fetch_source, parse_and_chunk, index_vector,
  inject_canonical, build_property_graph, finalize, mark_failed).

* **llm**  — polls ``settings.temporal.llm_task_queue`` with
  ``llm_activity_concurrency`` (default 1) so the GPU isn't asked
  to serve more than one extract_kg / merge_and_resolve at a time.

For a multi-GPU deployment, point ``TEMPORAL_LLM_ACTIVITY_CONCURRENCY``
at the right number; for a multi-machine deployment, run two
processes — one with `MAIN_ACTIVITIES` only, one with
`LLM_ACTIVITIES` on the GPU box.
"""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import LLM_ACTIVITIES, MAIN_ACTIVITIES
from src.workflow.client import get_temporal_client
from src.workflow.document_ingest import DocumentIngestWorkflow


async def _run() -> None:
    client = await get_temporal_client()
    logger.info(
        "temporal worker  target={t}  main_queue={mq}  main_concurrency={mc}  "
        "llm_queue={lq}  llm_concurrency={lc}",
        t=settings.temporal.target,
        mq=settings.temporal.task_queue,
        mc=settings.temporal.activity_concurrency,
        lq=settings.temporal.llm_task_queue,
        lc=settings.temporal.llm_activity_concurrency,
    )

    main_worker = Worker(
        client,
        task_queue=settings.temporal.task_queue,
        workflows=[DocumentIngestWorkflow],
        activities=MAIN_ACTIVITIES,
        max_concurrent_activities=settings.temporal.activity_concurrency,
    )
    llm_worker = Worker(
        client,
        task_queue=settings.temporal.llm_task_queue,
        activities=LLM_ACTIVITIES,
        max_concurrent_activities=settings.temporal.llm_activity_concurrency,
    )

    await asyncio.gather(main_worker.run(), llm_worker.run())


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

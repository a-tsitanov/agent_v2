"""Producer-side dispatch for document admission (Track B).

``/ingest`` calls :func:`submit_document`, which routes the document to
the configured backlog backend (``INGEST_QUEUE_BACKEND``):

* ``temporal`` (default) — signal-with-start the singleton
  ``IngestSchedulerWorkflow`` exactly as before (backlog lives in
  workflow state).
* ``rabbitmq`` — publish ``IngestParams`` to a durable queue; a separate
  consumer admits at most ``max_inflight`` at a time and starts the
  per-document workflow.  Keeps the bulk-insert backlog OUT of Temporal
  history (the singleton-as-queue choke).

The Temporal branch still raises ``WorkflowAlreadyStartedError`` straight
through, so the route keeps mapping it to HTTP 409.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from src.config import settings
from src.workflow.contracts import IngestParams, SchedulerParams
from src.workflow.ingest_scheduler import IngestSchedulerWorkflow


async def submit_document(client: Client, params: IngestParams) -> None:
    """Hand one document to the configured ingest backlog backend."""
    if settings.ingest_admission.backend == "rabbitmq":
        # Lazy import: aio_pika is only required when this backend is
        # actually selected (default is temporal).
        from src.ingest_queue.publisher import publish_ingest

        await publish_ingest(params)
        return
    await _submit_to_scheduler(client, params)


async def _submit_to_scheduler(client: Client, params: IngestParams) -> None:
    """Signal-with-start the admission singleton (the original path)."""
    await client.start_workflow(
        IngestSchedulerWorkflow.run,
        SchedulerParams(max_inflight=settings.ingest_admission.max_inflight),
        id="ingest-scheduler",
        # Dedicated queue: the scheduler runs on its own worker pool,
        # isolated from DocumentIngestWorkflow on `task_queue` (main).
        task_queue=settings.temporal.scheduler_task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        start_signal="submit",
        start_signal_args=[params],
        # Headroom over the 10s default so a one-off larger history (e.g.
        # right after a deploy) can't time out the workflow task and wedge
        # admission.  Only applies to a freshly-started singleton.
        task_timeout=timedelta(seconds=30),
    )

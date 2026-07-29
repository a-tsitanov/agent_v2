"""RabbitMQ consumer for the ingest queue (Track B).

Replaces the ``IngestSchedulerWorkflow`` admission role when
``INGEST_QUEUE_BACKEND=rabbitmq``.  Pulls from ``ingest.pending`` with
``prefetch_count`` = ``INGEST_ADMISSION_MAX_INFLIGHT`` (K) and, per
message, starts a ``DocumentIngestWorkflow`` and **awaits its
completion** before acking.  Awaiting is what makes prefetch the
admission ceiling: at most K messages are unacked at once, so at most K
documents run concurrently — same contract as the scheduler's K, but the
backlog lives in RabbitMQ, not Temporal history.

Failure handling per message:
* unparseable payload       → reject(requeue=False) → dead-letter
* workflow already started  → ATTACH to that run and await it, then as below
* workflow run failed       → reject(requeue=cfg.requeue_on_failure)
* transient/transport error → reject(requeue=True) → redelivered
* success                   → ack

The attach matters for the ceiling.  Acking a redelivery whose workflow is still
running frees the prefetch slot while the load continues, so K stops bounding
anything: 226 concurrent DocumentIngestWorkflow were observed against K=5 on
2026-07-28.  That saturated the host until postgres logged `FATAL: canceling
authentication due to timeout`, which broke more awaits, which caused more
redeliveries — a positive feedback loop.  Redelivery is routine, not exotic:
the transient branch below requeues deliberately.

Only a ``WorkflowFailureError`` is a verdict about the DOCUMENT — the workflow
ran and failed.  Everything else (Temporal RPC errors, dropped connections,
host starvation) breaks the ``execute_workflow`` await while saying nothing
about the document, so it is requeued rather than discarded.  Treating the two
alike silently destroyed data: on 2026-07-28 a host stall (postgres logged 15x
``FATAL: canceling authentication due to timeout``) put 770 messages in the DLQ
while only 48 workflows had actually Failed — 202 of the dead-lettered docs were
already ``completed`` and 182 still ``processing``.
"""

from __future__ import annotations

import asyncio

import aio_pika
from loguru import logger
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from src.config import RabbitMQSettings, settings
from src.ingest_queue.topology import declare_ingest_topology
from src.workflow.client import get_temporal_client
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow


async def _start_or_attach(client: Client, params: IngestParams) -> None:
    """Run this document to completion: start its workflow, or ATTACH to the
    run that already owns the id, and await either way.

    Attaching (rather than acking) is what keeps prefetch=K an actual ceiling.
    The message stays unacked for as long as the document is being processed,
    whoever started it.  Acking a redelivery of a still-running workflow frees
    the slot while the load continues — see the module docstring.

    Raises whatever the await raises, so ``handle_message`` applies the SAME
    failure/transient split to a started run and an attached one.
    """
    try:
        await client.execute_workflow(
            DocumentIngestWorkflow.run,
            params,
            id=f"ingest-{params.doc_id}",
            task_queue=settings.temporal.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        )
    except WorkflowAlreadyStartedError:
        # A run already owns this doc_id. If it is still going we block here
        # until it finishes; if it already finished, result() returns at once
        # (and re-raises its failure, which is the verdict we want).
        logger.warning(
            "duplicate ingest doc={d}; attaching to the running run",
            d=params.doc_id,
        )
        await client.get_workflow_handle(f"ingest-{params.doc_id}").result()


async def handle_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    client: Client,
    cfg: RabbitMQSettings,
) -> None:
    """Process one ingest message: start + await the document workflow,
    then ack/reject.  Pure of any connection setup so it unit-tests
    against fake message/client doubles."""
    try:
        params = IngestParams.model_validate_json(message.body)
    except Exception as exc:  # malformed payload — never retriable
        logger.error("poison ingest message; dead-lettering: {e}", e=exc)
        await message.reject(requeue=False)
        return

    try:
        await _start_or_attach(client, params)
    except WorkflowFailureError as exc:
        # The workflow RAN and failed — a verdict about this document.
        dest = "requeue" if cfg.requeue_on_failure else "dead-letter"
        logger.error(
            "ingest workflow failed doc={d} ({dest}): {e}",
            d=params.doc_id, dest=dest, e=exc,
        )
        await message.reject(requeue=cfg.requeue_on_failure)
        return
    except Exception as exc:
        # The await broke, not the document: RPC error, dropped connection,
        # starved host.  Requeue — discarding here loses a good document.
        logger.error(
            "ingest transport/transient error doc={d} (requeue): {e!r}",
            d=params.doc_id, e=exc,
        )
        await message.reject(requeue=True)
        return

    await message.ack()
    logger.info("ingest completed doc={d}; acked", d=params.doc_id)


async def run_consumer(stop_event: asyncio.Event | None = None) -> None:
    """Connect, set prefetch=K, and consume ``ingest.pending`` until
    ``stop_event`` is set (or forever).  Infra entrypoint — exercised
    against a live broker, not in unit tests."""
    cfg = settings.rabbitmq
    k = settings.ingest_admission.max_inflight
    client = await get_temporal_client()

    connection = await aio_pika.connect_robust(cfg.url)
    try:
        channel = await connection.channel()
        # global_=True → prefetch=K is shared across ALL consumers on this
        # channel, so total unacked (= documents in flight) ≤ K across every
        # configured queue, not K per queue. This keeps the admission ceiling
        # global even with N queues.
        await channel.set_qos(prefetch_count=k, global_=True)
        queues = await declare_ingest_topology(channel, cfg)
        logger.info(
            "ingest consumer up  queues={q}  prefetch(K,global)={k}",
            q=cfg.queues, k=k,
        )

        async def _on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            await handle_message(message, client, cfg)

        for q in queues:
            await q.consume(_on_message)
        await (stop_event or asyncio.Event()).wait()
    finally:
        await connection.close()


def main() -> None:
    """``python -m src.ingest_queue.consumer`` process entrypoint."""
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()

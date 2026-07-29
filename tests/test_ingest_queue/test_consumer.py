"""Consumer message-handling contract (Track B3).

No broker / no Temporal: a fake message (ack/reject doubles) and a fake
client exercise every ack/reject branch of ``handle_message``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError

from src.config import settings
from src.ingest_queue.consumer import handle_message
from src.workflow.contracts import IngestParams


class _FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.ack = AsyncMock()
        self.reject = AsyncMock()


def _good_body() -> bytes:
    return IngestParams(doc_id="d1", path="s3://b/d1/f.pdf").model_dump_json().encode()


@pytest.mark.asyncio
async def test_success_acks_after_workflow_completes() -> None:
    msg = _FakeMessage(_good_body())
    client = AsyncMock()

    await handle_message(msg, client, settings.rabbitmq)

    assert client.execute_workflow.await_count == 1
    _, kwargs = client.execute_workflow.call_args
    assert kwargs["id"] == "ingest-d1"
    assert kwargs["task_queue"] == settings.temporal.task_queue
    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_poison_payload_dead_lettered_without_workflow() -> None:
    msg = _FakeMessage(b"{not json")
    client = AsyncMock()

    await handle_message(msg, client, settings.rabbitmq)

    client.execute_workflow.assert_not_awaited()
    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


# ── admission ceiling: a duplicate must be AWAITED, not acked early ────────
#
# Admission is enforced by holding the message unacked while the workflow runs
# (prefetch=K, global).  Acking a redelivered message whose workflow is STILL
# RUNNING frees the prefetch slot while the load continues, so K stops being a
# ceiling.  Observed 2026-07-28: 226 concurrent DocumentIngestWorkflow against
# K=5 (45x), which saturated the host badly enough that postgres logged
# `FATAL: canceling authentication due to timeout` — that broke more awaits,
# which produced more redeliveries, which leaked more slots.
#
# Redelivery is not exotic: the transient-error branch requeues on purpose, so
# every transport blip produces a redelivery of a still-running workflow.


def _attachable(client: AsyncMock, on_result=None) -> MagicMock:
    """Make `client` raise AlreadyStarted, and hand back a handle double.

    `get_workflow_handle` is SYNC in temporalio and returns a handle whose
    `.result()` is async — mirrored here so the double can't pass against an
    implementation the real client would reject.
    """
    client.execute_workflow.side_effect = WorkflowAlreadyStartedError(
        "dup", "ingest-d1", run_id="r1",
    )
    handle = MagicMock()
    handle.result = AsyncMock(side_effect=on_result)
    client.get_workflow_handle = MagicMock(return_value=handle)
    return handle


@pytest.mark.asyncio
async def test_duplicate_doc_attaches_and_awaits_before_acking() -> None:
    order: list[str] = []
    msg = _FakeMessage(_good_body())
    msg.ack = AsyncMock(side_effect=lambda: order.append("ack"))
    client = AsyncMock()
    handle = _attachable(client, on_result=lambda: order.append("awaited"))

    await handle_message(msg, client, settings.rabbitmq)

    client.get_workflow_handle.assert_called_once_with("ingest-d1")
    handle.result.assert_awaited_once()
    # The ack must come AFTER the run finishes — that ordering IS the ceiling.
    assert order == ["awaited", "ack"]
    msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_doc_failure_is_not_acked_as_success(monkeypatch) -> None:
    """Attaching must not swallow the verdict: if the run we attached to
    failed, the message follows the workflow-failure path, not the ack path."""
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", False)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    # an exception INSTANCE as side_effect is raised by the mock
    _attachable(client, on_result=_workflow_failure())

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_doc_transient_attach_error_requeues() -> None:
    """A blip while attaching says nothing about the document either."""
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    _attachable(client, on_result=ConnectionError("connection reset"))

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()


def _workflow_failure() -> WorkflowFailureError:
    return WorkflowFailureError(cause=ApplicationError("parse blew up"))


@pytest.mark.asyncio
async def test_workflow_failure_dead_letters_by_default(monkeypatch) -> None:
    """The workflow RAN and returned a failure — that is a verdict about the
    document itself, so honour requeue_on_failure (default: dead-letter)."""
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", False)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = _workflow_failure()

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_workflow_failure_requeues_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", True)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = _workflow_failure()

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=True)


# ── transient infrastructure errors must NOT destroy the message ──────────
#
# `execute_workflow` starts the workflow AND awaits it — an await that can span
# the whole ingest.  Anything that breaks the await (Temporal RPC error, dropped
# connection, host starvation) says nothing about the document, but the handler
# used to reject every exception with requeue=False and discard it for good.
#
# Production evidence (2026-07-28): the host stalled hard enough that postgres
# logged 15x `FATAL: canceling authentication due to timeout`.  770 messages
# landed in the DLQ, yet only 48 workflows had actually Failed — 202 of the
# dead-lettered docs were already `completed` and 182 were still `processing`.
# 424 documents were thrown away while their workflow was fine.


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [
    RuntimeError("temporal down"),
    ConnectionError("connection reset"),
    TimeoutError("rpc deadline exceeded"),
    OSError("[Errno 111] Connect call failed"),
])
async def test_transient_error_requeues_regardless_of_config(
    monkeypatch, exc,
) -> None:
    # requeue_on_failure is about WORKFLOW verdicts; it must not cause a
    # transport blip to permanently discard a perfectly good document.
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", False)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = exc

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=True)
    msg.ack.assert_not_awaited()

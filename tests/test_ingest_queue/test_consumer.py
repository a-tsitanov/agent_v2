"""Consumer message-handling contract (Track B3).

No broker / no Temporal: a fake message (ack/reject doubles) and a fake
client exercise every ack/reject branch of ``handle_message``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

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


@pytest.mark.asyncio
async def test_duplicate_doc_is_acked() -> None:
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = WorkflowAlreadyStartedError(
        "dup", "ingest-d1", run_id="r1",
    )

    await handle_message(msg, client, settings.rabbitmq)

    msg.ack.assert_awaited_once()
    msg.reject.assert_not_awaited()


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

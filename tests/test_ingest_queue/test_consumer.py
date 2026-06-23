"""Consumer message-handling contract (Track B3).

No broker / no Temporal: a fake message (ack/reject doubles) and a fake
client exercise every ack/reject branch of ``handle_message``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from temporalio.exceptions import WorkflowAlreadyStartedError

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


@pytest.mark.asyncio
async def test_workflow_failure_dead_letters_by_default(monkeypatch) -> None:
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", False)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = RuntimeError("temporal down")

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=False)
    msg.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_workflow_failure_requeues_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(settings.rabbitmq, "requeue_on_failure", True)
    msg = _FakeMessage(_good_body())
    client = AsyncMock()
    client.execute_workflow.side_effect = RuntimeError("transient")

    await handle_message(msg, client, settings.rabbitmq)

    msg.reject.assert_awaited_once_with(requeue=True)

"""Producer dispatch by INGEST_QUEUE_BACKEND (Track B2).

No broker / no Temporal server: the client and the RabbitMQ publisher are
mocked so we assert ONLY the routing decision and the args each backend
is handed.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock

import pytest

from src.config import settings
from src.workflow.contracts import IngestParams
from src.workflow.ingest_submit import submit_document


def _params() -> IngestParams:
    return IngestParams(doc_id="doc-1", path="s3://bucket/doc-1/file.pdf")


@pytest.mark.asyncio
async def test_temporal_backend_signal_with_starts_scheduler(monkeypatch) -> None:
    monkeypatch.setattr(settings.ingest_admission, "backend", "temporal")
    client = AsyncMock()
    params = _params()

    await submit_document(client, params)

    assert client.start_workflow.await_count == 1
    _, kwargs = client.start_workflow.call_args
    assert kwargs["id"] == "ingest-scheduler"
    assert kwargs["task_queue"] == settings.temporal.scheduler_task_queue
    assert kwargs["start_signal"] == "submit"
    assert kwargs["start_signal_args"] == [params]


@pytest.mark.asyncio
async def test_rabbitmq_backend_publishes_and_skips_temporal(monkeypatch) -> None:
    monkeypatch.setattr(settings.ingest_admission, "backend", "rabbitmq")

    # Inject a fake publisher module so the lazy import in submit_document
    # resolves without aio_pika / a live broker.
    published: list[IngestParams] = []
    fake_mod = types.ModuleType("src.ingest_queue.publisher")

    async def _publish(p: IngestParams, queue: str | None = None) -> None:
        published.append((p, queue))

    fake_mod.publish_ingest = _publish
    monkeypatch.setitem(sys.modules, "src.ingest_queue.publisher", fake_mod)

    client = AsyncMock()
    params = _params()
    await submit_document(client, params, queue="ingest.bulk")

    assert published == [(params, "ingest.bulk")]  # queue forwarded to publisher
    assert client.start_workflow.await_count == 0  # never touched Temporal

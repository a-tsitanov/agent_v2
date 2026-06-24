"""Topology declares EVERY configured work queue with dead-letter +
consumer-timeout args (Track B multi-queue). No broker — a fake channel
records the declare calls."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import RabbitMQSettings
from src.ingest_queue.topology import declare_ingest_topology


@pytest.mark.asyncio
async def test_declares_all_queues_with_dlx_and_consumer_timeout() -> None:
    cfg = RabbitMQSettings(queues=["q1", "q2"], consumer_timeout_ms=123_000)

    channel = MagicMock()
    channel.declare_exchange = AsyncMock(return_value=MagicMock())
    dlq = MagicMock()
    dlq.bind = AsyncMock()
    q1, q2 = MagicMock(), MagicMock()
    # declare order: DLQ first, then one per configured queue.
    channel.declare_queue = AsyncMock(side_effect=[dlq, q1, q2])

    result = await declare_ingest_topology(channel, cfg)

    assert result == [q1, q2]  # work-queue objects in cfg.queues order
    work_calls = channel.declare_queue.call_args_list[1:]  # skip the DLQ
    assert [c.args[0] for c in work_calls] == ["q1", "q2"]
    for c in work_calls:
        arguments = c.kwargs["arguments"]
        assert arguments["x-dead-letter-exchange"] == cfg.dlx
        assert arguments["x-consumer-timeout"] == 123_000


def test_consumer_timeout_default_far_above_default_30min() -> None:
    # RabbitMQ's default is 30 min (1_800_000 ms); ours must dwarf it so a
    # slow document can't trip the channel-close → requeue storm.
    assert RabbitMQSettings().consumer_timeout_ms >= 1_800_000 * 10


def test_queues_parse_and_default():
    # comma-separated env string → list; first is the default.
    cfg = RabbitMQSettings(queues="ingest.pending, ingest.bulk ,")
    assert cfg.queues == ["ingest.pending", "ingest.bulk"]
    assert cfg.default_queue == "ingest.pending"
    # empty → falls back to the single default queue.
    assert RabbitMQSettings(queues="").queues == ["ingest.pending"]
    assert RabbitMQSettings().default_queue == "ingest.pending"

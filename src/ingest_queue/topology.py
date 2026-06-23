"""RabbitMQ topology for the ingest queue (Track B).

Declared (idempotently) by BOTH the producer and the consumer so the
first publish works even before any consumer has run, and a restarted
consumer re-asserts the same shapes:

* ``ingest.pending`` — durable work queue.  Messages that the consumer
  rejects (poison payload, or a workflow start that failed) are
  dead-lettered via ``x-dead-letter-exchange``.
* ``ingest.dlx`` / ``ingest.dlq`` — fanout dead-letter exchange + queue
  holding the rejected messages for operator inspection / replay.
"""

from __future__ import annotations

import aio_pika

from src.config import RabbitMQSettings


async def declare_ingest_topology(
    channel: aio_pika.abc.AbstractChannel, cfg: RabbitMQSettings
) -> aio_pika.abc.AbstractQueue:
    """Declare the DLX/DLQ and the main work queue; return the work
    queue.  Idempotent — safe to call on every connect."""
    dlx = await channel.declare_exchange(
        cfg.dlx, aio_pika.ExchangeType.FANOUT, durable=True,
    )
    dlq = await channel.declare_queue(cfg.dlq, durable=True)
    await dlq.bind(dlx)

    return await channel.declare_queue(
        cfg.queue,
        durable=True,
        arguments={"x-dead-letter-exchange": cfg.dlx},
    )

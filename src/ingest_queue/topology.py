"""RabbitMQ topology for the ingest queue (Track B).

Declared (idempotently) by BOTH the producer and the consumer so the
first publish works even before any consumer has run, and a restarted
consumer re-asserts the same shapes:

* one durable work queue per ``RabbitMQSettings.queues`` entry.  Messages
  the consumer rejects (poison payload, or a failed workflow start) are
  dead-lettered via ``x-dead-letter-exchange``.
* ``ingest.dlx`` / ``ingest.dlq`` — fanout dead-letter exchange + queue
  holding the rejected messages for operator inspection / replay.
"""

from __future__ import annotations

import aio_pika

from src.config import RabbitMQSettings


async def declare_ingest_topology(
    channel: aio_pika.abc.AbstractChannel, cfg: RabbitMQSettings
) -> list[aio_pika.abc.AbstractQueue]:
    """Declare the shared DLX/DLQ and EVERY configured work queue; return
    the work-queue objects (in ``cfg.queues`` order).  Idempotent — safe
    to call on every connect."""
    dlx = await channel.declare_exchange(
        cfg.dlx, aio_pika.ExchangeType.FANOUT, durable=True,
    )
    dlq = await channel.declare_queue(cfg.dlq, durable=True)
    await dlq.bind(dlx)

    # x-consumer-timeout: the consumer holds a delivery unacked for the
    # whole document workflow, so this must exceed the longest per-document
    # wall-clock — otherwise RabbitMQ force-closes the channel and requeues
    # every in-flight message, restarting workflows in a storm. See
    # RabbitMQSettings.consumer_timeout_ms.  All queues share one DLX.
    args = {
        "x-dead-letter-exchange": cfg.dlx,
        "x-consumer-timeout": cfg.consumer_timeout_ms,
        "x-max-priority": cfg.max_priority,
    }
    queues = []
    for name in cfg.queues:
        queues.append(
            await channel.declare_queue(name, durable=True, arguments=dict(args))
        )
    return queues

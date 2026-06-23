"""RabbitMQ producer for the ingest queue (Track B).

``/ingest`` → :func:`submit_document` → :func:`publish_ingest` when
``INGEST_QUEUE_BACKEND=rabbitmq``.  Publishes one persistent JSON
``IngestParams`` message per document to ``ingest.pending``; the backlog
then lives in RabbitMQ instead of the Temporal singleton's history.

A process-global robust connection is reused across publishes (mirrors
the get_pg_pool / cached-store singletons) — ``connect_robust``
transparently re-establishes the link across broker blips.
"""

from __future__ import annotations

import asyncio

import aio_pika
from loguru import logger

from src.config import settings
from src.ingest_queue.topology import declare_ingest_topology
from src.workflow.contracts import IngestParams

_connection: aio_pika.abc.AbstractRobustConnection | None = None
_lock = asyncio.Lock()


async def _get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """Return the per-process robust connection, opening it lazily."""
    global _connection
    if _connection is not None and not _connection.is_closed:
        return _connection
    async with _lock:
        if _connection is None or _connection.is_closed:
            _connection = await aio_pika.connect_robust(settings.rabbitmq.url)
    return _connection


async def publish_ingest(params: IngestParams) -> None:
    """Publish one document to the durable ``ingest.pending`` queue.

    ``message_id`` = ``doc_id`` so the payload is dedup-friendly and
    traceable; ``PERSISTENT`` so an enqueued document survives a broker
    restart before the consumer admits it."""
    cfg = settings.rabbitmq
    connection = await _get_connection()
    channel = await connection.channel()
    try:
        await declare_ingest_topology(channel, cfg)
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=params.model_dump_json().encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                message_id=params.doc_id,
            ),
            routing_key=cfg.queue,
        )
        logger.info("ingest published to rabbitmq  doc={d}", d=params.doc_id)
    finally:
        await channel.close()


async def close_publisher() -> None:
    """Close the shared connection on process shutdown (API lifespan)."""
    global _connection
    if _connection is not None and not _connection.is_closed:
        await _connection.close()
    _connection = None

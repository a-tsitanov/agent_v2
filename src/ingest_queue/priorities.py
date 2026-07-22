"""Ingest message priority levels (RabbitMQ backend).

A single work queue declared with ``x-max-priority`` carries two lanes:
live traffic at ``PRIO_LIVE`` and manual channel reingest at
``PRIO_BACKFILL``. RabbitMQ hands a lower-priority message to a free
consumer slot only when no higher-priority message is ready, so backfill
drains only when the live feed is idle.

Kept import-light (no aio_pika) so the API route and the tg_ingest script
can import these constants on any backend.
"""
from __future__ import annotations

PRIO_LIVE = 5
PRIO_BACKFILL = 0

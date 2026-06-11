"""Background heartbeater for long-running Temporal activities.

A long activity that never calls ``activity.heartbeat`` between its
start and finish hits ``heartbeat_timeout`` if the work (or the wait for
an LLM-pool slot) outruns that window.  Temporal then cancels and
retries it — which under backend saturation amplifies offered load into
a retry storm (see ``extract_kg``: one LLM call per chunk, no pulse
inside ``extractor.acall``).  ``heartbeat_every`` pulses on a timer for
the duration of the wrapped work so a progressing-but-slow activity is
never mistaken for a dead one.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from temporalio import activity


@asynccontextmanager
async def heartbeat_every(
    interval_s: float, detail: Any = None,
) -> AsyncIterator[None]:
    """Emit ``activity.heartbeat(detail)`` every ``interval_s`` seconds
    while the wrapped block runs.

    The first pulse fires after one interval (callers typically heartbeat
    once at entry already).  The background task is always cancelled on
    exit — normal or exceptional — and exceptions raised inside the
    wrapped block propagate unchanged.
    """

    async def _beat() -> None:
        while True:
            await asyncio.sleep(interval_s)
            if detail is None:
                activity.heartbeat()
            else:
                activity.heartbeat(detail)

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

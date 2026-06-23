"""Bounded retry for transient Neo4j write failures (Track A3).

Concurrent ``MERGE`` into shared canonical ``__Entity__`` hub nodes (the
merge/graph contention) makes Neo4j throw a RETRYABLE
``Neo.TransientError.*`` — deadlock detection or a lock-acquisition
timeout (the latter only appears once ``db.lock.acquisition.timeout`` is
set, see docker-compose A1).  The correct response to a transient is to
re-run the whole transaction, not fail the document; Temporal's
activity-level retry is too coarse (it replays the entire activity,
re-reading staging + rebuilding the index).  ``write_with_retry`` wraps
just the store write with a short bounded backoff.

Runs SYNC (callers invoke it inside ``asyncio.to_thread``), so the
backoff uses ``time.sleep`` — fine off the event loop.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from loguru import logger

from src.config import settings

# Neo4j status codes that mean "transient write contention — re-run the
# transaction".  Deadlock + lock-client races + lock-acquisition timeout.
_RETRYABLE_CODES: frozenset[str] = frozenset({
    "Neo.TransientError.Transaction.DeadlockDetected",
    "Neo.TransientError.Transaction.LockClientStopped",
    "Neo.TransientError.Transaction.LockAcquisitionTimeout",
    # "transaction has been terminated … retry in a new transaction" —
    # a lock-victim / guard-terminated tx. (Most TransientErrors are also
    # caught by the isinstance check below; listed here for clarity.)
    "Neo.TransientError.Transaction.Terminated",
})


def _is_transient(exc: BaseException) -> bool:
    """True for Neo4j transient/contention errors that are safe to retry.

    Matches the status ``code`` (works without importing neo4j) and, when
    the driver is importable, also any ``neo4j.exceptions.TransientError``
    instance — covering codes not in the explicit set above."""
    if getattr(exc, "code", None) in _RETRYABLE_CODES:
        return True
    try:
        from neo4j.exceptions import TransientError

        return isinstance(exc, TransientError)
    except Exception:
        return False


def write_with_retry[T](
    fn: Callable[..., T],
    *args: Any,
    max_attempts: int | None = None,
    base_delay_s: float | None = None,
    **kwargs: Any,
) -> T:
    """Call ``fn(*args, **kwargs)``, retrying on transient Neo4j errors.

    Up to ``max_attempts`` total tries (default from
    ``Neo4jSettings.write_retry_max_attempts``) with capped exponential
    backoff (``base_delay_s`` default from settings).  Non-transient
    errors propagate immediately; the final transient is re-raised after
    the attempts are spent (Temporal's activity retry is the backstop)."""
    cfg = settings.neo4j
    attempts = max_attempts if max_attempts is not None else cfg.write_retry_max_attempts
    delay = base_delay_s if base_delay_s is not None else cfg.write_retry_base_delay_s

    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if not _is_transient(exc):
                raise
            last = exc
            if attempt >= attempts:
                break
            logger.warning(
                "neo4j transient write error (attempt {a}/{n}, code={c}); "
                "retrying", a=attempt, n=attempts, c=getattr(exc, "code", "?"),
            )
            # Capped exponential backoff: base, 2·base, 4·base … ≤ 2s.
            time.sleep(min(delay * (2 ** (attempt - 1)), 2.0))

    assert last is not None  # only reached after a transient broke the loop
    logger.error(
        "neo4j write failed after {n} transient retries (code={c})",
        n=attempts, c=getattr(last, "code", "?"),
    )
    raise last

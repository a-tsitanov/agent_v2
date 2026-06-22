"""Shared retry policy for the R2 plan-execute search path.

Three retries, short backoff — search latency matters; we don't want
forever-retry like ingest.  Application-level failures (tool not found,
LLM 4xx) are non-retryable and bubble up.  Used by both
``SearchOrchestratorWorkflow`` and ``SubQueryRetrievalWorkflow``.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.common import RetryPolicy

FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
)

# Timeouts for activities that make an LLM call (planner, retrieve,
# coverage, route, per-community MAP partial, community summary, final
# synthesis).  A loaded local LLM / proxy can legitimately take many
# minutes on a long generation; a tight start-to-close lets Temporal kill
# a slow-but-healthy call and retry into the same slowness.  Floor the
# single-attempt ceiling at 1h; schedule-to-close leaves room for the
# 3 FAST_RETRY attempts.  Non-LLM activities (rerank cross-encoder, graph
# reads, GDS) keep their own tighter timeouts.
LLM_START_TO_CLOSE = timedelta(hours=1)
LLM_SCHEDULE_TO_CLOSE = timedelta(hours=3)

# Detect-communities is heavy and resource-bound: a true OOM/resource error
# will recur on retry and only pile load onto Neo4j/the worker, so it is
# non-retryable.  Transient transport errors still get a couple of tries.
DETECT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=2,
    non_retryable_error_types=["MemoryError"],
)

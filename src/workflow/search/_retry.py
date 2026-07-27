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

# Heartbeat window for the final synthesis.  The activity pulses every 30s
# THROUGH the compact-and-refine call (``heartbeat_every``), so a wedged
# attempt now surfaces in minutes instead of consuming the whole 1h
# start-to-close while the search chain waits behind it.  5m — wide enough
# that event-loop contention on a saturated worker cannot false-positive a
# healthy generation; the 1h ceiling is still the real upper bound.
LLM_HEARTBEAT_TIMEOUT = timedelta(minutes=5)

# Timeouts for detect_communities_activity.  The leidenalg backend clusters
# in a C extension (leidenalg/igraph ``find_partition``) that HOLDS the GIL
# for the entire compute, so the ``heartbeat_every`` background pulse cannot
# fire while clustering runs — a 150k-node / 1.2M-edge graph clusters in
# ~35s holding the GIL, and a production-scale graph runs for minutes.  A
# tight 2-minute heartbeat window therefore false-positives a healthy-but-
# busy detect as dead and kills it with ``timeout_type_heartbeat``.  Since
# heartbeating THROUGH a single GIL-held C call from a worker thread is
# impossible, the heartbeat window must simply exceed the compute; the
# start-to-close ceiling stays the real upper bound on a genuinely stuck
# (or OOM-dead) run.
DETECT_HEARTBEAT_TIMEOUT = timedelta(minutes=30)
DETECT_START_TO_CLOSE = timedelta(minutes=60)
DETECT_SCHEDULE_TO_CLOSE = timedelta(minutes=90)

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

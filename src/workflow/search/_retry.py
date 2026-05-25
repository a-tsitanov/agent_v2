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

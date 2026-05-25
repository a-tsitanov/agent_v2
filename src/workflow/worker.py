"""Temporal worker entry point.

Run with::

    uv run python -m src.workflow.worker

Starts two worker pools in the same process against the same
Temporal client:

* **main** — polls ``settings.temporal.task_queue`` with normal
  concurrency.  Hosts the workflow definition plus IO / embedding
  activities (fetch_source, parse_and_chunk, index_vector,
  inject_canonical, build_property_graph, finalize, mark_failed).

* **llm**  — polls ``settings.temporal.llm_task_queue`` with
  ``llm_activity_concurrency`` (default 1) so the GPU isn't asked
  to serve more than one extract_kg / merge_and_resolve at a time.

For a multi-GPU deployment, point ``TEMPORAL_LLM_ACTIVITY_CONCURRENCY``
at the right number; for a multi-machine deployment, run two
processes — one with `MAIN_ACTIVITIES` only, one with
`LLM_ACTIVITIES` on the GPU box.
"""

from __future__ import annotations

import asyncio

from loguru import logger
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import (
    LLM_ACTIVITIES,
    MAIN_ACTIVITIES,
    SEARCH_ACTIVITIES,
)
from src.workflow.document_ingest import DocumentIngestWorkflow
from src.workflow.graph_build import GraphBuildWorkflow
from src.workflow.search.activities import SEARCH_V2_ACTIVITIES
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow
from src.workflow.search_workflow import SearchWorkflow


def _build_runtime() -> Runtime | None:
    """Build a process-wide Temporal Runtime with a Prometheus exporter.

    Skipped when ``settings.metrics.enabled`` is false — caller passes
    ``None`` to ``Client.connect`` which uses Temporal's default
    no-telemetry runtime.
    """
    if not settings.metrics.enabled:
        return None
    logger.info(
        "temporal worker  prometheus exporter listening on {addr}",
        addr=settings.metrics.bind_address,
    )
    return Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(
                bind_address=settings.metrics.bind_address,
                durations_as_seconds=True,
            ),
        ),
    )


async def _run() -> None:
    # Surface LiteLLM model-config mistakes at boot, not at the
    # first activity that hits the proxy and gets a 500 (see
    # src/observability/litellm_models.py for what it catches).
    from src.observability.litellm_models import validate_litellm_models
    validate_litellm_models(source="worker")

    runtime = _build_runtime()
    client = await Client.connect(
        settings.temporal.target,
        namespace=settings.temporal.namespace,
        data_converter=pydantic_data_converter,
        runtime=runtime,
    )
    logger.info(
        "temporal worker  target={t}  main_queue={mq}  main_concurrency={mc}  "
        "llm_queue={lq}  llm_concurrency={lc}",
        t=settings.temporal.target,
        mq=settings.temporal.task_queue,
        mc=settings.temporal.activity_concurrency,
        lq=settings.temporal.llm_task_queue,
        lc=settings.temporal.llm_activity_concurrency,
    )

    main_worker = Worker(
        client,
        task_queue=settings.temporal.task_queue,
        workflows=[DocumentIngestWorkflow],
        activities=MAIN_ACTIVITIES,
        max_concurrent_activities=settings.temporal.activity_concurrency,
    )
    # GraphBuildWorkflow runs on the LLM queue alongside merge_and_resolve
    # + build_property_graph activities so the GPU-cap (concurrency=1)
    # serialises the heavy work — child workflow dispatch itself is
    # lightweight, the activities inside are the real LLM load.
    llm_worker = Worker(
        client,
        task_queue=settings.temporal.llm_task_queue,
        workflows=[GraphBuildWorkflow],
        activities=LLM_ACTIVITIES,
        max_concurrent_activities=settings.temporal.llm_activity_concurrency,
    )
    # SearchWorkflow lives on its own queue so concurrent search
    # sessions don't fight ingest for GPU budget.  Cap independently
    # via TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY (default 4 — assumes
    # LLM proxy can handle a small handful of parallel sessions).
    # Both flows share the search queue during the R2 parity window:
    # the legacy ReAct SearchWorkflow and the new plan-execute
    # SearchOrchestratorWorkflow + SubQueryRetrievalWorkflow child.  The
    # orchestrator reuses synthesize_answer (in SEARCH_ACTIVITIES) and
    # adds plan_subquestions + retrieve_subquestion (SEARCH_V2_ACTIVITIES).
    search_worker = Worker(
        client,
        task_queue=settings.temporal.search_task_queue,
        workflows=[
            SearchWorkflow,
            SearchOrchestratorWorkflow,
            SubQueryRetrievalWorkflow,
        ],
        activities=SEARCH_ACTIVITIES + SEARCH_V2_ACTIVITIES,
        max_concurrent_activities=settings.temporal.search_activity_concurrency,
    )
    logger.info(
        "temporal worker  search_queue={sq}  search_concurrency={sc}",
        sq=settings.temporal.search_task_queue,
        sc=settings.temporal.search_activity_concurrency,
    )

    await asyncio.gather(
        main_worker.run(), llm_worker.run(), search_worker.run(),
    )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

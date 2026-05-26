"""Temporal worker entry point.

Run with::

    uv run python -m src.workflow.worker

Starts several worker pools in the same process against the same
Temporal client:

* **main** — polls ``settings.temporal.task_queue`` with normal
  concurrency.  Hosts the workflow definition plus IO / embedding
  activities (fetch_source, parse_and_chunk, index_vector,
  inject_canonical, build_property_graph, finalize, mark_failed).

* **llm**  — polls ``settings.temporal.llm_task_queue`` with
  ``llm_activity_concurrency`` (default 1).  Hosts ONLY ``extract_kg``
  (``EXTRACT_ACTIVITIES``) so a burst of extracts has its own lane.

* **merge** — polls ``settings.temporal.merge_task_queue`` with
  ``merge_activity_concurrency`` (default 1).  Hosts
  ``GraphBuildWorkflow`` + ``MERGE_ACTIVITIES`` (merge_and_resolve +
  build_property_graph).  A separate lane from extract so a flood of
  extract_kg can no longer starve a document's merge (head-of-line
  blocking) — up to ~2 concurrent LLM tasks in flight.

For a multi-GPU deployment, point the per-queue
``TEMPORAL_*_ACTIVITY_CONCURRENCY`` vars at the right numbers; for a
multi-machine deployment, run separate processes — one with
`MAIN_ACTIVITIES`, and the LLM lanes (`EXTRACT_ACTIVITIES` /
`MERGE_ACTIVITIES`) on the GPU box.
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
    EXTRACT_ACTIVITIES,
    MAIN_ACTIVITIES,
    MERGE_ACTIVITIES,
    SEARCH_ACTIVITIES,
    synthesize_answer,
)
from src.workflow.document_ingest import DocumentIngestWorkflow
from src.workflow.graph_build import GraphBuildWorkflow
from src.workflow.search.activities import (
    GRAPH_BUILD_ACTIVITIES,
    SEARCH_V2_ACTIVITIES,
)
from src.workflow.search.community_wf import CommunityBuildWorkflow
from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.router_wf import AutoSearchWorkflow, DriftSearchWorkflow
from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow


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
        "llm_queue={lq}  llm_concurrency={lc}  "
        "merge_queue={mgq}  merge_concurrency={mgc}",
        t=settings.temporal.target,
        mq=settings.temporal.task_queue,
        mc=settings.temporal.activity_concurrency,
        lq=settings.temporal.llm_task_queue,
        lc=settings.temporal.llm_activity_concurrency,
        mgq=settings.temporal.merge_task_queue,
        mgc=settings.temporal.merge_activity_concurrency,
    )

    main_worker = Worker(
        client,
        task_queue=settings.temporal.task_queue,
        workflows=[DocumentIngestWorkflow],
        activities=MAIN_ACTIVITIES,
        max_concurrent_activities=settings.temporal.activity_concurrency,
    )
    # extract_kg gets its OWN lane (kb-ingest-llm).  GraphBuildWorkflow
    # and its merge activities moved OFF this queue (see merge_worker)
    # so a burst of extracts no longer parks behind the queue ahead of a
    # document's merge.  Concurrency-1 still serialises extract on the GPU.
    llm_worker = Worker(
        client,
        task_queue=settings.temporal.llm_task_queue,
        activities=EXTRACT_ACTIVITIES,
        max_concurrent_activities=settings.temporal.llm_activity_concurrency,
    )
    # Merge lane (kb-ingest-merge): hosts GraphBuildWorkflow + its
    # merge_and_resolve / build_property_graph activities on a SEPARATE
    # queue + concurrency cap so merge interleaves with extract instead
    # of queueing behind a flood of extract_kg (head-of-line blocking).
    # With both lanes at concurrency 1 that's up to ~2 LLM tasks in
    # flight — the GPU/proxy is sized for that.
    merge_worker = Worker(
        client,
        task_queue=settings.temporal.merge_task_queue,
        workflows=[GraphBuildWorkflow],
        activities=MERGE_ACTIVITIES,
        max_concurrent_activities=settings.temporal.merge_activity_concurrency,
    )
    # Search workflows live on their own queue so concurrent search
    # sessions don't fight ingest for GPU budget.  Cap independently
    # via TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY (default 4 — assumes
    # LLM proxy can handle a small handful of parallel sessions).
    # R7b cutover: the legacy ReAct SearchWorkflow was removed — the
    # plan-execute SearchOrchestratorWorkflow (+ SubQueryRetrievalWorkflow
    # child) is now the sole local path.  The orchestrator reuses
    # synthesize_answer (in SEARCH_ACTIVITIES) and adds plan_subquestions +
    # retrieve_subquestion (SEARCH_V2_ACTIVITIES).
    # R7a adds the GraphRAG GlobalSearchWorkflow (orchestration on this
    # small queue; its MAP partials are small-tier and its REDUCE pins
    # synthesize_answer to the large queue, same as the local orchestrator)
    # plus route_query + map_communities/map_community_partial activities.
    search_worker = Worker(
        client,
        task_queue=settings.temporal.search_task_queue,
        workflows=[
            SearchOrchestratorWorkflow,
            SubQueryRetrievalWorkflow,
            GlobalSearchWorkflow,
            DriftSearchWorkflow,
            AutoSearchWorkflow,
        ],
        activities=SEARCH_ACTIVITIES + SEARCH_V2_ACTIVITIES,
        max_concurrent_activities=settings.temporal.search_activity_concurrency,
    )
    logger.info(
        "temporal worker  search_queue={sq}  search_concurrency={sc}",
        sq=settings.temporal.search_task_queue,
        sc=settings.temporal.search_activity_concurrency,
    )
    # Large-tier final synthesis (Search R5) lives on its own queue with
    # a LOW concurrency cap so the heavyweight synthesis model never
    # serves many parallel sessions.  Same process, separate Worker pool:
    # the orchestrator pins ``synthesize_answer`` here via
    # ``execute_activity(task_queue=large_task_queue)``.  Only the
    # synthesize activity registers here — plan/retrieve/rerank stay on
    # the small queue.  No workflows host on this queue (it runs activities
    # only); the orchestrator itself still lives on the small queue.
    large_worker = Worker(
        client,
        task_queue=settings.temporal.large_task_queue,
        activities=[synthesize_answer],
        max_concurrent_activities=settings.temporal.large_activity_concurrency,
    )
    logger.info(
        "temporal worker  large_queue={lgq}  large_concurrency={lgc}",
        lgq=settings.temporal.large_task_queue,
        lgc=settings.temporal.large_activity_concurrency,
    )
    # Offline graph-community build (Search R6) lives on its OWN dedicated
    # queue so the heavy GDS Leiden projection + per-community batch
    # summaries are fully DECOUPLED from the query hot path.  Hosts the
    # CommunityBuildWorkflow + its detect/summarize activities; concurrency
    # is kept low (TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY) so a rebuild
    # doesn't flood the small-tier LLM proxy.  Triggered by the admin
    # endpoint (and an optional Temporal Schedule) — never by a search.
    graph_build_worker = Worker(
        client,
        task_queue=settings.temporal.graph_build_task_queue,
        workflows=[CommunityBuildWorkflow],
        activities=GRAPH_BUILD_ACTIVITIES,
        max_concurrent_activities=settings.temporal.graph_build_activity_concurrency,
    )
    logger.info(
        "temporal worker  graph_build_queue={gbq}  graph_build_concurrency={gbc}",
        gbq=settings.temporal.graph_build_task_queue,
        gbc=settings.temporal.graph_build_activity_concurrency,
    )

    await asyncio.gather(
        main_worker.run(), llm_worker.run(), merge_worker.run(),
        search_worker.run(), large_worker.run(), graph_build_worker.run(),
    )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

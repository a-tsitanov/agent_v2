"""`DocumentIngestWorkflow` — composes 8 activities + best-effort
graph half and on-failure mark_failed.

Outer `try/except ActivityError` covers the vector half: any non-graph
activity that exhausts retries triggers `mark_failed` then re-raises
so Temporal records the workflow as failed.  The inner `try/except`
makes the four graph activities best-effort.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        Ctx,
        FinalizeIn,
        GraphBuilt,
        Indexed,
        IngestParams,
        IngestResult,
        Injected,
        KGExtracted,
        MarkFailedIn,
        Merged,
        Parsed,
    )


_DEFAULT_RETRY = RetryPolicy(maximum_attempts=3)
_GRAPH_HEAVY_RETRY = RetryPolicy(
    maximum_attempts=2,
    initial_interval=timedelta(minutes=2),
)
_FAST_RETRY = RetryPolicy(maximum_attempts=5)


@workflow.defn
class DocumentIngestWorkflow:
    @workflow.run
    async def run(self, params: IngestParams) -> IngestResult:
        ctx: Ctx | None = None
        try:
            ctx = await workflow.execute_activity(
                "fetch_source", params,
                result_type=Ctx,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_DEFAULT_RETRY,
            )
            parsed = await workflow.execute_activity(
                "parse_and_chunk", ctx,
                result_type=Parsed,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_DEFAULT_RETRY,
            )
            indexed = await workflow.execute_activity(
                "index_vector", parsed,
                result_type=Indexed,
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=_DEFAULT_RETRY,
            )

            graph_status: str = "completed"
            try:
                injected = await workflow.execute_activity(
                    "inject_canonical", parsed,
                    result_type=Injected,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=_FAST_RETRY,
                )
                workflow.logger.info(
                    "inject_canonical done count=%d", injected.count,
                )
                kg = await workflow.execute_activity(
                    "extract_kg", parsed,
                    result_type=KGExtracted,
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=_GRAPH_HEAVY_RETRY,
                )
                merged = await workflow.execute_activity(
                    "merge_and_resolve", kg,
                    result_type=Merged,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                built = await workflow.execute_activity(
                    "build_property_graph", merged,
                    result_type=GraphBuilt,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                workflow.logger.info(
                    "graph built entities=%d relations=%d",
                    built.entities, built.relations,
                )
            except ActivityError as exc:
                workflow.logger.warning(
                    "graph stage failed, continuing: %s", exc,
                )
                graph_status = "vector_only"

            return await workflow.execute_activity(
                "finalize",
                FinalizeIn(ctx=ctx, indexed=indexed, graph_status=graph_status),
                result_type=IngestResult,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
        except ActivityError as exc:
            # Vector-half failure or other terminal failure outside the
            # graph try.  Run mark_failed compensation then re-raise.
            await workflow.execute_activity(
                "mark_failed",
                MarkFailedIn(ctx=ctx, params=params, error=str(exc)),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
            raise

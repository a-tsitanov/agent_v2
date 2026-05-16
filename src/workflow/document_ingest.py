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
        log = workflow.logger
        # Attach human-readable context to the workflow header in the UI.
        # `memo` is editable; updates are visible without drilling into events.
        workflow.upsert_memo({
            "doc_id": params.doc_id,
            "source_path": params.path,
            "stage": "starting",
        })
        log.info(
            "workflow start  doc_id=%s  path=%s", params.doc_id, params.path,
        )
        ctx: Ctx | None = None
        try:
            workflow.upsert_memo({"stage": "fetch_source"})
            log.info("→ fetch_source")
            ctx = await workflow.execute_activity(
                "fetch_source", params,
                result_type=Ctx,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_DEFAULT_RETRY,
            )
            log.info(
                "← fetch_source  local=%s  cleanup_dir=%s",
                ctx.local_path, ctx.cleanup_dir,
            )

            workflow.upsert_memo({"stage": "parse_and_chunk"})
            log.info("→ parse_and_chunk")
            parsed = await workflow.execute_activity(
                "parse_and_chunk", ctx,
                result_type=Parsed,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_DEFAULT_RETRY,
            )
            log.info(
                "← parse_and_chunk  chunks=%d  nodes_uri=%s",
                parsed.chunk_count, parsed.nodes_uri,
            )

            workflow.upsert_memo({"stage": "index_vector", "chunks": parsed.chunk_count})
            log.info("→ index_vector")
            indexed = await workflow.execute_activity(
                "index_vector", parsed,
                result_type=Indexed,
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=_DEFAULT_RETRY,
            )
            log.info("← index_vector  inserted=%d", indexed.count)

            graph_status: str = "completed"
            try:
                workflow.upsert_memo({"stage": "inject_canonical"})
                log.info("→ inject_canonical")
                injected = await workflow.execute_activity(
                    "inject_canonical", parsed,
                    result_type=Injected,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=_FAST_RETRY,
                )
                log.info("← inject_canonical  count=%d", injected.count)

                workflow.upsert_memo({"stage": "extract_kg"})
                log.info("→ extract_kg (LLM heavy)")
                kg = await workflow.execute_activity(
                    "extract_kg", parsed,
                    result_type=KGExtracted,
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=_GRAPH_HEAVY_RETRY,
                )
                log.info(
                    "← extract_kg  nodes_with_kg_uri=%s",
                    kg.nodes_with_kg_uri,
                )

                workflow.upsert_memo({"stage": "merge_and_resolve"})
                log.info("→ merge_and_resolve")
                merged = await workflow.execute_activity(
                    "merge_and_resolve", kg,
                    result_type=Merged,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                log.info(
                    "← merge_and_resolve  merged_uri=%s",
                    merged.merged_entities_uri,
                )

                workflow.upsert_memo({"stage": "build_property_graph"})
                log.info("→ build_property_graph")
                built = await workflow.execute_activity(
                    "build_property_graph", merged,
                    result_type=GraphBuilt,
                    start_to_close_timeout=timedelta(minutes=30),
                    retry_policy=_DEFAULT_RETRY,
                )
                log.info(
                    "← build_property_graph  entities=%d  relations=%d",
                    built.entities, built.relations,
                )
            except ActivityError as exc:
                log.warning("graph stage failed, downgrading to vector_only: %s", exc)
                graph_status = "vector_only"

            workflow.upsert_memo({"stage": "finalize", "graph_status": graph_status})
            log.info("→ finalize  graph_status=%s", graph_status)
            result = await workflow.execute_activity(
                "finalize",
                FinalizeIn(ctx=ctx, indexed=indexed, graph_status=graph_status),
                result_type=IngestResult,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
            log.info(
                "workflow done  doc_id=%s  chunks=%d  status=%s",
                result.doc_id, result.chunk_count, result.graph_status,
            )
            return result
        except ActivityError as exc:
            # Vector-half failure or other terminal failure outside the
            # graph try.  Run mark_failed compensation then re-raise.
            workflow.upsert_memo({"stage": "failing", "error": str(exc)[:200]})
            log.error(
                "workflow failing  doc_id=%s  error=%s", params.doc_id, exc,
            )
            await workflow.execute_activity(
                "mark_failed",
                MarkFailedIn(ctx=ctx, params=params, error=str(exc)),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_FAST_RETRY,
            )
            raise

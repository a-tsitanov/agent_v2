"""`GraphBuildWorkflow` — runs the heavy LLM merge + Neo4j graph
build as a Temporal **child** of `DocumentIngestWorkflow`.

Splitting these two activities out of the parent has three wins:

* Independent retry policy / ``schedule_to_close`` ceiling — a stuck
  merge can be cancelled without restarting the whole document ingest.
* Independent visibility in Temporal Web UI: parent shows
  ``ingest-{doc_id}`` finishing in seconds for the vector half,
  child shows ``graph-{doc_id}`` doing the slow LLM work.
* Per-activity ``ingest_metrics`` rows for ``merge_and_resolve`` and
  ``build_property_graph`` now live in the child's own history —
  ``finalize`` pulls both parent and child histories so the analytics
  pipeline records all stages in one batch (see
  ``src/workflow/activities/finalize.py:_persist_ingest_metrics``).

The child is **awaited** (``execute_child_workflow``), not
fire-and-forget — keeps "ingest complete" semantics simple. Failure
inside the child surfaces as ``ChildWorkflowError`` in the parent,
which the parent maps to ``graph_status="vector_only"`` (mirrors
the activity-error path).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        GraphBuildResult, GraphBuilt, KGExtracted, Merged,
    )


# Bounded per-activity retries (mirror of document_ingest._MAX_INGEST_ATTEMPTS):
# a permanently-failing doc gives up and frees its admission slot instead of
# looping forever. Attempt cap, NOT wall-clock — the LLM stages keep no
# schedule_to_close (see tests/test_workflow/test_completion_no_walltime_cap.py).
_MAX_INGEST_ATTEMPTS = 50
_HEAVY_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=30),
    maximum_attempts=_MAX_INGEST_ATTEMPTS,
)
_FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=_MAX_INGEST_ATTEMPTS,
)


@workflow.defn
class GraphBuildWorkflow:
    @workflow.run
    async def run(self, kg: KGExtracted) -> GraphBuildResult:
        log = workflow.logger
        workflow.upsert_memo({
            "doc_id": kg.parsed.ctx.doc_id,
            "stage": "merge_and_resolve",
        })
        log.info(
            "graph_build start  doc_id=%s  chunks=%d",
            kg.parsed.ctx.doc_id, kg.parsed.chunk_count,
        )

        merged: Merged = await workflow.execute_activity(
            "merge_and_resolve", kg,
            result_type=Merged,
            start_to_close_timeout=timedelta(hours=1),
            # Headroom for pool-wait before the first heartbeat
            # (LLMPool may block on the lane under saturation).
            heartbeat_timeout=timedelta(minutes=15),
            # No schedule_to_close wall: merge (LLM judge) must retry
            # until success rather than permanently fail under a transient
            # proxy saturation.  merge_and_resolve heartbeats throughout
            # merge_kg_extraction, so an attempt can't silently die mid-work.
            retry_policy=_HEAVY_RETRY,
        )
        log.info("← merge_and_resolve  uri=%s", merged.merged_entities_uri)

        workflow.upsert_memo({"stage": "build_property_graph"})
        # No task_queue override: this activity (and merge_and_resolve
        # above) inherits the child workflow's queue, which the parent
        # starts on ``merge_task_queue`` (kb-ingest-merge).  So the whole
        # merge stage rides the merge lane, separate from extract_kg on
        # kb-ingest-llm.  ``build_property_graph`` is also in
        # MAIN_ACTIVITIES (Neo4j-write, not GPU-bound) for single-pool
        # deployments.
        built: GraphBuilt = await workflow.execute_activity(
            "build_property_graph", merged,
            result_type=GraphBuilt,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(hours=24),
            retry_policy=_FAST_RETRY,
        )
        log.info(
            "graph_build done  entities=%d  relations=%d",
            built.entities, built.relations,
        )
        return GraphBuildResult(merged=merged, built=built)

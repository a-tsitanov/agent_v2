"""`DocumentIngestWorkflow` — composes 8 activities + best-effort
graph half and on-failure mark_failed.

Outer `try/except ActivityError` covers the vector half: any non-graph
activity that exhausts retries triggers `mark_failed` then re-raises
so Temporal records the workflow as failed.  The inner `try/except`
makes the four graph activities best-effort.

## Retry policy

Every activity is configured to **retry indefinitely** on transient
failure (``maximum_attempts=0``).  Backoff is exponential, capped at a
per-profile maximum interval so retries don't stretch out to hours.
The hard stop is ``schedule_to_close_timeout`` — the overall wall-clock
budget for an activity (sum of all attempts + waits).  Once that
ceiling is reached Temporal fails the activity with a timeout error,
which our inner / outer ``try/except ActivityError`` handles:

* graph half exhausting its budget → ``graph_status='vector_only'``
* vector half exhausting its budget → ``mark_failed`` + workflow fails

Permanent input problems (corrupt file, schema violation) should be
raised from inside activities as
``ApplicationError(non_retryable=True)`` to bypass the retry loop —
otherwise we'd loop for the full budget on a known-dead document.

Two retry profiles:

* ``_FAST_FOREVER`` — IO / embedding / Neo4j MERGE / PG UPDATE.
  1s → 2s → 4s → … capped at 60s, forever.
* ``_HEAVY_FOREVER`` — LLM-bound (``extract_kg``, ``merge_and_resolve``).
  2min → 4min → … capped at 30min, forever.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ActivityError, ChildWorkflowError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from src.config import settings
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
        WikibasePushed,
    )
    from src.workflow.graph_build import GraphBuildWorkflow


# Forever-retry profiles.  ``maximum_attempts=0`` means no cap on
# attempts; the activity stops only when ``schedule_to_close_timeout``
# fires or a non-retryable ``ApplicationError`` is raised inside.
_FAST_FOREVER = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=0,
)
_HEAVY_FOREVER = RetryPolicy(
    initial_interval=timedelta(minutes=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=30),
    maximum_attempts=0,
)


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
                start_to_close_timeout=timedelta(minutes=5),
                schedule_to_close_timeout=timedelta(hours=1),
                retry_policy=_FAST_FOREVER,
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
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=2),
                schedule_to_close_timeout=timedelta(hours=6),
                retry_policy=_FAST_FOREVER,
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
                start_to_close_timeout=timedelta(hours=1),
                heartbeat_timeout=timedelta(minutes=2),
                schedule_to_close_timeout=timedelta(hours=24),
                retry_policy=_FAST_FOREVER,
            )
            log.info("← index_vector  inserted=%d", indexed.count)

            graph_status: str = "completed"
            built: GraphBuilt | None = None
            # Bind ``merged`` outside the inner try so the post-graph
            # ``push_wikibase`` block can safely reference it even when
            # ``merge_and_resolve`` raised and the inner except path ran.
            merged: Merged | None = None
            try:
                workflow.upsert_memo({"stage": "inject_canonical"})
                log.info("→ inject_canonical")
                injected = await workflow.execute_activity(
                    "inject_canonical", parsed,
                    result_type=Injected,
                    # embedding kNN + optional LLM verify — give the LLM
                    # path a 1h single-attempt ceiling (matches the other
                    # LLM activities) so a slow proxy isn't killed early.
                    start_to_close_timeout=timedelta(hours=1),
                    schedule_to_close_timeout=timedelta(hours=12),
                    retry_policy=_FAST_FOREVER,
                )
                log.info("← inject_canonical  count=%d", injected.count)

                workflow.upsert_memo({"stage": "extract_kg"})
                log.info("→ extract_kg (LLM heavy)")
                # LLM-bound: routed to the GPU-serialised task queue
                # so simultaneous workflows don't dogpile the local
                # model.
                kg = await workflow.execute_activity(
                    "extract_kg", parsed,
                    result_type=KGExtracted,
                    task_queue=settings.temporal.llm_task_queue,
                    start_to_close_timeout=timedelta(hours=2),
                    # Headroom for pool-wait before the first heartbeat
                    # (LLMPool may block on the lane under saturation).
                    heartbeat_timeout=timedelta(minutes=15),
                    schedule_to_close_timeout=timedelta(hours=48),
                    retry_policy=_HEAVY_FOREVER,
                )
                log.info(
                    "← extract_kg  nodes_with_kg_uri=%s",
                    kg.nodes_with_kg_uri,
                )

                workflow.upsert_memo({"stage": "graph_build_child"})
                log.info("→ GraphBuildWorkflow (child, queue=%s)",
                         settings.temporal.merge_task_queue)
                # merge_and_resolve + build_property_graph now run as
                # a Temporal child workflow so they get independent
                # retry / visibility / scheduling.  Parent awaits — keeps
                # "ingest complete" semantics simple.  ChildWorkflowError
                # is caught below alongside ActivityError so a stuck
                # child still downgrades to vector_only without failing
                # the whole document.
                gb_result = await workflow.execute_child_workflow(
                    GraphBuildWorkflow.run, kg,
                    id=f"graph-{params.doc_id}",
                    task_queue=settings.temporal.merge_task_queue,
                    parent_close_policy=ParentClosePolicy.REQUEST_CANCEL,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                )
                merged = gb_result.merged
                built = gb_result.built
                log.info(
                    "← GraphBuildWorkflow  merged_uri=%s  entities=%d  relations=%d",
                    merged.merged_entities_uri,
                    built.entities, built.relations,
                )
            except (ActivityError, ChildWorkflowError) as exc:
                log.warning("graph stage failed, downgrading to vector_only: %s", exc)
                graph_status = "vector_only"

            # Wikibase push lives OUTSIDE the inner try/except: the graph
            # is already built and committed; Wikibase success/failure is
            # an independent status and must not lie about graph_status.
            # The activity itself is best-effort -- it never raises, it
            # reports its outcome via ``WikibasePushed.status``.
            wb: WikibasePushed = WikibasePushed(status="skipped")
            if graph_status == "completed" and merged is not None:
                workflow.upsert_memo({"stage": "push_wikibase"})
                log.info("→ push_wikibase")
                wb = await workflow.execute_activity(
                    "push_wikibase", merged,
                    result_type=WikibasePushed,
                    start_to_close_timeout=timedelta(minutes=15),
                    heartbeat_timeout=timedelta(minutes=2),
                    schedule_to_close_timeout=timedelta(hours=6),
                    retry_policy=_FAST_FOREVER,
                )
                log.info(
                    "← push_wikibase  status=%s  created=%d  updated=%d  "
                    "ext_ids=%d  rels=%d  new_props=%d",
                    wb.status, wb.created_items, wb.updated_items,
                    wb.external_id_statements, wb.relation_statements,
                    wb.new_properties_created,
                )
            else:
                log.info(
                    "skipping push_wikibase  graph_status=%s  merged=%s",
                    graph_status, "yes" if merged is not None else "no",
                )

            entities = built.entities if built is not None else 0
            relations = built.relations if built is not None else 0
            workflow.upsert_memo({
                "stage": "finalize",
                "graph_status": graph_status,
                "wikibase_status": wb.status,
                "entities": entities,
                "relations": relations,
            })
            log.info(
                "→ finalize  graph_status=%s  wikibase_status=%s  "
                "entities=%d  relations=%d",
                graph_status, wb.status, entities, relations,
            )
            result = await workflow.execute_activity(
                "finalize",
                FinalizeIn(
                    ctx=ctx,
                    indexed=indexed,
                    graph_status=graph_status,
                    entities=entities,
                    relations=relations,
                    wikibase=wb,
                    version_tag=params.version_tag,
                    model=params.model,
                    extraction_model=params.extraction_model,
                    judge_model=params.judge_model,
                    search_model=params.search_model,
                    env=params.env,
                ),
                result_type=IngestResult,
                start_to_close_timeout=timedelta(minutes=10),
                schedule_to_close_timeout=timedelta(hours=12),
                retry_policy=_FAST_FOREVER,
            )
            log.info(
                "workflow done  doc_id=%s  chunks=%d  status=%s  "
                "entities=%d  relations=%d  wikibase=%s",
                result.doc_id, result.chunk_count, result.graph_status,
                result.entities, result.relations, result.wikibase_status,
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
                start_to_close_timeout=timedelta(minutes=10),
                schedule_to_close_timeout=timedelta(hours=12),
                retry_policy=_FAST_FOREVER,
            )
            raise

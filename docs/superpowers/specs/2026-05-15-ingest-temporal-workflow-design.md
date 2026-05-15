# Ingest Pipeline as Temporal Workflow — Design

**Date:** 2026-05-15
**Status:** Approved (brainstorming) — pending implementation plan
**Owner:** kb-llamaindex

## 1. Motivation

The current ingestion path is one taskiq task, `src.ingestion.tasks.process_document`, which runs ~17 sequential steps end-to-end: parse → chunk → identifier-canonicalisation → optional translation → vector indexing → canonical entity inject → LLM KG extraction → cross-chunk merge → phone consolidation → entity resolution → property graph build → finalize. The block is wrapped in two coarse `try/except` layers; a failure inside any of the inner steps either silently downgrades the graph half or fails the whole document.

This shape causes four problems we want to fix:

1. **Crash mid-pipeline loses progress.** A worker crash on step 14 reruns step 1 from scratch, repaying the embeddings and LLM bill.
2. **Coarse retry granularity.** A flaky Neo4j connection or a transient LLM 503 forces a full re-ingest instead of retrying just the affected stage.
3. **No per-stage observability.** Postgres tracks only `pending / processing / completed / failed`. There is no way to see "doc X stuck on KG extraction for 40 minutes."
4. **Long async stages block the worker.** A multi-minute `extract_kg` call holds the taskiq worker; the same worker is the one acking RabbitMQ, so flow control is awkward.

The fix is to model ingestion as a Temporal workflow where each natural stage is an activity with its own retry policy, timeout, and heartbeat.

## 2. Scope

**In scope**
- Replace taskiq + RabbitMQ as the ingestion queue stack. The only existing taskiq task is `process_document`.
- Add Temporal (self-hosted) to `docker-compose.yml`, including Temporal Web UI.
- New `src/workflow/` package: workflow definition, activity functions, contracts, worker entry point.
- API change: `POST /api/v1/ingest` calls `temporal_client.start_workflow` directly instead of `process_document.kiq()`.
- Tests at three levels: unit (activity-level), workflow (mocked activities via `WorkflowEnvironment.start_time_skipping()`), integration (`start_local()` with live Milvus/Neo4j/MinIO).
- Migration plan that keeps the system serviceable during the swap.

**Out of scope**
- A separate `RebuildGraphWorkflow(doc_id)` for re-running just the graph half of an existing doc — useful follow-up but not required for this change.
- Per-chunk activity fan-out for `extract_kg`. Single activity with internal LlamaIndex concurrency for now; revisit if a single doc exceeds the 1-hour timeout.
- Migrating other background work to Temporal. There is no other background work today.

## 3. Architecture

```
POST /api/v1/ingest
  │
  ├─ MinIO  put_object → s3://kb-uploads/...
  ├─ Postgres INSERT documents (status="pending")
  └─ temporal_client.start_workflow(
         DocumentIngestWorkflow.run,
         IngestParams(doc_id, s3_uri),
         id=f"ingest-{doc_id}",
         task_queue="kb-ingest",
         id_reuse_policy=ALLOW_DUPLICATE_FAILED_ONLY,
     )
  → 202 Accepted { doc_id, workflow_id }

Worker process: `uv run python -m src.workflow.worker`
  ├─ WorkflowRunner: DocumentIngestWorkflow
  └─ ActivityWorker: 8 activities, concurrency = WORKFLOW_ACTIVITY_CONCURRENCY (default 4)

Temporal stack (docker-compose):
  - temporal       (temporalio/auto-setup:1.25.x, Postgres backend, DB "temporal")
  - temporal-ui    (temporalio/ui:2.x → http://localhost:8080)
  Reuses the existing Postgres instance.

Removed from docker-compose: rabbitmq.
Removed from pyproject: taskiq, taskiq-aio-pika.
```

State between activities is passed via **claim check in MinIO** (bucket `kb-staging`, key `{workflow_run_id}/{stage}.pkl`). Activity payloads carry only URIs, IDs, and small counters; no `BaseNode` / `EntityNode` ever crosses the workflow boundary. The `finalize` activity (and the failure path) deletes the whole `{workflow_run_id}/` prefix.

## 4. Workflow Definition

`src/workflow/document_ingest.py`:

```python
@workflow.defn
class DocumentIngestWorkflow:
    @workflow.run
    async def run(self, params: IngestParams) -> IngestResult:
        ctx: Ctx | None = None
        try:
            ctx = await wf.execute_activity(
                fetch_source, params,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            parsed = await wf.execute_activity(
                parse_and_chunk, ctx,
                start_to_close_timeout=timedelta(minutes=15),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            indexed = await wf.execute_activity(
                index_vector, parsed,
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

            graph_status = "completed"
            try:
                await wf.execute_activity(inject_canonical, parsed, ...)
                kg = await wf.execute_activity(
                    extract_kg, parsed,
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=RetryPolicy(
                        maximum_attempts=2,
                        initial_interval=timedelta(minutes=2),
                    ),
                )
                merged = await wf.execute_activity(merge_and_resolve, kg, ...)
                await wf.execute_activity(build_property_graph, merged, ...)
            except ActivityError as exc:
                workflow.logger.warning(
                    "graph stage failed, continuing", exc_info=exc,
                )
                graph_status = "vector_only"

            return await wf.execute_activity(
                finalize,
                FinalizeIn(ctx=ctx, indexed=indexed, graph_status=graph_status),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
        except ActivityError as exc:
            # Vector-half failure (fetch / parse / index_vector exhausted retries)
            # or any other terminal failure outside the graph try/except.
            await wf.execute_activity(
                mark_failed,
                MarkFailedIn(ctx=ctx, params=params, error=str(exc)),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
            raise
```

Outer `try / except ActivityError` covers the vector half: any activity that exhausts its retry policy outside the inner graph block triggers `mark_failed` (writes Postgres `status="failed"`, removes the staging prefix) and then re-raises so Temporal records the workflow as failed. The inner `try / except` is independent — only the graph activities are best-effort.

Cancellation (operator-initiated) propagates as `CancelledError` rather than `ActivityError` and is intentionally not caught here; the staging-blob cleanup cron picks up the orphaned prefix.

## 5. Activities

All activities live under `src/workflow/activities/`. Each is a thin wrapper around already-tested code from `src/ingestion/tasks.py` and `src/graph/`.

| # | Activity | Input | Output | Timeout | Retry | Notes |
|---|----------|-------|--------|---------|-------|-------|
| 1 | `fetch_source` | `IngestParams(doc_id, path)` | `Ctx(doc_id, local_path, cleanup_dir, workflow_run_id)` | 2 min | 3× exp | Downloads from MinIO if `s3://`, otherwise passes through. Sets PG `status="processing"`. Idempotent: skip download if local file present. |
| 2 | `parse_and_chunk` | `Ctx` | `Parsed(nodes_uri)` | 15 min | 3× exp | `read_documents` + `build_ingestion_pipeline().arun()`. Writes `nodes` to `kb-staging/{run_id}/parsed.pkl`. Heartbeat per chunk. |
| 3 | `index_vector` | `Parsed` | `Indexed(node_ids, count)` | 30 min | 3× exp | Loads `parsed.pkl`, scrubs `FULL_TRANSLATED_TEXT_KEY` / `ORIGINAL_DOC_LENGTH_KEY`, snapshot-strip-insert-restore around `index_nodes`. Idempotent via Milvus PK = `node_id`. Heartbeat per batch. |
| 4 | `inject_canonical` | `Parsed` | `Injected(count)` | 5 min | 5× exp | `inject_canonical_entities(graph_store, nodes)`. Idempotent via Neo4j `MERGE`. |
| 5 | `extract_kg` | `Parsed` | `KGExtracted(nodes_with_kg_uri)` | 1 hour | 2× (2 min initial) | `build_kg_extractor(llm, mode="lightrag").acall(nodes)`. Heaviest activity. Heartbeat every N chunks. Writes augmented nodes to `kb-staging/{run_id}/kg.pkl`. |
| 6 | `merge_and_resolve` | `KGExtracted` | `Merged(merged_entities_uri, nodes_uri)` | 30 min | 3× exp | `merge_kg_extraction` → `_consolidate_phone_entities` → `resolve_entities` (if `agent.er_enabled`). Writes merged tuple to `kb-staging/{run_id}/merged.pkl`. |
| 7 | `build_property_graph` | `Merged` | `GraphBuilt(entities, relations)` | 30 min | 3× exp | `_strip_neo4j_unsafe_metadata` → `build_property_graph_index(..., extractor=NoOpKGExtractor())` → `upsert_nodes` + `upsert_relations`. Idempotent via Neo4j `MERGE`. |
| 8 | `finalize` | `FinalizeIn(ctx, indexed, graph_status)` | `IngestResult` | 2 min | 5× exp | PG `status=graph_status` (`completed` or `vector_only`). Deletes `kb-staging/{run_id}/` prefix in MinIO. `rmtree(cleanup_dir)` if set. |

`mark_failed(ctx, error_message)` is a 9th, on-failure-only activity with the same shape as `finalize` but writes PG `status="failed"`.

## 6. Contracts

`src/workflow/contracts.py`. Pydantic v2 models, JSON-serializable through Temporal's default `DataConverter` (no `pickle` over the wire — pickle is used only for the MinIO blobs).

```python
class IngestParams(BaseModel):
    doc_id: str
    path: str            # s3://... or filesystem path

class Ctx(BaseModel):
    doc_id: str
    local_path: str
    cleanup_dir: str | None
    workflow_run_id: str

class Parsed(BaseModel):
    ctx: Ctx
    nodes_uri: str       # s3://kb-staging/{run_id}/parsed.pkl
    chunk_count: int

# ... Indexed, Injected, KGExtracted, Merged, GraphBuilt, FinalizeIn, IngestResult
```

Re-running a workflow: each run gets a fresh `workflow_run_id`, so its staging prefix is isolated. Old blobs from a prior failed run are cleaned by the failure path; if that path itself crashes, a separate cron sweeps `kb-staging/` prefixes older than 24 h.

## 7. Failure Semantics

- **`ApplicationError(non_retryable=True)`** — bad file, unparseable PDF, schema violation. Fails the activity once, no retry. Workflow either fails (if upstream) or skips graph (if inside the `try`).
- **Transient (default)** — exponential backoff per the per-activity retry policy. Heartbeat timeouts count as transient.
- **Graph block** — exhausted retries logged; workflow continues to `finalize` with `graph_status="vector_only"`.
- **Vector block** (`fetch / parse / index_vector`) — exhausted retries fail the workflow; `mark_failed` runs from the WF-level `try / finally`.
- **Workflow worker crash** — Temporal preserves state; on replay the workflow resumes from the next activity. Activities are idempotent.

Postgres `documents.status` enum extended:
- `pending` → `processing` → `completed` (vector + graph both OK)
- `pending` → `processing` → `vector_only` (graph stage gave up)
- `pending` → `processing` → `failed` (vector stage failed terminally)

## 8. Idempotency and Re-ingest

- **Re-running the same `doc_id`** — `WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY` lets a failed workflow be re-started under the same `ingest-{doc_id}` ID. Successful runs are protected against accidental re-ingest at the Temporal layer.
- **Idempotency per activity**:
  - `fetch_source` — skip download if file already on disk.
  - `index_vector` — Milvus uses `node_id` as PK; re-inserts are upserts.
  - `inject_canonical`, `build_property_graph` — Neo4j `MERGE` semantics.
  - `extract_kg`, `merge_and_resolve` — not idempotent on the LLM side (token cost paid again on retry), but writes are overwrites of `kb-staging/{run_id}/...` so no state pollution.
- **Forced re-ingest** (operator) — API surface adds `force=True` parameter that uses a fresh `workflow_id` suffix (`ingest-{doc_id}-{ts}`) bypassing the reuse policy.

## 9. Observability

- **Temporal Web UI** (`http://localhost:8080`) is the primary dashboard. Shows per-workflow timeline, per-activity duration, retry history, payload (URIs, counters — small).
- **LangFuse** still captures LLM spans inside activities (`extract_kg`, `merge_and_resolve`, optional translator). Activities pass `workflow.info().run_id` into the LangFuse trace metadata so traces are correlatable with Temporal runs.
- **Structlog** continues to emit JSON; we add a default field `workflow_run_id` to every log line inside an activity.
- **Postgres `documents`** remains the API-facing status source. Temporal is operational; PG is canonical for the application.

## 10. Testing

- **Unit (`tests/unit/workflow/`)**: each activity is a plain async function. Call it directly with monkeypatched dependencies (`build_vector_index`, `build_neo4j_graph_store`, `build_llm`). Assert side effects and return shape. No Temporal involved.
- **Workflow (`tests/workflow/`)**: `temporalio.testing.WorkflowEnvironment.start_time_skipping()`. Register the workflow with mocked activities that return canned payloads. Assert:
  - Happy path: all 8 activities called in order, final status `completed`.
  - Graph failure: `extract_kg` raises after retries → status `vector_only`, `finalize` still runs.
  - Vector failure: `index_vector` raises after retries → workflow fails, `mark_failed` runs.
  - Time-skipping verifies retry backoffs without waiting.
- **Integration (`tests/integration/workflow/`)**: `WorkflowEnvironment.start_local()` + live Milvus / Neo4j / MinIO / Postgres from `docker-compose`. Two scenarios: live happy path on a small test PDF, and graph-failure simulation by pointing `OLLAMA_HOST` at a black-holed port.

Existing `tests/integration/test_ingest_pipeline.py` (if present) is converted to drive the new workflow; the old taskiq-based test goes away with taskiq.

## 11. Migration

1. **Step 1 — additive.** Add `src/workflow/` package, Temporal compose services, worker entrypoint. Tests pass. No call sites switched.
2. **Step 2 — shim.** Refactor `src/ingestion/tasks.py:process_document` to a thin shim that does `await temporal_client.start_workflow(...).result()`. Existing taskiq-based call sites keep working; the body moves to the activities.
3. **Step 3 — API switch.** `POST /api/v1/ingest` calls `temporal_client.start_workflow` directly. Returns `workflow_id` alongside `doc_id`. The shim still exists for any other internal caller.
4. **Step 4 — clean up.** Remove the taskiq shim, the `rabbitmq` service from `docker-compose.yml`, the `taskiq` / `taskiq-aio-pika` entries from `pyproject.toml`, and the `RABBITMQ_URL` env var.

Each step is independently mergeable. Step 1 lands behind feature parity with the old path; only Step 3 changes user-visible behaviour.

## 12. Open Questions / Follow-ups

- Per-chunk fan-out for `extract_kg` (parent-child workflows) — defer until a real document hits the 1-hour ceiling.
- `RebuildGraphWorkflow(doc_id)` for re-running just the graph half — useful for operators after an LLM model upgrade.
- Cross-document `extract_kg` rate limiting — Temporal task-queue concurrency caps this naturally at `WORKFLOW_ACTIVITY_CONCURRENCY`. If we need a global LLM rate limit, a `RateLimit` activity gate is the standard pattern.
- Staging-blob cleanup cron for orphaned `kb-staging/` prefixes (workflows that crashed before `finalize` or `mark_failed` ran).

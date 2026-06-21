"""Document upload + ingestion-status endpoints.

Flow: upload file to MinIO (synchronous put), insert a Postgres
``documents`` row, then hand the document to the
``IngestSchedulerWorkflow`` admission singleton (K = max in-flight
documents) which starts the actual processing workflow once a slot is
available.  Returns ``202 Accepted`` with the job id.  The Temporal
worker (``src.workflow.worker``) runs the activities asynchronously.
"""

from __future__ import annotations

import asyncio
import io
import uuid
from datetime import timedelta
from pathlib import Path

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile, status
from loguru import logger
from pydantic import BaseModel
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from urllib3.exceptions import MaxRetryError

from src.api.auth import require_api_key
from src.config import settings
from src.storage.minio import S3Error, build_minio_storage
from src.storage.postgres import AsyncPostgres
from src.workflow.client import get_temporal_client
from src.workflow.contracts import IngestParams, SchedulerParams
from src.workflow.ingest_scheduler import IngestSchedulerWorkflow

router = APIRouter(tags=["ingestion"])


class IngestEnqueuedResponse(BaseModel):
    job_id: uuid.UUID


class JobProgressResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    path: str
    department: str
    doc_type: str
    error: str
    summary: str


@router.post(
    "/ingest",
    response_model=IngestEnqueuedResponse,
    dependencies=[Depends(require_api_key)],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload a document and enqueue it for ingestion",
)
@inject
async def upload_document(
    pg: FromDishka[AsyncPostgres],
    file: UploadFile = File(...),
    department: str = Form(default=""),
    force: bool = Form(default=False),
    x_version_tag: str | None = Header(default=None, alias="X-Version-Tag"),
) -> IngestEnqueuedResponse:
    if not file.filename:
        raise HTTPException(400, "filename required")

    doc_id = uuid.uuid4()
    # Object key keeps the original filename so the worker can pick the
    # right SimpleDirectoryReader extension handler after download.
    object_key = f"{doc_id}/{file.filename}"

    # Read the full body into memory.  Large PDFs (~hundreds of MB)
    # should arrive via presigned URLs in a future iteration; the
    # synchronous path here matches the previous local-disk behaviour
    # in terms of memory profile.
    contents = await file.read()

    # Wrap BOTH the storage init (which on cold-start probes the
    # bucket via `bucket_exists`) AND the actual `put_object` call.
    # If MinIO is down at process start, the singleton constructor
    # is the first place that touches the network and raises.
    # `S3Error` covers protocol-level rejections (auth, missing bucket,
    # bad key); `MaxRetryError` / `OSError` cover transport failures
    # when MinIO is fully unreachable (container down, DNS, refused).
    # Both surface as the same user-visible state: storage unavailable.
    try:
        storage = build_minio_storage()
        s3_uri = await asyncio.to_thread(
            storage.put_object,
            object_key,
            io.BytesIO(contents),
            len(contents),
            file.content_type or "application/octet-stream",
        )
    except (S3Error, MaxRetryError, OSError) as exc:
        logger.warning(
            "minio upload failed  doc_id={d}  err={e}", d=doc_id, e=exc,
        )
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "upload storage unavailable",
        ) from exc

    doc_type = Path(file.filename).suffix.lstrip(".").lower()
    await pg.insert_pending(
        doc_id, s3_uri, department=department, doc_type=doc_type,
    )

    # Analytics labels: explicit header wins, else AnalyticsSettings default.
    # Models are auto-captured from runtime config so model swaps
    # (LITELLM_*_MODEL env changes) are reflected without operator effort.
    # All three role models are snapshotted at submit time and propagated
    # via IngestParams → FinalizeIn → ingest_metrics rows; this guarantees
    # each row's `model` column reflects the model that activity actually
    # used (per Stage 4 of the multimodel plan).
    version_tag = x_version_tag or settings.analytics.default_version_tag
    cfg = settings.litellm
    model = cfg.effective_base
    extraction_model = cfg.model_for("extraction")
    judge_model = cfg.model_for("judge")
    search_model = cfg.model_for("search")
    env_name = settings.analytics.env_name

    client = await get_temporal_client()
    params = IngestParams(
        doc_id=str(doc_id), path=s3_uri,
        version_tag=version_tag, model=model,
        extraction_model=extraction_model,
        judge_model=judge_model,
        search_model=search_model,
        env=env_name,
        # Snapshot the wiki flag here (outside the Temporal sandbox) so
        # the workflow never reads settings.wiki — constructing
        # WikiSettings touches .env, a determinism violation inside
        # @workflow.run.
        wiki_enabled=settings.wiki.enabled,
        # Same determinism reasoning: snapshot the classifier flag here,
        # ship the operator's force override.
        classifier_enabled=settings.classifier.enabled,
        force=force,
    )
    # Admission is always on: hand the document to the singleton scheduler
    # (signal-with-start; USE_EXISTING reuses the running one) so at most K
    # (max_inflight) documents run at once, each to completion, FIFO.
    try:
        await client.start_workflow(
            IngestSchedulerWorkflow.run,
            SchedulerParams(max_inflight=settings.ingest_admission.max_inflight),
            id="ingest-scheduler",
            # Dedicated queue: the scheduler runs on its own worker pool,
            # isolated from DocumentIngestWorkflow on `task_queue` (main).
            task_queue=settings.temporal.scheduler_task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            start_signal="submit",
            start_signal_args=[params],
            # Defense-in-depth for the always-on singleton: the workflow
            # bounds its own history via drain-then-continue_as_new
            # (see IngestSchedulerWorkflow), but give a cold replay extra
            # headroom over the 10s default so a one-off larger history
            # (e.g. right after a deploy) can't time out the workflow task
            # and wedge admission.  Only applies to a freshly-started
            # singleton; USE_EXISTING reuses the running one untouched.
            task_timeout=timedelta(seconds=30),
        )
    except WorkflowAlreadyStartedError as exc:
        # Reuse policy rejected the start: a workflow with this id is
        # already running or already completed successfully.  Don't
        # 500 the caller; surface 409 with the existing run details.
        logger.warning(
            "ingest duplicate  workflow_id={w}  run_id={r}",
            w=exc.workflow_id, r=exc.run_id,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "workflow already exists for this job_id",
                "workflow_id": exc.workflow_id,
                "run_id": exc.run_id,
            },
        ) from exc
    logger.info(
        "ingest enqueued  doc_id={d}  path={p}  dept={dept}  "
        "version_tag={v}  model={m}  ext={ext}  judge={j}  search={s}  env={e}",
        d=doc_id, p=s3_uri, dept=department,
        v=version_tag, m=model, ext=extraction_model,
        j=judge_model, s=search_model, e=env_name,
    )
    return IngestEnqueuedResponse(job_id=doc_id)


@router.get(
    "/ingest/{job_id}",
    response_model=JobProgressResponse,
    dependencies=[Depends(require_api_key)],
    summary="Get ingestion job status",
)
@inject
async def job_status(
    job_id: uuid.UUID,
    pg: FromDishka[AsyncPostgres],
) -> JobProgressResponse:
    row = await pg.get(job_id)
    if row is None:
        raise HTTPException(404, f"job {job_id} not found")
    return JobProgressResponse(
        job_id=row.id,
        status=row.status,
        path=row.path,
        department=row.department,
        doc_type=row.doc_type,
        error=row.error,
        summary=row.summary,
    )

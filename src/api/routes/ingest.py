"""Document upload + ingestion-status endpoints.

Flow: upload file to MinIO (synchronous put), insert a Postgres
``documents`` row, start the Temporal ``DocumentIngestWorkflow`` and
return ``202 Accepted`` with the job id.  The Temporal worker
(``src.workflow.worker``) runs the activities asynchronously.
"""

from __future__ import annotations

import asyncio
import io
import uuid
from pathlib import Path

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from loguru import logger
from pydantic import BaseModel
from temporalio.common import WorkflowIDReusePolicy

from urllib3.exceptions import MaxRetryError

from src.api.auth import require_api_key
from src.config import settings
from src.storage.minio import S3Error, build_minio_storage
from src.storage.postgres import AsyncPostgres
from src.workflow.client import get_temporal_client
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow

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

    # Kick off the Temporal workflow directly.  The Temporal worker
    # (``python -m src.workflow.worker``) polls the task queue and
    # executes ``DocumentIngestWorkflow`` end-to-end (fetch → parse →
    # vector → graph → finalize).  The workflow id is derived from
    # ``doc_id`` so we get idempotent de-dup at the Temporal level.
    #
    # ``ALLOW_DUPLICATE_FAILED_ONLY`` blocks a fresh upload of the same
    # doc_id from re-running a workflow that already succeeded (we'd
    # silently re-index, paying the LLM bill again).  Failed workflows
    # CAN be restarted under the same id — that's the explicit retry
    # path: re-upload to retry an ingest that died terminally.
    client = await get_temporal_client()
    await client.start_workflow(
        DocumentIngestWorkflow.run,
        IngestParams(doc_id=str(doc_id), path=s3_uri),
        id=f"ingest-{doc_id}",
        task_queue=settings.temporal.task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
    )
    logger.info(
        "ingest enqueued  doc_id={d}  path={p}  dept={dept}",
        d=doc_id, p=s3_uri, dept=department,
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

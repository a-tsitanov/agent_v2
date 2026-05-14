"""Document upload + ingestion-status endpoints.

Flow mirrors enterprise-kb's ``/api/v1/ingest`` — write file to a
shared volume, insert a Postgres ``documents`` row, kick a taskiq
task and return ``202 Accepted`` with the job id.  The worker
(``src/ingestion/tasks.py``) processes asynchronously.
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

from src.api.auth import require_api_key
from src.ingestion.tasks import process_document
from src.storage.minio import S3Error, build_minio_storage
from src.storage.postgres import AsyncPostgres

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

    storage = build_minio_storage()
    try:
        s3_uri = await asyncio.to_thread(
            storage.put_object,
            object_key,
            io.BytesIO(contents),
            len(contents),
            file.content_type or "application/octet-stream",
        )
    except S3Error as exc:
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

    # Push the actual processing onto RabbitMQ.  The taskiq broker is
    # started in `src/api/main.py` lifespan; the worker process
    # (``taskiq worker src.ingestion.tasks:broker``) consumes from
    # the same queue and runs ``process_document`` end-to-end.  Worker
    # detects the s3:// prefix and downloads the object before reading.
    await process_document.kiq(str(doc_id), s3_uri)
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

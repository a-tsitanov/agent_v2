"""`GET /api/v1/documents/{doc_id}` — download the original source file.

Streams the original uploaded file from MinIO (the URI is stored in
Postgres `documents.path`).  doc_id is the value search responses expose
in `sources[]` / `documents[]`.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse, StreamingResponse
from loguru import logger

from src.api.auth import require_api_key
from src.storage.minio import S3Error, build_minio_storage
from src.storage.postgres import AsyncPostgres

router = APIRouter(tags=["documents"])


@router.get(
    "/documents/{doc_id}",
    dependencies=[Depends(require_api_key)],
    summary="Download the original source file of an ingested document",
)
@inject
async def download_document(doc_id: str, pg: FromDishka[AsyncPostgres]):
    try:
        row = await pg.get(uuid.UUID(doc_id))
    except (ValueError, TypeError):
        row = None
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "document not found")

    path = row.path
    # Legacy local-path docs (pre-MinIO): stream from disk if present.
    if not path.startswith("s3://"):
        local = Path(path)
        if local.is_file():
            return FileResponse(local, filename=local.name)
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "document source not available")

    try:
        storage = build_minio_storage()
        filename, size, content_type = storage.stat_object(path)
    except S3Error as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "document source not available") from exc
    except Exception as exc:  # MinIO unreachable, transport errors, etc.
        logger.exception("document download storage error doc_id={d}", d=doc_id)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "storage unavailable") from exc

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(size),
    }
    return StreamingResponse(
        storage.stream_object(path), media_type=content_type, headers=headers)

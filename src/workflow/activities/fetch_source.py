"""`fetch_source` — resolve doc path to a local file + mark processing.

Idempotent: a second attempt after a worker crash finds the file on
disk and skips the MinIO GET.  Postgres `update_status('processing')`
is a no-op overwrite if already processing — safe to repeat.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from loguru import logger
from temporalio import activity

from src.storage.minio import build_minio_storage
from src.storage.postgres import AsyncPostgres
from src.workflow.contracts import Ctx, IngestParams


@activity.defn
async def fetch_source(params: IngestParams) -> Ctx:
    info = activity.info()
    activity.logger.info(
        "fetch_source start  doc=%s  path=%s", params.doc_id, params.path,
    )
    activity.heartbeat({"stage": "init", "path": params.path})

    pg = AsyncPostgres()
    await pg.update_status(uuid.UUID(params.doc_id), status="processing")
    activity.heartbeat({"stage": "pg_processing"})

    if not params.path.startswith("s3://"):
        activity.logger.info("fetch_source legacy local path; no download")
        return Ctx(
            doc_id=params.doc_id,
            local_path=params.path,
            cleanup_dir=None,
            workflow_run_id=info.workflow_run_id,
            doc_date_epoch=params.doc_date_epoch,
            inserted_at_epoch=params.inserted_at_epoch,
        )

    storage = build_minio_storage()
    _, key = storage.parse_s3_uri(params.path)
    filename = Path(key).name
    target = storage.download_dir / params.doc_id / filename
    if not target.exists():
        activity.heartbeat({"stage": "downloading"})
        await asyncio.to_thread(storage.get_object_to_path, params.path, target)
        activity.heartbeat({"stage": "downloaded", "local": str(target)})
        logger.info(
            "fetch_source  download  doc={d}  s3={p}  local={t}",
            d=params.doc_id, p=params.path, t=target,
        )
    else:
        activity.heartbeat({"stage": "cache_hit", "local": str(target)})
        logger.info(
            "fetch_source  cache_hit  doc={d}  local={t}",
            d=params.doc_id, t=target,
        )
    return Ctx(
        doc_id=params.doc_id,
        local_path=str(target),
        cleanup_dir=str(target.parent),
        workflow_run_id=info.workflow_run_id,
        doc_date_epoch=params.doc_date_epoch,
        inserted_at_epoch=params.inserted_at_epoch,
    )

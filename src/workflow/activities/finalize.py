"""`finalize` (success path) and `mark_failed` (workflow-level
on-failure) — write Postgres terminal status + cleanup MinIO staging
+ remove local download dir.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from loguru import logger
from temporalio import activity

from src.storage.postgres import AsyncPostgres
from src.workflow.contracts import FinalizeIn, IngestResult, MarkFailedIn
from src.workflow.staging import build_staging_store


def _rmtree(path: str | None) -> None:
    if not path:
        return
    p = Path(path)
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)


@activity.defn
async def finalize(payload: FinalizeIn) -> IngestResult:
    activity.logger.info(
        "finalize start  doc=%s  status=%s  chunks=%d",
        payload.ctx.doc_id, payload.graph_status, payload.indexed.count,
    )
    activity.heartbeat({"stage": "init", "status": payload.graph_status})

    pg = AsyncPostgres()
    await pg.update_status(
        uuid.UUID(payload.ctx.doc_id), status=payload.graph_status,
    )
    activity.heartbeat({"stage": "pg_status_written"})

    staging = build_staging_store()
    staging.delete_prefix(payload.ctx.workflow_run_id)
    activity.heartbeat({"stage": "staging_cleaned"})

    _rmtree(payload.ctx.cleanup_dir)
    activity.heartbeat({"stage": "local_cleaned"})

    logger.info(
        "finalize  doc={d}  status={s}  chunks={c}  entities={e}  relations={r}",
        d=payload.ctx.doc_id, s=payload.graph_status,
        c=payload.indexed.count,
        e=payload.entities, r=payload.relations,
    )
    return IngestResult(
        doc_id=payload.ctx.doc_id,
        chunk_count=payload.indexed.count,
        graph_status=payload.graph_status,
        entities=payload.entities,
        relations=payload.relations,
    )


@activity.defn
async def mark_failed(payload: MarkFailedIn) -> None:
    doc_id = payload.ctx.doc_id if payload.ctx else payload.params.doc_id
    activity.logger.warning(
        "mark_failed start  doc=%s  error=%s", doc_id, payload.error,
    )
    activity.heartbeat({"stage": "init", "doc_id": doc_id})

    pg = AsyncPostgres()
    await pg.update_status(uuid.UUID(doc_id), status="failed", error=payload.error)
    activity.heartbeat({"stage": "pg_failed_written"})

    staging = build_staging_store()
    if payload.ctx:
        staging.delete_prefix(payload.ctx.workflow_run_id)
        _rmtree(payload.ctx.cleanup_dir)
        activity.heartbeat({"stage": "cleanup_done"})
    logger.warning(
        "mark_failed  doc={d}  error={e}", d=doc_id, e=payload.error,
    )

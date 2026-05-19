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

from src.observability.ingest_metrics_extractor import parse_activity_timings
from src.storage.ingest_metrics import build_ingest_metrics_store
from src.storage.postgres import AsyncPostgres
from src.workflow.client import get_temporal_client
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

    await _persist_ingest_metrics(payload)
    activity.heartbeat({"stage": "metrics_written"})

    wikibase_status = (
        payload.wikibase.status if payload.wikibase else "skipped"
    )
    logger.info(
        "finalize  doc={d}  status={s}  chunks={c}  entities={e}  "
        "relations={r}  wikibase={w}",
        d=payload.ctx.doc_id, s=payload.graph_status,
        c=payload.indexed.count,
        e=payload.entities, r=payload.relations,
        w=wikibase_status,
    )
    return IngestResult(
        doc_id=payload.ctx.doc_id,
        chunk_count=payload.indexed.count,
        graph_status=payload.graph_status,
        entities=payload.entities,
        relations=payload.relations,
        wikibase_status=wikibase_status,
    )


async def _persist_ingest_metrics(payload: FinalizeIn) -> None:
    """Pull this workflow's history, derive per-activity durations,
    and persist them into ``ingest_metrics``.

    Best-effort — any failure (Temporal momentarily unavailable,
    Postgres connectivity, malformed history) is logged but does
    not fail the workflow.  Note that ``fetch_history`` from inside
    ``finalize`` sees the history up to (but not including) finalize's
    own COMPLETED event, so finalize's own duration is recorded
    only on the NEXT ingest's read.  Acceptable for v1; a
    self-instrumented finalize timing line is Phase 2.
    """
    try:
        client = await get_temporal_client()
        info = activity.info()
        handle = client.get_workflow_handle(
            info.workflow_id, run_id=info.workflow_run_id,
        )
        history = await handle.fetch_history()
        rows = parse_activity_timings(
            history,
            doc_id=payload.ctx.doc_id,
            workflow_id=info.workflow_id,
            workflow_run_id=info.workflow_run_id,
            version_tag=payload.version_tag,
            model=payload.model,
            env=payload.env,
        )
        if not rows:
            activity.logger.info("ingest_metrics: no completed activities yet")
            return
        store = build_ingest_metrics_store()
        inserted = await store.insert_metrics(rows)
        activity.logger.info(
            "ingest_metrics  rows=%d  inserted=%d  version_tag=%s  model=%s",
            len(rows), inserted, payload.version_tag, payload.model,
        )
    except Exception as exc:  # noqa: BLE001
        activity.logger.warning(
            "ingest_metrics persist failed (best-effort): %s", exc,
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

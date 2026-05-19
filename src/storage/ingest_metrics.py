"""Async Postgres wrapper for the ``ingest_metrics`` table.

Owned by the analytics layer (Stage 3 of the Grafana plan).  One row
per (workflow_run, activity_name, attempt) — populated from
Temporal event history in the ``finalize`` activity (Stage 5), then
read by the Grafana Postgres datasource for version-compare and
per-run drill-down dashboards (Stage 6).

The pattern matches ``src/storage/postgres.py``: one
``psycopg.AsyncConnection`` per call (low call volume; LlamaIndex
is the heavy consumer, not analytics writes).
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import psycopg
from pydantic import BaseModel, ConfigDict, Field

from src.config import settings


class MetricRow(BaseModel):
    """One activity-execution attempt, schema-mirroring ingest_metrics.

    Frozen — every row is built once by the extractor and never
    mutated.
    """

    model_config = ConfigDict(frozen=True)

    doc_id: str
    workflow_id: str
    workflow_run_id: str
    activity_name: str
    attempt: int = Field(ge=1)
    duration_ms: int = Field(ge=0)
    started_at: datetime
    completed_at: datetime
    version_tag: str | None = None
    model: str | None = None
    env: str | None = None


class AsyncIngestMetrics:
    """Bulk INSERT wrapper with ON CONFLICT DO NOTHING.

    The conflict target ``(workflow_run_id, activity_name, attempt)``
    matches the UNIQUE constraint in ``scripts/setup_db.py`` — so
    re-running the finalize hook on the same workflow (e.g. when
    Temporal replays history) is a no-op rather than a duplicate.
    """

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or settings.postgres.dsn

    async def insert_metrics(self, rows: Iterable[MetricRow]) -> int:
        """Bulk-insert measurement rows, returning the count of
        rows actually written (after de-dup against the UNIQUE
        constraint).
        """
        payload = [
            (
                r.doc_id,
                r.workflow_id,
                r.workflow_run_id,
                r.activity_name,
                r.attempt,
                r.duration_ms,
                r.started_at,
                r.completed_at,
                r.version_tag,
                r.model,
                r.env,
            )
            for r in rows
        ]
        if not payload:
            return 0

        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO ingest_metrics (
                        doc_id, workflow_id, workflow_run_id,
                        activity_name, attempt, duration_ms,
                        started_at, completed_at,
                        version_tag, model, env
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (workflow_run_id, activity_name, attempt)
                    DO NOTHING
                    """,
                    payload,
                )
                inserted = cur.rowcount
            await conn.commit()
        return inserted


def build_ingest_metrics_store() -> AsyncIngestMetrics:
    """Factory consistent with the rest of ``src/storage/``."""
    return AsyncIngestMetrics()

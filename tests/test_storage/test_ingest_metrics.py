"""Integration test for src/storage/ingest_metrics.py against the
local Postgres (started via docker compose).  Skipped when PG isn't
reachable so the unit-suite still runs offline.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from src.config import settings
from src.storage.ingest_metrics import (
    AsyncIngestMetrics,
    MetricRow,
    build_ingest_metrics_store,
)


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(),
    reason="local Postgres unreachable — integration test skipped",
)


def _row(workflow_run_id: str, name: str, attempt: int = 1) -> MetricRow:
    now = datetime.now(UTC)
    return MetricRow(
        doc_id=str(uuid.uuid4()),
        workflow_id="ingest-test",
        workflow_run_id=workflow_run_id,
        activity_name=name,
        attempt=attempt,
        duration_ms=42,
        started_at=now - timedelta(milliseconds=42),
        completed_at=now,
        version_tag="tests",
        model="test-model",
        env="ci",
    )


def _cleanup(workflow_run_id: str) -> None:
    with psycopg.connect(settings.postgres.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingest_metrics WHERE workflow_run_id = %s",
                (workflow_run_id,),
            )
        conn.commit()


def test_build_returns_async_wrapper():
    s = build_ingest_metrics_store()
    assert isinstance(s, AsyncIngestMetrics)


def test_bulk_insert_then_dedup_on_unique_constraint():
    """Two writes of the same (workflow_run_id, activity, attempt)
    tuple should land once.  Mirrors the finalize-replay scenario
    (Temporal sometimes drives finalize twice on retry)."""

    wf = str(uuid.uuid4())
    rows = [_row(wf, name) for name in (
        "fetch_source", "parse_and_chunk", "extract_kg",
    )]

    async def go() -> tuple[int, int]:
        store = build_ingest_metrics_store()
        first = await store.insert_metrics(rows)
        second = await store.insert_metrics(rows)   # re-emit identical batch
        return first, second

    try:
        first, second = asyncio.run(go())
        assert first == 3
        assert second == 0
    finally:
        _cleanup(wf)


def test_empty_batch_is_noop():
    async def go() -> int:
        store = build_ingest_metrics_store()
        return await store.insert_metrics([])
    assert asyncio.run(go()) == 0


def test_retry_writes_attempt_as_separate_row():
    """One workflow_run_id + same activity_name + different attempt
    is allowed by the UNIQUE constraint."""

    wf = str(uuid.uuid4())
    rows = [
        _row(wf, "extract_kg", attempt=1),
        _row(wf, "extract_kg", attempt=2),
    ]

    async def go() -> int:
        store = build_ingest_metrics_store()
        return await store.insert_metrics(rows)

    try:
        assert asyncio.run(go()) == 2
    finally:
        _cleanup(wf)

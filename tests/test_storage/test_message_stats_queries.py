"""status_counts_by + timeline_counts aggregate documents rows. Integration —
skipped when Postgres is unreachable."""

from __future__ import annotations

import asyncio
import uuid
from datetime import date

import psycopg
import pytest

from src.config import settings
from src.storage.postgres import AsyncPostgres


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="local Postgres unreachable"
)

_TAG = "stats-test-" + uuid.uuid4().hex[:8]  # unique marker for cleanup via path
_CH_ALPHA = f"alpha-{_TAG}"
_CH_BETA = f"beta-{_TAG}"


def _seed() -> None:
    rows = [
        # source_channel, source_group, status, doc_date
        (_CH_ALPHA, "news", "completed", "2026-07-20"),
        (_CH_ALPHA, "news", "completed", "2026-07-21"),
        (_CH_ALPHA, "news", "failed", "2026-07-21"),
        (_CH_BETA, "analytics", "completed", "2026-07-21"),
        ("", "", "completed", "2026-07-21"),  # non-TG: must be excluded
    ]
    with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for ch, gr, st, dd in rows:
                cur.execute(
                    "INSERT INTO documents "
                    "(id, path, status, source_channel, source_group, doc_date) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (str(uuid.uuid4()), f"{_TAG}/x.txt", st, ch, gr, dd),
                )


def _cleanup() -> None:
    with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM documents WHERE path LIKE %s", (f"{_TAG}/%",))


def test_status_counts_by_channel() -> None:
    pg = AsyncPostgres(settings.postgres.dsn)
    try:
        _seed()
        rows = asyncio.run(pg.status_counts_by("source_channel"))
        by_key = {r["key"]: r for r in rows}
        assert "" not in by_key  # non-TG excluded
        assert by_key[_CH_ALPHA]["total"] == 3
        assert by_key[_CH_ALPHA]["completed"] == 2
        assert by_key[_CH_ALPHA]["failed"] == 1
        assert by_key[_CH_BETA]["completed"] == 1
        # sorted by total desc → alpha before beta
        keys = [r["key"] for r in rows if r["key"] in (_CH_ALPHA, _CH_BETA)]
        assert keys.index(_CH_ALPHA) < keys.index(_CH_BETA)
    finally:
        _cleanup()


def test_timeline_counts_by_channel_on_doc_date() -> None:
    pg = AsyncPostgres(settings.postgres.dsn)
    try:
        _seed()
        buckets = asyncio.run(
            pg.timeline_counts(date_field="doc_date", group_by="channel",
                               channel=_CH_ALPHA)
        )
        by_day = {b["day"]: b["count"] for b in buckets}
        assert by_day[date(2026, 7, 20)] == 1
        assert by_day[date(2026, 7, 21)] == 2
        assert all(b["key"] == _CH_ALPHA for b in buckets)
    finally:
        _cleanup()

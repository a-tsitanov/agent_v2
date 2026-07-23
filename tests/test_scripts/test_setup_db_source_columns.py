"""Integration test for the documents source_channel/source_group columns,
the 'skipped' status CHECK, and the source_channel backfill. Skipped when
local Postgres is unreachable (mirrors test_storage/test_ingest_metrics.py).
"""

from __future__ import annotations

import uuid

import psycopg
import pytest

from scripts.setup_db import _BACKFILL_SOURCE_CHANNEL_SQL, _DOCUMENTS_DDL
from src.config import settings


def _pg_reachable() -> bool:
    try:
        with psycopg.connect(settings.postgres.dsn, connect_timeout=2):
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="local Postgres unreachable"
)


def test_source_columns_backfill_and_skipped_status() -> None:
    # DDL only (idempotent create/alter) — does NOT run the backfill, so no
    # data is touched here.
    with (
        psycopg.connect(settings.postgres.dsn, autocommit=True) as ddl_conn,
        ddl_conn.cursor() as cur,
    ):
        cur.execute(_DOCUMENTS_DDL)

    doc_id = uuid.uuid4()
    path = f"{doc_id}/tg_somechannel_4567.txt"
    conn = psycopg.connect(settings.postgres.dsn, autocommit=False)
    try:
        with conn.cursor() as cur:
            # 'skipped' must be an accepted status now.
            cur.execute(
                "INSERT INTO documents (id, path, status, source_channel) "
                "VALUES (%s, %s, 'skipped', '')",
                (str(doc_id), path),
            )
            cur.execute(_BACKFILL_SOURCE_CHANNEL_SQL)
            cur.execute(
                "SELECT source_channel, source_group FROM documents WHERE id = %s",
                (str(doc_id),),
            )
            channel, group = cur.fetchone()
        assert channel == "somechannel"
        assert group == ""
    finally:
        # Rolls back both the inserted row AND any incidental backfill of
        # other rows, so this test never permanently mutates the table.
        conn.rollback()
        conn.close()

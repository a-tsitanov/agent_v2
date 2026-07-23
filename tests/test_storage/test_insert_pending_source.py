"""insert_pending persists source_channel/source_group. Integration —
skipped when Postgres is unreachable."""

from __future__ import annotations

import asyncio
import uuid

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


def test_insert_pending_writes_source_columns() -> None:
    doc_id = uuid.uuid4()
    pg = AsyncPostgres(settings.postgres.dsn)

    async def go():
        await pg.insert_pending(
            doc_id, f"{doc_id}/tg_chan_1.txt",
            source_channel="chan", source_group="news",
        )

    try:
        asyncio.run(go())
        with psycopg.connect(settings.postgres.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source_channel, source_group FROM documents WHERE id = %s",
                (str(doc_id),),
            )
            assert cur.fetchone() == ("chan", "news")
    finally:
        with psycopg.connect(settings.postgres.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents WHERE id = %s", (str(doc_id),))

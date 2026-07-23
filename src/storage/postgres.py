"""Async Postgres client for the documents table.

Tracks ingestion-job state across the upload → worker → status flow.
Schema is the one initialised by ``scripts/setup_db.py`` (Stage 1).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.storage.pg_pool import get_pg_pool


@dataclass
class DocumentRow:
    id: uuid.UUID
    path: str
    department: str
    doc_type: str
    status: str
    error: str
    summary: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> DocumentRow:
        return cls(
            id=row["id"],
            path=row["path"],
            department=row["department"] or "",
            doc_type=row["doc_type"] or "",
            status=row["status"],
            error=row["error"] or "",
            summary=row["summary"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class AsyncPostgres:
    """Thin async wrapper around the document-status table.

    Connections come from the per-process pool
    (``src/storage/pg_pool.py``) so high-volume status updates reuse
    connections instead of opening one per call.  Passing an explicit
    ``dsn`` (e.g. a one-off script pointed at another database) keeps
    the legacy connect-per-call path.
    """

    def __init__(self, dsn: str | None = None) -> None:
        # None → shared pool; explicit dsn → legacy direct connect.
        self._dsn = dsn

    @asynccontextmanager
    async def _conn(self) -> AsyncIterator[psycopg.AsyncConnection]:
        if self._dsn is None:
            pool = await get_pg_pool()
            async with pool.connection() as conn:
                yield conn
        else:
            async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
                yield conn

    async def insert_pending(
        self, doc_id: uuid.UUID, path: str,
        department: str = "", doc_type: str = "",
        doc_date: str | None = None,
        source_channel: str = "", source_group: str = "",
    ) -> None:
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO documents
                        (id, path, department, doc_type, doc_date,
                         source_channel, source_group, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                    """,
                    (str(doc_id), path, department, doc_type, doc_date,
                     source_channel, source_group),
                )
            await conn.commit()

    async def update_status(
        self,
        doc_id: uuid.UUID,
        *,
        status: str,
        error: str = "",
        summary: str = "",
    ) -> None:
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE documents
                       SET status = %s,
                           error = %s,
                           summary = COALESCE(NULLIF(%s, ''), summary),
                           updated_at = NOW()
                     WHERE id = %s
                    """,
                    (status, error, summary, str(doc_id)),
                )
            await conn.commit()

    async def list_id_path(self) -> list[tuple[str, str]]:
        """Return `(doc_id, path)` for every registered document.

        Used by the legacy `doc_id` backfill to map a chunk's stored
        `file_path` back to its document id."""
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute("SELECT id, path FROM documents")
            rows = await cur.fetchall()
        return [(str(r[0]), r[1]) for r in rows]

    async def get(self, doc_id: uuid.UUID) -> DocumentRow | None:
        async with (
            self._conn() as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(
                "SELECT * FROM documents WHERE id = %s", (str(doc_id),),
            )
            row = await cur.fetchone()
        return DocumentRow.from_dict(row) if row else None

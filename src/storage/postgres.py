"""Async Postgres client for the documents table.

Tracks ingestion-job state across the upload → worker → status flow.
Schema is the one initialised by ``scripts/setup_db.py`` (Stage 1).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.config import settings


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

    Uses ``psycopg.AsyncConnection`` per call — connection pooling is
    not in scope for the prototype (LlamaIndex is the heavy
    consumer; document-table calls are infrequent).
    """

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or settings.postgres.dsn

    async def insert_pending(
        self, doc_id: uuid.UUID, path: str,
        department: str = "", doc_type: str = "",
    ) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO documents (id, path, department, doc_type, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    """,
                    (str(doc_id), path, department, doc_type),
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
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
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

    async def get(self, doc_id: uuid.UUID) -> DocumentRow | None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT * FROM documents WHERE id = %s", (str(doc_id),),
                )
                row = await cur.fetchone()
        return DocumentRow.from_dict(row) if row else None

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

DOC_STATUSES: tuple[str, ...] = (
    "pending", "processing", "completed", "vector_only", "failed", "skipped",
)

_DIMENSIONS = {"source_channel", "source_group"}
_DATE_FIELDS = {"created_at", "doc_date"}
_GROUP_BY_COL = {"channel": "source_channel", "group": "source_group"}


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

    async def status_counts_by(
        self,
        dimension: str,
        since: object | None = None,
        until: object | None = None,
    ) -> list[dict]:
        """Per-key (channel or group) count of each pipeline status.

        `dimension` MUST be 'source_channel' or 'source_group' — validated
        against a hardcoded set before interpolation. Empty keys (non-TG
        docs) are excluded. `since`/`until` bound `created_at` (inclusive /
        exclusive). Returns dicts with a count per status in DOC_STATUSES plus
        `total`, sorted by total desc."""
        if dimension not in _DIMENSIONS:
            raise ValueError(f"bad dimension {dimension!r}")
        where = [f"{dimension} <> ''"]
        params: list[object] = []
        if since is not None:
            where.append("created_at >= %s")
            params.append(since)
        if until is not None:
            where.append("created_at < %s")
            params.append(until)
        sql = (
            f"SELECT {dimension} AS key, status, COUNT(*) AS n "
            f"FROM documents WHERE {' AND '.join(where)} "
            f"GROUP BY key, status"
        )
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            raw = await cur.fetchall()
        agg: dict[str, dict] = {}
        for key, status, n in raw:
            row = agg.setdefault(
                key,
                {"key": key, "total": 0, **{s: 0 for s in DOC_STATUSES}},
            )
            if status in row:
                row[status] = n
            row["total"] += n
        return sorted(agg.values(), key=lambda r: r["total"], reverse=True)

    async def timeline_counts(
        self,
        date_field: str = "created_at",
        group_by: str | None = None,
        channel: str | None = None,
        group: str | None = None,
        since: object | None = None,
        until: object | None = None,
    ) -> list[dict]:
        """Daily message counts. `date_field` ∈ {'created_at','doc_date'};
        `group_by` ∈ {None,'channel','group'} adds a per-key breakdown.
        Optional `channel`/`group` filter to one key. All identifiers are
        allowlisted; values bind as parameters. Ordered by day."""
        if date_field not in _DATE_FIELDS:
            raise ValueError(f"bad date_field {date_field!r}")
        keycol = None
        if group_by is not None:
            if group_by not in _GROUP_BY_COL:
                raise ValueError(f"bad group_by {group_by!r}")
            keycol = _GROUP_BY_COL[group_by]
        select = [f"date_trunc('day', {date_field})::date AS day"]
        group_cols = ["day"]
        if keycol:
            select.append(f"{keycol} AS key")
            group_cols.append("key")
        where = [f"{date_field} IS NOT NULL"]
        params: list[object] = []
        if channel is not None:
            where.append("source_channel = %s")
            params.append(channel)
        if group is not None:
            where.append("source_group = %s")
            params.append(group)
        if since is not None:
            where.append(f"{date_field} >= %s")
            params.append(since)
        if until is not None:
            where.append(f"{date_field} < %s")
            params.append(until)
        sql = (
            f"SELECT {', '.join(select)}, COUNT(*) AS n "
            f"FROM documents WHERE {' AND '.join(where)} "
            f"GROUP BY {', '.join(group_cols)} ORDER BY day"
        )
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(sql, params)
            raw = await cur.fetchall()
        out: list[dict] = []
        for rec in raw:
            if keycol:
                day, key, n = rec
                out.append({"day": day, "key": key, "count": n})
            else:
                day, n = rec
                out.append({"day": day, "count": n})
        return out

"""Postgres access for the statistics subsystem.

The single query layer for `stat_indicator` / `stat_observation`, so
every surface reports identical numbers — the same discipline
`_stats_by` follows for `/api/v1/stats`.  Query construction is split
into pure builders so it can be asserted exactly without a live
database.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.stats.align import GRANULARITIES, VALUE_KINDS
from src.storage.pg_pool import get_pg_pool

_SERIES_COLUMNS = (
    "period_start, period_end, dims, value, sample_n, revision, source_doc_id"
)


def build_series_query(
    indicator_id: int,
    since: date | None,
    until: date | None,
    dims: dict[str, Any] | None,
    revision: int | None,
) -> tuple[str, list[Any]]:
    """Rows for one indicator, newest revision per period unless pinned."""
    params: list[Any] = [indicator_id]
    where = ["indicator_id = %s"]
    if since is not None:
        where.append("period_start >= %s")
        params.append(since)
    if until is not None:
        where.append("period_start <= %s")
        params.append(until)
    if dims:
        where.append("dims @> %s")
        params.append(json.dumps(dims))
    if revision is not None:
        where.append("revision = %s")
        params.append(revision)
        sql = (
            f"SELECT {_SERIES_COLUMNS} FROM stat_observation "
            f"WHERE {' AND '.join(where)} ORDER BY period_start, dims"
        )
        return sql, params
    sql = (
        f"SELECT DISTINCT ON (period_start, dims) {_SERIES_COLUMNS} "
        f"FROM stat_observation WHERE {' AND '.join(where)} "
        "ORDER BY period_start, dims, revision DESC"
    )
    return sql, params


def build_search_query(
    query: str, source: str | None, limit: int,
) -> tuple[str, list[Any]]:
    """Trigram search over the registry — the only searchable surface
    the subsystem has; values themselves carry no semantics."""
    # Placeholder order follows the SQL below: two in the SELECT
    # (similarity scoring), then two in the WHERE (the `%` trigram
    # operator, escaped as `%%`), then the optional source, then LIMIT.
    params: list[Any] = [query, query]
    where = ["(title %% %s OR question_text %% %s)"]
    params.extend([query, query])
    if source is not None:
        where.append("source = %s")
        params.append(source)
    params.append(limit)
    sql = (
        "SELECT id, source, code, title, question_text, unit, value_kind, "
        "granularity, dims_schema, entity_vid, "
        "GREATEST(similarity(title, %s), similarity(question_text, %s)) AS score "
        "FROM stat_indicator "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY score DESC, title LIMIT %s"
    )
    return sql, params


def _row_out(row: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe projection: dates to ISO, NUMERIC to float, UUID to str."""
    return {
        "period_start": row["period_start"].isoformat(),
        "period_end": row["period_end"].isoformat(),
        "dims": row["dims"],
        "value": float(row["value"]),
        "sample_n": row["sample_n"],
        "revision": row["revision"],
        "source_doc_id": (
            str(row["source_doc_id"]) if row["source_doc_id"] else None
        ),
    }


class StatsRepository:
    """Async wrapper over the two `stat_*` tables."""

    def __init__(self, dsn: str | None = None) -> None:
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

    async def search_indicators(
        self, query: str, *, source: str | None = None, limit: int = 20,
    ) -> list[dict[str, Any]]:
        sql, params = build_search_query(query, source, limit)
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return [{**r, "score": float(r["score"])} for r in rows]

    async def get_indicator(self, indicator_id: int) -> dict[str, Any] | None:
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, source, code, title, question_text, unit, "
                "value_kind, granularity, dims_schema, entity_vid "
                "FROM stat_indicator WHERE id = %s",
                [indicator_id],
            )
            return await cur.fetchone()

    async def series(
        self,
        indicator_id: int,
        *,
        since: date | None = None,
        until: date | None = None,
        dims: dict[str, Any] | None = None,
        revision: int | None = None,
    ) -> list[dict[str, Any]]:
        if dims is not None and not isinstance(dims, dict):
            raise ValueError("dims must be an object mapping name → value")
        sql, params = build_series_query(
            indicator_id, since, until, dims, revision,
        )
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return [_row_out(r) for r in rows]

    async def upsert_indicator(
        self,
        *,
        source: str,
        code: str,
        title: str,
        unit: str,
        value_kind: str,
        granularity: str,
        question_text: str = "",
        dims_schema: dict[str, Any] | None = None,
        entity_vid: str | None = None,
    ) -> int:
        if value_kind not in VALUE_KINDS:
            raise ValueError(f"unknown value_kind {value_kind!r}")
        if granularity not in GRANULARITIES:
            raise ValueError(f"unknown granularity {granularity!r}")
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO stat_indicator
                        (source, code, title, question_text, unit,
                         value_kind, granularity, dims_schema, entity_vid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, code) DO UPDATE SET
                        title = EXCLUDED.title,
                        question_text = EXCLUDED.question_text,
                        unit = EXCLUDED.unit,
                        value_kind = EXCLUDED.value_kind,
                        granularity = EXCLUDED.granularity,
                        dims_schema = EXCLUDED.dims_schema,
                        entity_vid = EXCLUDED.entity_vid
                    RETURNING id
                    """,
                    (source, code, title, question_text, unit, value_kind,
                     granularity, json.dumps(dims_schema or {}), entity_vid),
                )
                row = await cur.fetchone()
            await conn.commit()
        return int(row[0]) if not isinstance(row, dict) else int(row["id"])

    async def upsert_observations(
        self, rows: Sequence[dict[str, Any]],
    ) -> int:
        """Insert or restate observations.  Idempotent by
        (indicator_id, period_start, dims, revision)."""
        if not rows:
            return 0
        payload = [
            (
                r["indicator_id"], r["period_start"], r["period_end"],
                json.dumps(r.get("dims") or {}), r["value"],
                r.get("sample_n"), int(r.get("revision", 0)),
                str(r["source_doc_id"]) if r.get("source_doc_id") else None,
            )
            for r in rows
        ]
        async with self._conn() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO stat_observation
                        (indicator_id, period_start, period_end, dims,
                         value, sample_n, revision, source_doc_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (indicator_id, period_start, dims, revision)
                    DO UPDATE SET
                        period_end = EXCLUDED.period_end,
                        value = EXCLUDED.value,
                        sample_n = EXCLUDED.sample_n,
                        source_doc_id = EXCLUDED.source_doc_id,
                        loaded_at = now()
                    """,
                    payload,
                )
            await conn.commit()
        return len(payload)

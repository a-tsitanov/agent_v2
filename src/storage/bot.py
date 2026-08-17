"""Postgres access for the Telegram bot: who may use it, and what was asked.

Same split as `src/storage/stats.py` — pure query builders so the SQL can
be asserted exactly without a live database, thin async methods over the
process pool. Async (unlike the ER verdict cache's sync pool) because the
bot is aiogram, i.e. already on an event loop.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.storage.pg_pool import get_pg_pool

USER_COLUMNS = (
    "telegram_id, username, status, role, daily_quota, "
    "created_at, approved_at, approved_by"
)
REQUEST_COLUMNS = (
    "id, telegram_id, chat_id, command, args, status, "
    "started_at, finished_at, answer, sources, error"
)

STATUSES = ("pending", "active", "blocked")
ROLES = ("client", "admin")


def build_recent_requests_query(telegram_id: int, limit: int) -> tuple[str, list[Any]]:
    """This user's requests, newest first.

    Scoped to one user by construction — `/history` must not become a way
    to read other people's questions, and neither must a caller that
    forgets a filter.
    """
    sql = (
        f"SELECT {REQUEST_COLUMNS} FROM bot_request "
        "WHERE telegram_id = %s ORDER BY started_at DESC LIMIT %s"
    )
    return sql, [telegram_id, max(1, int(limit))]


def build_list_users_query(status: str | None) -> tuple[str, list[Any]]:
    """All users, or only those in one status. Oldest first, so the
    longest-waiting pending request is at the top of an admin's list."""
    params: list[Any] = []
    where = ""
    if status is not None:
        where = "WHERE status = %s "
        params.append(status)
    return f"SELECT {USER_COLUMNS} FROM bot_user {where}ORDER BY created_at", params


class BotRepository:
    """Async wrapper over `bot_user` / `bot_request`."""

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

    # ── users ────────────────────────────────────────────────────────

    async def get_or_create_user(
        self, telegram_id: int, username: str = "",
    ) -> dict[str, Any]:
        """The user's row, creating a `pending` one if this id is new.

        On conflict ONLY `username` is refreshed. Status and role are left
        alone deliberately: an approved user who sends `/start` again must
        not be silently reset to pending, which would revoke their access
        without anyone deciding to.
        """
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO bot_user (telegram_id, username) VALUES (%s, %s) "
                "ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username "
                f"RETURNING {USER_COLUMNS}",
                (telegram_id, username or ""),
            )
            return await cur.fetchone()

    async def set_status(
        self, telegram_id: int, status: str, *, approved_by: int | None = None,
    ) -> None:
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE bot_user SET status = %s, approved_at = now(), "
                "approved_by = %s WHERE telegram_id = %s",
                (status, approved_by, telegram_id),
            )

    async def set_role(self, telegram_id: int, role: str) -> None:
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE bot_user SET role = %s WHERE telegram_id = %s",
                (role, telegram_id),
            )

    async def list_users(self, status: str | None = None) -> list[dict[str, Any]]:
        sql, params = build_list_users_query(status)
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())

    # ── requests ─────────────────────────────────────────────────────

    async def count_requests_today(self, telegram_id: int) -> int:
        """Requests this user started today, for the quota check.

        Counts EVERY row, including refusals — otherwise a user at their
        limit could keep generating refusals for free, and each refusal
        still costs a round trip.
        """
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM bot_request "
                "WHERE telegram_id = %s AND started_at >= date_trunc('day', now())",
                (telegram_id,),
            )
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def start_request(
        self, *, telegram_id: int, chat_id: int, command: str, args: str = "",
        status: str = "running",
    ) -> int:
        """Record a request BEFORE the work starts; returns its id.

        A crash mid-search therefore leaves a `running` row. That is
        correct — it is the evidence that the work began.
        """
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO bot_request (telegram_id, chat_id, command, args, status) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (telegram_id, chat_id, command, args, status),
            )
            row = await cur.fetchone()
        return int(row[0])

    async def finish_request(
        self, request_id: int, *, status: str, answer: str = "",
        sources: list[dict[str, Any]] | None = None, error: str = "",
    ) -> None:
        async with self._conn() as conn, conn.cursor() as cur:
            await cur.execute(
                "UPDATE bot_request SET status = %s, finished_at = now(), "
                "answer = %s, sources = %s, error = %s WHERE id = %s",
                (status, answer, json.dumps(sources or []), error, request_id),
            )

    async def recent_requests(
        self, telegram_id: int, limit: int = 10,
    ) -> list[dict[str, Any]]:
        sql, params = build_recent_requests_query(telegram_id, limit)
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())

    async def get_request(self, request_id: int) -> dict[str, Any] | None:
        async with self._conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {REQUEST_COLUMNS} FROM bot_request WHERE id = %s",
                (request_id,),
            )
            return await cur.fetchone()


__all__ = [
    "ROLES",
    "STATUSES",
    "BotRepository",
    "build_list_users_query",
    "build_recent_requests_query",
]

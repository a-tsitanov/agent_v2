"""`bot_user` / `bot_request` access, against a stub connection.

The stub HONOURS `row_factory`, like the stats-repository one: dict rows
only when `dict_row` was actually asked for, tuples otherwise. On live
psycopg3 the pool sets no row factory, so a read that forgets it gets
tuples and `row["status"]` raises — that class of bug has to be visible
here, not only in production.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest
from psycopg.rows import dict_row

from src.storage.bot import (
    BotRepository,
    build_list_users_query,
    build_recent_requests_query,
)

# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubCursor:
    rows: list[dict]
    row_factory: object = None
    executed: list[tuple] = field(default_factory=list)

    async def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def _shaped(self, row):
        if self.row_factory is dict_row:
            return row
        return tuple(row.values()) if isinstance(row, dict) else row

    async def fetchall(self):
        return [self._shaped(r) for r in self.rows]

    async def fetchone(self):
        return self._shaped(self.rows[0]) if self.rows else None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@dataclass
class _StubConn:
    cur: _StubCursor

    def cursor(self, *a, **kw):
        self.cur.row_factory = kw.get("row_factory")
        return self.cur


def _repo_with(rows: list[dict]) -> tuple[BotRepository, _StubConn]:
    conn = _StubConn(cur=_StubCursor(rows=rows))
    repo = BotRepository()

    @asynccontextmanager
    async def _conn():
        yield conn

    repo._conn = _conn  # type: ignore[method-assign]
    return repo, conn


def _sql(conn: _StubConn) -> str:
    return conn.cur.executed[-1][0]


def _params(conn: _StubConn) -> Any:
    return conn.cur.executed[-1][1]


# ── query builders ───────────────────────────────────────────────────


def test_recent_requests_is_scoped_to_one_user():
    """`/history` must not become a way to read other people's questions,
    so the user filter is built in, not left to the caller."""
    sql, params = build_recent_requests_query(42, 10)
    assert "WHERE telegram_id = %s" in sql
    assert "ORDER BY started_at DESC" in sql
    assert params == [42, 10]


def test_recent_requests_limit_has_a_floor():
    """A zero or negative limit would render an empty history that looks
    like "you have never asked anything"."""
    _, params = build_recent_requests_query(42, 0)
    assert params[1] == 1


def test_list_users_filters_by_status_only_when_given():
    sql, params = build_list_users_query(None)
    assert "WHERE" not in sql
    assert params == []
    sql, params = build_list_users_query("pending")
    assert "WHERE status = %s" in sql
    assert params == ["pending"]


def test_list_users_is_oldest_first():
    """So the longest-waiting pending request sits at the top of an
    admin's list."""
    sql, _ = build_list_users_query("pending")
    assert "ORDER BY created_at" in sql
    assert "DESC" not in sql.split("ORDER BY")[1]


# ── users ────────────────────────────────────────────────────────────


async def test_get_or_create_user_does_not_reset_status_or_role():
    """The load-bearing one. An approved user sending `/start` again must
    not be silently returned to `pending` — that is an access revocation
    nobody decided on. Only the username refreshes."""
    repo, conn = _repo_with([{"telegram_id": 1, "status": "active", "role": "admin"}])
    await repo.get_or_create_user(1, "vasya")
    sql = _sql(conn)
    assert "ON CONFLICT (telegram_id) DO UPDATE" in sql
    # Only the SET clause — `RETURNING` legitimately names every column,
    # including status and role.
    set_clause = sql.split("DO UPDATE")[1].split("RETURNING")[0]
    assert "username = EXCLUDED.username" in set_clause
    assert "status" not in set_clause
    assert "role" not in set_clause


async def test_get_or_create_user_returns_the_row_as_a_dict():
    repo, conn = _repo_with([{"telegram_id": 1, "status": "pending", "role": "client"}])
    row = await repo.get_or_create_user(1)
    assert row["status"] == "pending"
    assert conn.cur.row_factory is dict_row


async def test_set_status_records_who_approved_and_when():
    repo, conn = _repo_with([])
    await repo.set_status(7, "active", approved_by=99)
    sql, params = conn.cur.executed[-1]
    assert "approved_at = now()" in sql
    assert params == ("active", 99, 7)


# ── requests ─────────────────────────────────────────────────────────


async def test_quota_count_is_scoped_to_the_user_and_to_today():
    repo, conn = _repo_with([{"count": 3}])
    n = await repo.count_requests_today(42)
    sql = _sql(conn)
    assert "telegram_id = %s" in sql
    assert "date_trunc('day', now())" in sql
    assert _params(conn) == (42,)
    assert n == 3


async def test_start_request_writes_a_running_row_and_returns_its_id():
    repo, conn = _repo_with([{"id": 123}])
    rid = await repo.start_request(
        telegram_id=1, chat_id=2, command="/ask", args="что нового",
    )
    assert rid == 123
    sql, params = conn.cur.executed[-1]
    assert "INSERT INTO bot_request" in sql
    assert "RETURNING id" in sql
    assert params == (1, 2, "/ask", "что нового", "running")


async def test_start_request_can_record_a_refusal():
    """Quota and busy refusals are rows too — an audit that records only
    successes is not an audit."""
    repo, conn = _repo_with([{"id": 5}])
    await repo.start_request(
        telegram_id=1, chat_id=2, command="/ask", args="x", status="denied",
    )
    assert _params(conn)[-1] == "denied"


async def test_finish_request_stores_answer_and_sources():
    repo, conn = _repo_with([])
    await repo.finish_request(
        7, status="done", answer="ответ", sources=[{"chunk_id": "c1"}],
    )
    sql, params = conn.cur.executed[-1]
    assert "finished_at = now()" in sql
    assert params[0] == "done"
    assert params[1] == "ответ"
    assert json.loads(params[2]) == [{"chunk_id": "c1"}]
    assert params[-1] == 7


async def test_finish_request_serialises_absent_sources_as_an_empty_array():
    """The column is `JSONB NOT NULL DEFAULT '[]'`; a bare None would
    violate it."""
    repo, conn = _repo_with([])
    await repo.finish_request(7, status="failed", error="boom")
    assert json.loads(_params(conn)[2]) == []


async def test_recent_requests_asks_for_dict_rows():
    repo, conn = _repo_with([{"id": 1, "command": "/ask"}])
    rows = await repo.recent_requests(42, 5)
    assert conn.cur.row_factory is dict_row
    assert rows[0]["command"] == "/ask"


async def test_get_request_returns_none_when_missing():
    repo, _ = _repo_with([])
    assert await repo.get_request(999) is None


async def test_stub_is_honest_about_row_factory():
    """Pins the stub itself: without `dict_row` it must hand back tuples,
    so a read that forgets it fails here rather than in production."""
    conn = _StubConn(cur=_StubCursor(rows=[{"telegram_id": 1, "status": "active"}]))
    async with conn.cursor() as cur:
        await cur.execute("SELECT telegram_id, status FROM bot_user")
        rows = await cur.fetchall()
    assert rows == [(1, "active")]
    with pytest.raises(TypeError):
        _ = rows[0]["status"]

"""The bot's DDL, asserted as text like the other setup_db tests."""

from __future__ import annotations

from scripts.setup_db import _BOT_INDEXES_DDL, _BOT_REQUEST_DDL, _BOT_USER_DDL


def test_tables_are_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS bot_user" in _BOT_USER_DDL
    assert "CREATE TABLE IF NOT EXISTS bot_request" in _BOT_REQUEST_DDL


def test_new_users_are_pending_and_clients():
    """Fail closed: a row that appears by itself must grant nothing."""
    assert "status       TEXT        NOT NULL DEFAULT 'pending'" in _BOT_USER_DDL
    assert "role         TEXT        NOT NULL DEFAULT 'client'" in _BOT_USER_DDL


def test_status_and_role_are_constrained():
    assert "CHECK (status IN ('pending', 'active', 'blocked'))" in _BOT_USER_DDL
    assert "CHECK (role IN ('client', 'admin'))" in _BOT_USER_DDL


def test_request_status_is_constrained_and_includes_refusals():
    """`denied` is a real status: a refusal is recorded, not dropped."""
    assert "CHECK (status IN ('running', 'done', 'failed', 'denied'))" in _BOT_REQUEST_DDL


def test_request_has_no_foreign_key_to_the_user():
    """Deliberate: the record of what someone asked must outlive their
    account being deleted."""
    assert "REFERENCES bot_user" not in _BOT_REQUEST_DDL


def test_sources_default_to_an_empty_array_not_null():
    assert "sources      JSONB       NOT NULL DEFAULT '[]'::jsonb" in _BOT_REQUEST_DDL


def test_one_index_serves_history_and_the_quota_count():
    """`(telegram_id, started_at DESC)` covers both reads this table has;
    anything else would be an index nobody uses."""
    assert "bot_request_user_time_idx" in _BOT_INDEXES_DDL
    assert "(telegram_id, started_at DESC)" in _BOT_INDEXES_DDL
    assert _BOT_INDEXES_DDL.count("CREATE INDEX") == 1

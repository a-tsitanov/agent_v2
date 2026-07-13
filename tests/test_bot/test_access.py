"""Whitelist access control (TDD). Empty whitelist = deny-all (secure default)."""
from __future__ import annotations

from src.bot.access import is_allowed, parse_allowed_users


def test_parse_comma_separated_ids():
    assert parse_allowed_users("1, 2 ,3") == frozenset({1, 2, 3})


def test_parse_empty_is_empty_set():
    assert parse_allowed_users("") == frozenset()
    assert parse_allowed_users(None) == frozenset()


def test_parse_skips_non_integer_tokens():
    assert parse_allowed_users("x, 5, ") == frozenset({5})


def test_allowed_user_passes():
    assert is_allowed(5, frozenset({5, 6})) is True


def test_unlisted_user_denied():
    assert is_allowed(9, frozenset({5, 6})) is False


def test_empty_whitelist_denies_everyone():
    assert is_allowed(5, frozenset()) is False

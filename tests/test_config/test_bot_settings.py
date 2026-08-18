"""`BotSettings`' new knobs, and the admin seed that makes the bot usable."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.bot.access import parse_allowed_users
from src.config import BotSettings

_BOT_ENV = ("BOT_ADMIN_IDS", "BOT_MAX_CONCURRENT", "BOT_DEFAULT_DAILY_QUOTA")


def _s(**kw) -> BotSettings:
    return BotSettings(_env_file=None, **kw)


def test_defaults_match_the_spec(monkeypatch):
    """The CODE's defaults. `_env_file=None` alone is not enough: anything
    that imports pymilvus calls `dotenv.load_dotenv()`, which puts the
    real `.env` into `os.environ`, and the process environment outranks
    the file either way. Run alone this passed; run after the rest of the
    suite it saw the operator's BOT_ADMIN_IDS."""
    for name in _BOT_ENV:
        monkeypatch.delenv(name, raising=False)
    s = _s()
    assert s.max_concurrent == 2
    assert s.default_daily_quota == 20
    assert s.admin_ids == ""


def test_zero_means_unlimited_and_is_accepted(monkeypatch):
    for name in _BOT_ENV:
        monkeypatch.delenv(name, raising=False)
    # 0 is a meaningful value for both, not a mistake to reject.
    s = _s(max_concurrent=0, default_daily_quota=0)
    assert s.max_concurrent == 0
    assert s.default_daily_quota == 0


@pytest.mark.parametrize("field", ["max_concurrent", "default_daily_quota"])
def test_negative_values_are_rejected(field):
    """A negative cap has no meaning — 0 already says "unlimited"."""
    with pytest.raises(ValidationError):
        _s(**{field: -1})


def test_admin_ids_parse_with_the_existing_helper():
    """Same parser as the old whitelist, so the format operators already
    know keeps working."""
    assert parse_allowed_users("1, 2 ,x, 3") == frozenset({1, 2, 3})
    assert parse_allowed_users("") == frozenset()


async def test_seed_admins_makes_them_active_admins():
    from src.bot.seed import seed_admins

    calls: list[tuple] = []

    class _Repo:
        async def get_or_create_user(self, uid, username=""):
            calls.append(("create", uid))
            return {"telegram_id": uid}

        async def set_status(self, uid, status, *, approved_by=None):
            calls.append(("status", uid, status))

        async def set_role(self, uid, role):
            calls.append(("role", uid, role))

    seeded = await seed_admins(_Repo(), "7, 9")
    assert seeded == [7, 9]
    assert ("status", 7, "active") in calls
    assert ("role", 7, "admin") in calls


async def test_seed_admins_survives_a_store_outage():
    """A database hiccup at startup must not stop the bot from starting —
    it just means nobody is seeded, which is logged."""
    from src.bot.seed import seed_admins

    class _Broken:
        async def get_or_create_user(self, uid, username=""):
            raise RuntimeError("postgres down")

    assert await seed_admins(_Broken(), "7") == []

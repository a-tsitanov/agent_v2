"""Seeding the first administrators.

Its own module rather than part of the entrypoint: `__main__` needs
aiogram, which only exists in the image's `bot` extra, so anything living
there is untestable in the dev environment. This is logic, so it lives
where it can be tested.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.bot.access import parse_allowed_users


async def seed_admins(repo: Any, raw_ids: str) -> list[int]:
    """Make the configured ids active admins. Returns the ones seeded.

    Without at least one there is nobody who can approve anybody. The bot
    fails closed, so an empty database means an unusable bot rather than
    an open one — which is the safe way round, but worth saying out loud
    at startup.

    Fail-soft per id: a database hiccup must not stop the bot starting.
    """
    seeded: list[int] = []
    for uid in sorted(parse_allowed_users(raw_ids)):
        try:
            await repo.get_or_create_user(uid)
            await repo.set_status(uid, "active", approved_by=uid)
            await repo.set_role(uid, "admin")
            seeded.append(uid)
        except Exception as exc:
            logger.warning("bot: cannot seed admin {u}: {e}", u=uid, e=exc)
    return seeded


__all__ = ["seed_admins"]

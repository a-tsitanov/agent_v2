"""Telegram user whitelist. An empty whitelist denies EVERYONE — a personal
KB bot must be opted into explicitly (fail closed, never open by default)."""
from __future__ import annotations


def parse_allowed_users(raw: str | None) -> frozenset[int]:
    """Parse a comma-separated ``BOT_ALLOWED_USERS`` into a set of user ids.
    Non-integer tokens are skipped (robust to stray spaces / typos)."""
    ids: set[int] = set()
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            ids.add(int(tok))
        except ValueError:
            continue
    return frozenset(ids)


def is_allowed(user_id: int, allowed: frozenset[int]) -> bool:
    """True iff ``user_id`` is whitelisted. Empty whitelist → always False."""
    return user_id in allowed

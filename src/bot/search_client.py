"""KB search adapter for the bot: POST /api/v1/search/{mode} and return the
synthesised answer. Thin I/O boundary over the existing search API — the bot
never talks to the graph/LLM directly, so it inherits KB-only grounding.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx
from loguru import logger

from src.bot.answers import is_empty_answer

SearchFn = Callable[[str], Awaitable[str]]
_VALID_MODES = frozenset({"auto", "local", "global", "drift"})


def make_search(
    *, api_base: str, api_key: str, mode: str = "auto", timeout_s: float = 120.0,
) -> Callable[[str], Awaitable[str]]:
    """Build an async ``search(query) -> answer`` bound to one search mode.

    Raises on a non-2xx / transport error so the pipeline's fail-soft path can
    turn it into a user-facing "search error" instead of a silent empty reply.
    """
    if mode not in _VALID_MODES:
        raise ValueError(f"unknown search mode {mode!r}, expected one of {sorted(_VALID_MODES)}")
    url = f"{api_base.rstrip('/')}/api/v1/search/{mode}"

    async def search(query: str) -> str:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            resp = await http.post(
                url, headers={"X-API-Key": api_key}, json={"query": query},
            )
            resp.raise_for_status()
            return (resp.json() or {}).get("answer") or ""

    return search


def make_analyze(
    *, api_base: str, api_key: str, timeout_s: float = 180.0,
) -> SearchFn:
    """Build an async ``analyze(query) -> answer`` over the analytical layer
    (POST /api/v1/analyze — plan→compute→synthesize over the graph). Same
    ``{query}``→``{answer}`` contract as search, so it composes identically."""
    url = f"{api_base.rstrip('/')}/api/v1/analyze"

    async def analyze(query: str) -> str:
        async with httpx.AsyncClient(timeout=timeout_s) as http:
            resp = await http.post(
                url, headers={"X-API-Key": api_key}, json={"query": query},
            )
            resp.raise_for_status()
            return (resp.json() or {}).get("answer") or ""

    return analyze


def with_fallback(primary: SearchFn, fallback: SearchFn) -> SearchFn:
    """Return a search that tries ``primary`` first and only falls back to
    ``fallback`` when primary yields nothing — an empty/marker answer OR an
    error. Keeps the common (fast) path fast; the slower fallback runs only on
    a miss."""

    async def search(query: str) -> str:
        try:
            answer = await primary(query)
            if not is_empty_answer(answer):
                return answer
            logger.info("bot search: primary empty, falling back")
        except Exception as exc:
            logger.warning("bot search: primary errored, falling back: {e}", e=exc)
        return await fallback(query)

    return search

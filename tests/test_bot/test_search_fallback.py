"""Search fallback combinator (TDD): primary mode, fall back to a second mode
only when primary yields nothing (empty / marker / error)."""
from __future__ import annotations

import pytest

from src.bot.search_client import with_fallback


@pytest.mark.asyncio
async def test_non_empty_primary_wins_and_fallback_not_called():
    calls = []

    async def primary(q):
        return "реальный ответ"

    async def fallback(q):
        calls.append(q)
        return "FALLBACK"

    search = with_fallback(primary, fallback)
    assert await search("вопрос") == "реальный ответ"
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", ["", "Empty Response"])
async def test_empty_primary_triggers_fallback(empty):
    async def primary(q):
        return empty

    async def fallback(q):
        return "ответ из fallback"

    search = with_fallback(primary, fallback)
    assert await search("вопрос") == "ответ из fallback"


@pytest.mark.asyncio
async def test_primary_error_triggers_fallback():
    async def primary(q):
        raise RuntimeError("primary down")

    async def fallback(q):
        return "ответ из fallback"

    search = with_fallback(primary, fallback)
    assert await search("вопрос") == "ответ из fallback"

"""Answer-source router (TDD): classify → analyze vs search."""
from __future__ import annotations

import pytest

from src.bot.intent import ANALYTICAL, SEARCH
from src.bot.router import make_router


@pytest.mark.asyncio
async def test_analytical_query_goes_to_analyze():
    hits = {"analyze": None, "search": None}

    async def analyze(q):
        hits["analyze"] = q
        return "аналитический ответ"

    async def search(q):
        hits["search"] = q
        return "поисковый ответ"

    route = make_router(lambda q: ANALYTICAL, analyze, search)
    assert await route("сколько сущностей?") == "аналитический ответ"
    assert hits["analyze"] == "сколько сущностей?"
    assert hits["search"] is None


@pytest.mark.asyncio
async def test_search_query_goes_to_search():
    hits = {"analyze": None, "search": None}

    async def analyze(q):
        hits["analyze"] = q
        return "аналитический ответ"

    async def search(q):
        hits["search"] = q
        return "поисковый ответ"

    route = make_router(lambda q: SEARCH, analyze, search)
    assert await route("что про Украину?") == "поисковый ответ"
    assert hits["search"] == "что про Украину?"
    assert hits["analyze"] is None

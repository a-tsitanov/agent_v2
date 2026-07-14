"""Route each query to the analytical layer or the retrieval layer by intent.

``make_router`` returns an async ``answer(query) -> str`` that the pipeline uses
in place of a bare search fn, so intent routing composes with the existing
session / rewrite / fallback machinery unchanged.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

from src.bot.intent import ANALYTICAL

Classifier = Callable[[str], str]
AnswerFn = Callable[[str], Awaitable[str]]


def make_router(classify: Classifier, analyze: AnswerFn, search: AnswerFn) -> AnswerFn:
    """Build ``answer(query)`` that dispatches to ``analyze`` for analytical
    intents and ``search`` otherwise."""

    async def answer(query: str) -> str:
        if classify(query) == ANALYTICAL:
            return await analyze(query)
        return await search(query)

    return answer

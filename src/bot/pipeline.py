"""The bot's answer pipeline: whitelist → session → follow-up rewrite → KB
search → persist. All external boundaries (rewrite LLM, KB search) are injected
so the flow is unit-testable; the session store is a real object.

KB-only: the answer comes solely from ``search`` (the KB orchestration). This
module never adds model/world knowledge of its own.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

from loguru import logger

from src.bot.access import is_allowed
from src.bot.answers import NO_RESULT_MESSAGE, is_empty_answer
from src.bot.session import Turn

DENIED_MESSAGE = "Доступ запрещён. Этот бот отвечает только доверенным пользователям."
_SEARCH_ERROR = "Не удалось получить ответ из базы (ошибка поиска). Попробуйте ещё раз."

RewriteFn = Callable[[list[Turn], str], Awaitable[str]]
SearchFn = Callable[[str], Awaitable[str]]


async def answer_question(
    *,
    chat_id: int,
    user_id: int,
    question: str,
    session,
    allowed: frozenset[int],
    rewrite: RewriteFn,
    search: SearchFn,
) -> str:
    """Produce the bot's reply for one incoming message.

    Denied users get ``DENIED_MESSAGE`` with no search call and no session
    write. On search failure nothing is persisted (the broken turn would poison
    later follow-up rewrites)."""
    if not is_allowed(user_id, allowed):
        return DENIED_MESSAGE

    history = session.load(chat_id)
    standalone = await rewrite(history, question)
    try:
        answer = await search(standalone)
    except Exception as exc:  # fail-soft — don't crash the bot on a search error
        logger.warning("bot search failed for chat={c}: {e}", c=chat_id, e=exc)
        return _SEARCH_ERROR

    if is_empty_answer(answer):
        # No hit / empty synthesis — friendly message, and DON'T persist (a junk
        # turn would poison later follow-up rewrites).
        return NO_RESULT_MESSAGE

    # Persist the ORIGINAL question (what the user actually typed) + the answer,
    # so later rewrites see the real conversation, not the rewritten queries.
    session.append(chat_id, Turn(role="user", text=question))
    session.append(chat_id, Turn(role="assistant", text=answer))
    return answer

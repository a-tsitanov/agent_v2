"""Rewrite a follow-up message into a standalone search query.

"а что по нему?" after a turn about Киев → "Какие удары были по Киеву?" —
so the KB search gets a self-contained query instead of a context-dependent
fragment. The LLM is injected as an async ``complete(prompt) -> str`` so this
module stays testable without a live model. Fail-soft: any error / empty
output / no history → return the original question unchanged.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

from loguru import logger

from src.bot.session import Turn

_INSTRUCTION = (
    "Ты переписываешь последний вопрос пользователя в САМОСТОЯТЕЛЬНЫЙ поисковый "
    "запрос с учётом истории диалога: подставь то, на что ссылаются местоимения "
    "и сокращения. Верни ТОЛЬКО переписанный запрос одной строкой, без пояснений.\n\n"
)


def build_rewrite_prompt(history: list[Turn], question: str) -> str:
    """Assemble the rewrite prompt from prior turns + the new question."""
    lines = [_INSTRUCTION, "История диалога:"]
    for t in history:
        who = "Пользователь" if t.role == "user" else "Ассистент"
        lines.append(f"{who}: {t.text}")
    lines.append(f"\nНовый вопрос: {question}")
    lines.append("Самостоятельный запрос:")
    return "\n".join(lines)


async def rewrite_query(
    history: list[Turn],
    question: str,
    *,
    complete: Callable[[str], Awaitable[str]],
) -> str:
    """Return a standalone query. No history → the question verbatim (no LLM
    call). Otherwise ask ``complete``; fall back to the question on empty/error."""
    if not history:
        return question
    try:
        rewritten = (await complete(build_rewrite_prompt(history, question))).strip()
    except Exception as exc:  # fail-soft — a bad rewrite must not drop the query
        logger.warning("query rewrite failed, using original: {e}", e=exc)
        return question
    return rewritten or question

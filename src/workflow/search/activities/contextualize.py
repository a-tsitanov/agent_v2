"""Rewrite a follow-up question into a standalone one using recent
conversation history (small tier).  Fail-open: returns the original
query on empty history / any error."""
from __future__ import annotations

from temporalio import activity

from src.config import settings
from src.workflow.contracts import (
    ContextualizeParams,
    ContextualizeResult,
    ConversationTurnDict,
)

_PROMPT = (
    "/no_think\n"
    "Перепиши ПОСЛЕДНИЙ вопрос пользователя как самодостаточный, "
    "подставив контекст из истории (раскрой местоимения и отсылки). "
    "Сохрани язык вопроса. Верни ТОЛЬКО переписанный вопрос, без пояснений.\n\n"
    "История:\n{history}\n\nПоследний вопрос: {query}\n\nСамодостаточный вопрос:"
)


def _bound_history(
    turns: list[ConversationTurnDict], *, max_turns: int, max_chars: int
) -> list[ConversationTurnDict]:
    recent = list(turns)[-max_turns:] if max_turns >= 0 else list(turns)
    out: list[ConversationTurnDict] = []
    total = 0
    for t in reversed(recent):  # keep the most recent within the char budget
        c = len(t.content or "")
        # ``max_chars == 0`` ⇒ no char cap (falsy). The ``and out`` guard
        # always keeps at least the latest turn, even if it alone exceeds
        # the budget — better one over-budget turn than empty history.
        if max_chars and total + c > max_chars and out:
            break
        out.append(t)
        total += c
    return list(reversed(out))


def _build_prompt(query: str, turns: list[ConversationTurnDict]) -> str:
    lines = [f"{t.role}: {t.content or ''}" for t in turns]
    return _PROMPT.format(history="\n".join(lines), query=query)


def _get_contextualize_llm():
    """Small-tier LLM (role ``route`` → small per ``_DEFAULT_ROLE_TIERS``).

    Reuses the SAME role the router uses (``get_llm_pool().get('route')``).
    Indirected through a module-level fn so tests can monkeypatch it
    without touching the LiteLLM factory.  Returns the pooled LLM so the
    global N semaphore counts this call."""
    from src.retrieval.llm_pool import get_llm_pool

    return get_llm_pool().get("route")


@activity.defn
async def contextualize_query(params: ContextualizeParams) -> ContextualizeResult:
    """Rewrite a follow-up into a standalone question (small tier).

    Fail-open: empty history, no usable turns, or ANY error → the original
    query (never raises through the Temporal boundary)."""
    if not params.history:
        return ContextualizeResult(query=params.query)
    turns = _bound_history(
        list(params.history),
        max_turns=settings.agent.history_max_turns,
        max_chars=settings.agent.history_max_chars,
    )
    if not turns:
        return ContextualizeResult(query=params.query)
    try:
        from src.retrieval._common import strip_thinking

        llm = _get_contextualize_llm()
        resp = await llm.acomplete(_build_prompt(params.query, turns))
        text = strip_thinking(getattr(resp, "text", None) or str(resp)).strip()
        return ContextualizeResult(query=text or params.query)
    except Exception as exc:  # fail-open
        activity.logger.warning(
            "contextualize_query failed, using raw query: %s", exc
        )
        return ContextualizeResult(query=params.query)


__all__ = ["contextualize_query"]

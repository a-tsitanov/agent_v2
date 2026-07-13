"""Wire the pure ``rewrite_query`` to the app's LLM (litellm via build_llm).

Kept apart from ``query_rewrite`` so that module stays LLM-free and unit-
testable; this file is the thin I/O binding used at runtime.
"""
from __future__ import annotations

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.bot.query_rewrite import rewrite_query
from src.bot.session import Turn
from src.retrieval.llm import build_llm


def make_rewrite():
    """Build an async ``rewrite(history, question) -> standalone_query`` backed
    by the default-role LLM. Reused across chats (the LLM object is cheap to
    hold; concurrency is bounded by the app's LLM pool)."""
    llm = build_llm()

    async def complete(prompt: str) -> str:
        resp = await llm.achat([ChatMessage(role=MessageRole.USER, content=prompt)])
        return resp.message.content or ""

    async def rewrite(history: list[Turn], question: str) -> str:
        return await rewrite_query(history, question, complete=complete)

    return rewrite

"""Follow-up query rewriting (TDD). LLM is injected as an async `complete`."""
from __future__ import annotations

import pytest

from src.bot.query_rewrite import build_rewrite_prompt, rewrite_query
from src.bot.session import Turn


def test_build_prompt_includes_history_and_question():
    hist = [Turn(role="user", text="Расскажи про Киев"),
            Turn(role="assistant", text="Киев — столица Украины")]
    p = build_rewrite_prompt(hist, "а что там с ударами?")
    assert "Киев" in p
    assert "а что там с ударами?" in p


@pytest.mark.asyncio
async def test_no_history_returns_question_verbatim_without_llm():
    called = False

    async def llm(prompt: str) -> str:
        nonlocal called
        called = True
        return "НЕ ДОЛЖНО ИСПОЛЬЗОВАТЬСЯ"

    out = await rewrite_query([], "Что нового?", complete=llm)
    assert out == "Что нового?"
    assert called is False


@pytest.mark.asyncio
async def test_with_history_uses_llm_output_stripped():
    hist = [Turn(role="user", text="Расскажи про Киев"),
            Turn(role="assistant", text="…")]

    async def llm(prompt: str) -> str:
        return "  Какие удары были по Киеву?  "

    out = await rewrite_query(hist, "а что с ударами?", complete=llm)
    assert out == "Какие удары были по Киеву?"


@pytest.mark.asyncio
async def test_llm_failure_falls_back_to_original_question():
    hist = [Turn(role="user", text="x")]

    async def llm(prompt: str) -> str:
        raise RuntimeError("boom")

    out = await rewrite_query(hist, "оригинальный вопрос", complete=llm)
    assert out == "оригинальный вопрос"


@pytest.mark.asyncio
async def test_empty_llm_output_falls_back_to_original_question():
    hist = [Turn(role="user", text="x")]

    async def llm(prompt: str) -> str:
        return "   "

    out = await rewrite_query(hist, "оригинальный вопрос", complete=llm)
    assert out == "оригинальный вопрос"

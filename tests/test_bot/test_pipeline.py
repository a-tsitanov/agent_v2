"""Answer pipeline (TDD): access -> session -> rewrite -> search -> persist.

Uses the REAL InMemorySessionStore; only the external LLM (rewrite) and KB
search are injected as async fakes (unavoidable I/O boundaries)."""
from __future__ import annotations

import pytest

from src.bot.pipeline import DENIED_MESSAGE, NO_RESULT_MESSAGE, answer_question
from src.bot.session import InMemorySessionStore, Turn


def _passthrough_rewrite():
    async def _rw(history, question):
        return question
    return _rw


@pytest.mark.asyncio
async def test_denied_user_gets_denial_and_no_side_effects():
    store = InMemorySessionStore(max_messages=8)
    calls = []

    async def search(q):
        calls.append(q)
        return "SHOULD NOT RUN"

    out = await answer_question(
        chat_id=1, user_id=999, question="секрет?",
        session=store, allowed=frozenset({5}),
        rewrite=_passthrough_rewrite(), search=search,
    )
    assert out == DENIED_MESSAGE
    assert calls == []              # search never called
    assert store.load(1) == []      # session untouched


@pytest.mark.asyncio
async def test_allowed_no_history_searches_question_and_persists():
    store = InMemorySessionStore(max_messages=8)

    async def search(q):
        return f"ответ на: {q}"

    out = await answer_question(
        chat_id=7, user_id=5, question="что нового?",
        session=store, allowed=frozenset({5}),
        rewrite=_passthrough_rewrite(), search=search,
    )
    assert out == "ответ на: что нового?"
    assert store.load(7) == [
        Turn(role="user", text="что нового?"),
        Turn(role="assistant", text="ответ на: что нового?"),
    ]


@pytest.mark.asyncio
async def test_rewritten_query_is_what_search_receives():
    store = InMemorySessionStore(max_messages=8)
    store.append(7, Turn(role="user", text="Расскажи про Киев"))
    store.append(7, Turn(role="assistant", text="Киев — ..."))
    seen = []

    async def rewrite(history, question):
        return "Какие удары были по Киеву?"

    async def search(q):
        seen.append(q)
        return "ответ"

    await answer_question(
        chat_id=7, user_id=5, question="а что с ударами?",
        session=store, allowed=frozenset({5}),
        rewrite=rewrite, search=search,
    )
    assert seen == ["Какие удары были по Киеву?"]
    # the ORIGINAL question is stored, not the rewrite
    assert store.load(7)[-2] == Turn(role="user", text="а что с ударами?")


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", ["", "   ", "Empty Response", "empty response"])
async def test_empty_answer_becomes_friendly_message_and_not_persisted(empty):
    store = InMemorySessionStore(max_messages=8)

    async def search(q):
        return empty

    out = await answer_question(
        chat_id=7, user_id=5, question="про марсиан?",
        session=store, allowed=frozenset({5}),
        rewrite=_passthrough_rewrite(), search=search,
    )
    assert out == NO_RESULT_MESSAGE
    assert "Empty Response" not in out          # never leak the internal marker
    assert store.load(7) == []                  # no junk turn persisted


@pytest.mark.asyncio
async def test_search_failure_is_soft_and_does_not_persist_broken_answer():
    store = InMemorySessionStore(max_messages=8)

    async def search(q):
        raise RuntimeError("MCP down")

    out = await answer_question(
        chat_id=7, user_id=5, question="вопрос",
        session=store, allowed=frozenset({5}),
        rewrite=_passthrough_rewrite(), search=search,
    )
    assert "ошибка" in out.lower() or "не удалось" in out.lower()
    assert store.load(7) == []      # nothing persisted on failure

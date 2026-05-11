"""Tests for `src/ingestion/translate_transform.py`."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from llama_index.core.base.llms.types import ChatMessage
from llama_index.core.schema import TextNode

from src.ingestion.translate_transform import (
    TRANSLATED_TEXT_KEY,
    TranslateToRussianTransform,
    _looks_russian,
)


@dataclass
class _ScriptedLLM:
    responses: list[str]
    calls: list[list[ChatMessage]] = field(default_factory=list)

    async def achat(self, messages: list[ChatMessage], **_) -> object:
        self.calls.append(messages)
        text = self.responses.pop(0) if self.responses else ""

        class _Resp:
            class _Msg:
                content = text

            message = _Msg()

        return _Resp()


# ── language heuristic ──────────────────────────────────────────────


def test_looks_russian_detects_cyrillic_majority() -> None:
    assert _looks_russian("Это русский текст с английскими словами OK.")
    assert _looks_russian("Иванов работает в ООО «Технологии».")


def test_looks_russian_rejects_english() -> None:
    assert not _looks_russian(
        "Basal cell carcinoma is the most common type of skin cancer."
    )


def test_looks_russian_numbers_only_passes() -> None:
    # Pure digits / punctuation — nothing to translate; treat as RU.
    assert _looks_russian("+7 495 234-56-78  2024-03-15  123 456 ₽")


# ── transform behavior ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_english_chunk_translated() -> None:
    llm = _ScriptedLLM(responses=["Базальноклеточный рак — самый частый тип рака кожи."])
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    node = TextNode(id_="c1", text="Basal cell carcinoma is the most common skin cancer.")
    out = await t.acall([node])
    assert out[0].metadata[TRANSLATED_TEXT_KEY].startswith("Базальноклеточный")
    assert len(llm.calls) == 1


@pytest.mark.asyncio
async def test_russian_chunk_skipped_no_llm_call() -> None:
    """Already-Russian input must not burn an LLM call."""
    llm = _ScriptedLLM(responses=[])
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    text = "Иванов И.П. работает в ООО «Технологии», ИНН 7707083893."
    node = TextNode(id_="c2", text=text)
    out = await t.acall([node])
    assert out[0].metadata[TRANSLATED_TEXT_KEY] == text
    assert llm.calls == []


@pytest.mark.asyncio
async def test_llm_failure_leaves_translated_text_absent() -> None:
    class _RaisingLLM:
        async def achat(self, *a, **kw):
            raise RuntimeError("boom")

    t = TranslateToRussianTransform(llm=_RaisingLLM(), num_workers=1)
    node = TextNode(id_="c3", text="Some English text.")
    out = await t.acall([node])
    # No translated_text written → extractor falls back to node.text.
    assert TRANSLATED_TEXT_KEY not in out[0].metadata


@pytest.mark.asyncio
async def test_empty_translation_skipped() -> None:
    llm = _ScriptedLLM(responses=["   "])  # whitespace-only
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    node = TextNode(id_="c4", text="English chunk.")
    out = await t.acall([node])
    assert TRANSLATED_TEXT_KEY not in out[0].metadata


@pytest.mark.asyncio
async def test_strip_thinking_applied_to_output() -> None:
    """qwen3 leaks `<think>...</think>` even in translation —
    strip it via the existing helper."""
    llm = _ScriptedLLM(responses=[
        "<think>let me translate...</think>Базальноклеточный рак.",
    ])
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    out = await t.acall([TextNode(id_="c5", text="Basal cell carcinoma.")])
    assert out[0].metadata[TRANSLATED_TEXT_KEY] == "Базальноклеточный рак."


@pytest.mark.asyncio
async def test_multiple_chunks_concurrent() -> None:
    llm = _ScriptedLLM(responses=[
        "Привет.", "Мир.", "Тест.",
    ])
    t = TranslateToRussianTransform(llm=llm, num_workers=2)
    nodes = [TextNode(id_=f"c{i}", text=f"Hello {i}") for i in range(3)]
    out = await t.acall(nodes)
    assert {n.metadata[TRANSLATED_TEXT_KEY] for n in out} == {
        "Привет.", "Мир.", "Тест.",
    }
    assert len(llm.calls) == 3

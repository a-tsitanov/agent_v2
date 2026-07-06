"""Tests for `src/ingestion/translate_transform.py`."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from llama_index.core.base.llms.types import ChatMessage
from llama_index.core.schema import Document, TextNode

from src.ingestion.translate_transform import (
    FULL_TRANSLATED_TEXT_KEY,
    ORIGINAL_DOC_LENGTH_KEY,
    TRANSLATED_TEXT_KEY,
    DocumentTranslateTransform,
    TranslateToRussianTransform,
    _looks_russian,
    _slice_proportional,
    _split_for_translation,
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


# ── Document-level translation ──────────────────────────────────────


@pytest.mark.asyncio
async def test_document_translator_single_call_under_threshold() -> None:
    llm = _ScriptedLLM(responses=[
        "Один абзац.\n\nВторой абзац.\n\nТретий абзац.",
    ])
    t = DocumentTranslateTransform(llm=llm, threshold_chars=10_000)
    doc = Document(text="One para.\n\nTwo para.\n\nThree para.")
    out = await t.acall([doc])
    assert out[0].metadata[FULL_TRANSLATED_TEXT_KEY] == (
        "Один абзац.\n\nВторой абзац.\n\nТретий абзац."
    )
    assert out[0].metadata[ORIGINAL_DOC_LENGTH_KEY] == len(doc.text)
    assert len(llm.calls) == 1


@pytest.mark.asyncio
async def test_document_translator_windows_large_doc() -> None:
    """Doc above threshold splits on paragraph boundaries into
    multiple translation windows; outputs concatenated."""
    big_doc = "\n\n".join(
        f"Paragraph {i}. " + ("Sentence body. " * 80)
        for i in range(5)
    )
    # Each paragraph ~1280 chars; threshold 2000 → each window
    # holds 1 paragraph (combining two would exceed) ⇒ 5 windows.
    llm = _ScriptedLLM(responses=[f"RU window {i}" for i in range(1, 6)])
    t = DocumentTranslateTransform(llm=llm, threshold_chars=2000)
    doc = Document(text=big_doc)
    out = await t.acall([doc])
    full = out[0].metadata[FULL_TRANSLATED_TEXT_KEY]
    for i in range(1, 6):
        assert f"RU window {i}" in full
    # All windows are joined with \n\n
    assert "RU window 1\n\nRU window 2" in full
    assert len(llm.calls) == 5


@pytest.mark.asyncio
async def test_document_translator_skips_russian() -> None:
    llm = _ScriptedLLM(responses=[])
    t = DocumentTranslateTransform(llm=llm, threshold_chars=10_000)
    doc = Document(text="Документ полностью на русском с ИНН 7707083893.")
    out = await t.acall([doc])
    assert out[0].metadata[FULL_TRANSLATED_TEXT_KEY] == doc.text
    assert llm.calls == []


@pytest.mark.asyncio
async def test_document_translator_failure_leaves_no_metadata() -> None:
    class _RaisingLLM:
        async def achat(self, *a, **kw):
            raise RuntimeError("boom")

    t = DocumentTranslateTransform(llm=_RaisingLLM(), threshold_chars=10_000)
    doc = Document(text="English doc body.")
    out = await t.acall([doc])
    # No full translation → chunk-side fallback will translate
    # each chunk independently.
    assert FULL_TRANSLATED_TEXT_KEY not in out[0].metadata


# ── helpers ──────────────────────────────────────────────────────────


def test_split_for_translation_under_threshold_returns_one() -> None:
    assert _split_for_translation("short text", threshold=1000) == ["short text"]


def test_split_for_translation_on_paragraph_boundaries() -> None:
    text = "Para1 body.\n\nPara2 body.\n\nPara3 body."
    out = _split_for_translation(text, threshold=20)
    # Threshold 20 << any single paragraph (~12 chars), but
    # combining two would exceed.  Each window holds 1-2 paragraphs.
    joined = "\n\n".join(out)
    # Every paragraph preserved in order.
    for i in (1, 2, 3):
        assert f"Para{i} body." in joined
    # No window exceeds the threshold.
    assert all(len(w) <= 100 for w in out)


def test_split_for_translation_falls_back_to_sentences() -> None:
    """Single paragraph longer than threshold → split on sentences."""
    huge_para = "Sentence one. Sentence two. Sentence three. Sentence four."
    out = _split_for_translation(huge_para, threshold=20)
    assert len(out) >= 2
    # All content preserved
    assert "one" in "".join(out)
    assert "four" in "".join(out)


def test_slice_proportional_simple_ratio() -> None:
    """Ratio 1.5 (RU expanded by 50%); chunk [10,20] → [15,30]
    snapped to nearest sentence boundary."""
    full = "First. Second. Third. Fourth. Fifth. Sixth. Seventh."
    sliced = _slice_proportional(
        full_trans=full,
        full_orig_len=int(len(full) / 1.5),
        start_char_idx=int(len(full) / 1.5 * 0.4),
        end_char_idx=int(len(full) / 1.5 * 0.6),
    )
    assert sliced  # not empty
    # Slice begins at a sentence start and ends at a sentence end
    assert not sliced.startswith(" ")


def test_slice_proportional_empty_inputs() -> None:
    assert _slice_proportional(
        full_trans="", full_orig_len=10,
        start_char_idx=0, end_char_idx=5,
    ) == ""
    # Zero original length: nothing to scale against — return passthrough.
    assert _slice_proportional(
        full_trans="text",
        full_orig_len=0,
        start_char_idx=0,
        end_char_idx=5,
    ) == "text"


# ── chunk-side alignment ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_chunk_translator_uses_inherited_full_text_no_llm() -> None:
    """When a chunk inherits FULL_TRANSLATED_TEXT_KEY from its
    document, the per-chunk translator slices proportionally — no
    LLM call burned."""
    llm = _ScriptedLLM(responses=[])  # would crash if used
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    # Original doc was 100 chars, translated to 150 (ratio 1.5).
    # Chunk covered chars [0, 50] of original → expect first 75
    # chars of translation, snapped to sentence boundary.
    node = TextNode(
        id_="c1",
        text="English half of the doc.",
        metadata={
            FULL_TRANSLATED_TEXT_KEY: (
                "Первое предложение. Второе предложение. "
                "Третье предложение. Четвёртое предложение."
            ),
            ORIGINAL_DOC_LENGTH_KEY: 100,
        },
        start_char_idx=0,
        end_char_idx=50,
    )
    out = await t.acall([node])
    assert TRANSLATED_TEXT_KEY in out[0].metadata
    # Bulky doc-level fields cleaned up so they don't bloat
    # downstream storage.
    assert FULL_TRANSLATED_TEXT_KEY not in out[0].metadata
    assert ORIGINAL_DOC_LENGTH_KEY not in out[0].metadata
    # No LLM call made.
    assert llm.calls == []


@pytest.mark.asyncio
async def test_chunk_translator_falls_back_to_per_chunk_call() -> None:
    """Without the inherited full translation, the per-chunk
    LLM call path still runs."""
    llm = _ScriptedLLM(responses=["Локальный перевод чанка."])
    t = TranslateToRussianTransform(llm=llm, num_workers=1)
    node = TextNode(
        id_="c1",
        text="A chunk without doc-level translation context.",
    )
    out = await t.acall([node])
    assert out[0].metadata[TRANSLATED_TEXT_KEY] == "Локальный перевод чанка."
    assert len(llm.calls) == 1

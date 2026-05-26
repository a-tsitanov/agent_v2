"""Unit tests for the query planner (R2 plan-execute flow).

Stubs the LLM so the suite runs without a live model.  Verifies:
  * atomic question → single-element list (the question itself),
  * compound question → N≥2 sub-questions parsed from the LLM list,
  * robust parsing across numbered / bulleted / JSON shapes,
  * fail-safe: an LLM that raises → ``[question]`` (never crashes).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from src.retrieval.query_planner import _parse_subquestions, decompose

# ── stub LLM ───────────────────────────────────────────────────────


@dataclass
class _StubLLM:
    """Minimal async chat LLM returning a canned reply (or raising)."""

    reply: str = ""
    raises: bool = False
    calls: list = field(default_factory=list)

    async def achat(self, messages):
        self.calls.append(messages)
        if self.raises:
            raise RuntimeError("llm down")

        class _Msg:
            content = self.reply

        class _Resp:
            message = _Msg()

        return _Resp()


# ── parsing unit tests (no LLM) ────────────────────────────────────


def test_parse_numbered_list():
    out = _parse_subquestions(
        "1. Кто такой Иванов?\n2. С кем он связан?",
        original="Кто такой Иванов и с кем он связан?",
    )
    assert out == ["Кто такой Иванов?", "С кем он связан?"]


def test_parse_bulleted_list():
    out = _parse_subquestions(
        "- Вопрос A\n* Вопрос B\n• Вопрос C",
        original="orig",
    )
    assert out == ["Вопрос A", "Вопрос B", "Вопрос C"]


def test_parse_json_array():
    out = _parse_subquestions(
        '["Вопрос X", "Вопрос Y"]',
        original="orig",
    )
    assert out == ["Вопрос X", "Вопрос Y"]


def test_parse_single_line_falls_back_to_original():
    # One non-list line ⇒ treat as atomic, return original verbatim.
    out = _parse_subquestions("Just one question", original="orig question")
    assert out == ["orig question"]


def test_parse_empty_falls_back_to_original():
    out = _parse_subquestions("", original="orig question")
    assert out == ["orig question"]


# ── decompose (with stub LLM) ──────────────────────────────────────


@pytest.mark.asyncio
async def test_decompose_compound_returns_multiple():
    llm = _StubLLM(reply="1. Кто такой Иванов?\n2. Где он работает?")
    out = await decompose("Кто такой Иванов и где он работает?", llm)
    assert len(out) >= 2
    assert out == ["Кто такой Иванов?", "Где он работает?"]


@pytest.mark.asyncio
async def test_decompose_atomic_returns_single():
    llm = _StubLLM(reply="Кто такой Иванов?")
    out = await decompose("Кто такой Иванов?", llm)
    assert out == ["Кто такой Иванов?"]


@pytest.mark.asyncio
async def test_decompose_llm_failure_failsafe():
    llm = _StubLLM(raises=True)
    out = await decompose("любой вопрос", llm)
    assert out == ["любой вопрос"]


@pytest.mark.asyncio
async def test_decompose_caps_subquestions():
    # 7 returned, max_subqueries=3 → truncated to 3.
    reply = "\n".join(f"{i}. q{i}" for i in range(1, 8))
    llm = _StubLLM(reply=reply)
    out = await decompose("compound", llm, max_subqueries=3)
    assert len(out) == 3

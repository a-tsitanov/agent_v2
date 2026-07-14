"""NLI verdict over a pair of claim values (TDD). LLM injected; tolerant parse."""
from __future__ import annotations

import pytest

from src.analytics.claim_nli import (
    AGREE,
    CONTRADICT,
    NEUTRAL,
    build_nli_prompt,
    nli_verdict,
    parse_nli_verdict,
    refine_contradictions,
)
from src.analytics.contradictions import Claim, detect_contradictions


def test_prompt_includes_both_statements():
    p = build_nli_prompt("погибло 5 человек", "погибло 8 человек")
    assert "погибло 5 человек" in p and "погибло 8 человек" in p


def test_parse_contradict_en_and_ru_and_json():
    assert parse_nli_verdict("Ответ: contradiction") == CONTRADICT
    assert parse_nli_verdict("эти утверждения противоречат") == CONTRADICT
    assert parse_nli_verdict('{"verdict": "contradict"}') == CONTRADICT


def test_parse_agree_en_and_ru():
    assert parse_nli_verdict("agree — same meaning") == AGREE
    assert parse_nli_verdict("утверждения согласуются") == AGREE


def test_parse_garbage_is_neutral():
    assert parse_nli_verdict("не пойми что") == NEUTRAL
    assert parse_nli_verdict("") == NEUTRAL


@pytest.mark.asyncio
async def test_nli_verdict_calls_llm():
    async def llm(prompt: str) -> str:
        return "these statements contradict each other"

    assert await nli_verdict("погибло 5", "погибло 8", complete=llm) == CONTRADICT


@pytest.mark.asyncio
async def test_nli_verdict_fail_open_neutral():
    async def llm(prompt: str) -> str:
        raise RuntimeError("boom")

    assert await nli_verdict("a", "b", complete=llm) == NEUTRAL


def _value_conflict(v1, v2):
    return detect_contradictions([
        Claim(subject="удар", attribute="жертвы", value=v1, source="A"),
        Claim(subject="удар", attribute="жертвы", value=v2, source="B"),
    ])


@pytest.mark.asyncio
async def test_refine_keeps_confirmed_contradiction():
    async def llm(prompt: str) -> str:
        return "contradiction"

    out = await refine_contradictions(_value_conflict("5", "8"), complete=llm)
    assert len(out) == 1


@pytest.mark.asyncio
async def test_refine_drops_phrasing_only_agreement():
    async def llm(prompt: str) -> str:
        return "agree"  # 'дважды' vs 'два раза' — same meaning

    out = await refine_contradictions(_value_conflict("дважды", "два раза"), complete=llm)
    assert out == []


@pytest.mark.asyncio
async def test_refine_keeps_neutral_conservatively():
    async def llm(prompt: str) -> str:
        return "neutral"

    out = await refine_contradictions(_value_conflict("5", "8"), complete=llm)
    assert len(out) == 1


@pytest.mark.asyncio
async def test_refine_keeps_polarity_split_without_calling_llm():
    called = False

    async def llm(prompt: str) -> str:
        nonlocal called
        called = True
        return "agree"

    contradictions = detect_contradictions([
        Claim(subject="Иванов", attribute="участие", value="в сделке", source="A"),
        Claim(subject="Иванов", attribute="участие", value="в сделке",
              polarity="negated", source="B"),
    ])
    out = await refine_contradictions(contradictions, complete=llm)
    assert len(out) == 1
    assert called is False  # polarity split is definitive — no NLI needed

"""End-to-end contradiction pipeline (TDD): extract → cluster → structural →
NLI-refine. All LLM/embed I/O injected."""
from __future__ import annotations

import pytest

from src.analytics.contradiction_pipeline import detect_contradictions_e2e

_DOCS = [
    {"doc_id": "d1", "source": "A", "text": "погибло 5"},
    {"doc_id": "d2", "source": "B", "text": "погибло 8"},
]


async def _extract(prompt: str) -> str:
    if "погибло 5" in prompt:
        return '[{"subject":"удар","attribute":"жертвы","value":"5"}]'
    if "погибло 8" in prompt:
        return '[{"subject":"удар","attribute":"жертвы","value":"8"}]'
    return "[]"


async def _embed(texts: list[str]) -> list[list[float]]:
    # both claims share the slot "удар жертвы" → identical vector → one cluster
    return [[1.0, 0.0] for _ in texts]


@pytest.mark.asyncio
async def test_e2e_confirmed_contradiction():
    async def _nli(prompt: str) -> str:
        return "contradiction"

    out = await detect_contradictions_e2e(
        _DOCS, extract_complete=_extract, embed=_embed, nli_complete=_nli, threshold=0.9,
    )
    assert len(out) == 1
    assert {v.value for v in out[0].versions} == {"5", "8"}


@pytest.mark.asyncio
async def test_e2e_nli_drops_phrasing_only():
    async def _nli(prompt: str) -> str:
        return "agree"

    out = await detect_contradictions_e2e(
        _DOCS, extract_complete=_extract, embed=_embed, nli_complete=_nli, threshold=0.9,
    )
    assert out == []


@pytest.mark.asyncio
async def test_e2e_no_claims_no_contradictions():
    async def _empty(prompt: str) -> str:
        return "[]"

    async def _nli(prompt: str) -> str:
        return "contradiction"

    out = await detect_contradictions_e2e(
        _DOCS, extract_complete=_empty, embed=_embed, nli_complete=_nli,
    )
    assert out == []

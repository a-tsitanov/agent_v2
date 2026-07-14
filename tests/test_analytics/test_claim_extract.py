"""LLM claim extraction (TDD): text -> Claims. Tolerant parse, LLM injected."""
from __future__ import annotations

import pytest

from src.analytics.claim_extract import build_extract_prompt, extract_claims, parse_claims
from src.analytics.contradictions import ASSERTED, NEGATED, Claim


def test_prompt_includes_text():
    assert "ВС РФ нанесли удар по Киеву" in build_extract_prompt("ВС РФ нанесли удар по Киеву")


def test_parse_valid_json_array_attaches_doc_and_source():
    raw = '[{"subject":"удар","attribute":"цель","value":"Киев","polarity":"asserted"}]'
    assert parse_claims(raw, doc_id="d1", source="Росичъ") == [
        Claim(subject="удар", attribute="цель", value="Киев",
              polarity="asserted", doc_id="d1", source="Росичъ"),
    ]


def test_parse_extracts_json_from_surrounding_prose():
    raw = 'Вот факты:\n[{"subject":"a","attribute":"b","value":"c"}]\nготово'
    claims = parse_claims(raw, doc_id="d1", source="S")
    assert len(claims) == 1 and claims[0].value == "c"


def test_parse_garbage_returns_empty():
    assert parse_claims("совсем не json", doc_id="d", source="s") == []


def test_parse_skips_items_missing_required_fields():
    raw = ('[{"subject":"a","attribute":"b","value":"c"},'
           '{"subject":"x"},{"attribute":"y","value":"z"}]')
    claims = parse_claims(raw, doc_id="d", source="s")
    assert len(claims) == 1 and claims[0].subject == "a"


def test_parse_polarity_defaults_and_normalizes():
    raw = ('[{"subject":"a","attribute":"b","value":"c"},'
           '{"subject":"d","attribute":"e","value":"f","polarity":"negated"},'
           '{"subject":"g","attribute":"h","value":"i","polarity":"maybe"}]')
    claims = parse_claims(raw, doc_id="d", source="s")
    assert claims[0].polarity == ASSERTED          # missing -> asserted
    assert claims[1].polarity == NEGATED           # explicit negated
    assert claims[2].polarity == ASSERTED          # unknown -> asserted


@pytest.mark.asyncio
async def test_extract_calls_llm_and_parses():
    async def llm(prompt: str) -> str:
        return '[{"subject":"удар","attribute":"цель","value":"Киев"}]'

    claims = await extract_claims("текст", doc_id="d1", source="S", complete=llm)
    assert len(claims) == 1
    assert claims[0].value == "Киев" and claims[0].doc_id == "d1" and claims[0].source == "S"


@pytest.mark.asyncio
async def test_extract_fail_open_on_llm_error():
    async def llm(prompt: str) -> str:
        raise RuntimeError("boom")

    assert await extract_claims("текст", doc_id="d", source="s", complete=llm) == []

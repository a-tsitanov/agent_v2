"""LLM extraction of atomic claims from a document (offline, hybrid method B).

Mirrors ``analytics/planner.py``: plain ``achat`` + a tolerant JSON parse +
per-item validation (never raises). The LLM is injected as an async
``complete(prompt) -> str`` so this stays unit-testable; the offline workflow
binds it to ``build_llm``. ``doc_id``/``source`` come from the document, not the
model — the LLM only proposes ``subject/attribute/value/polarity``.
"""
from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable

from loguru import logger

from src.analytics.contradictions import ASSERTED, NEGATED, Claim

_INSTRUCTION = (
    "Извлеки из текста атомарные ПРОВЕРЯЕМЫЕ утверждения (факты и оценки) в виде "
    "JSON-массива объектов с полями: subject (о чём/о ком), attribute (какой признак/"
    "аспект), value (значение/содержание), polarity (\"asserted\" если утверждается, "
    "\"negated\" если отрицается). Только JSON-массив, без пояснений. "
    "Если утверждений нет — верни [].\n\nТекст:\n"
)


def build_extract_prompt(text: str) -> str:
    return f"{_INSTRUCTION}{text}"


def _one(item: dict, *, doc_id: str, source: str) -> Claim | None:
    subject = str(item.get("subject") or "").strip()
    attribute = str(item.get("attribute") or "").strip()
    value = str(item.get("value") or "").strip()
    if not (subject and attribute and value):
        return None
    polarity = NEGATED if str(item.get("polarity") or "").lower() == NEGATED else ASSERTED
    return Claim(
        subject=subject, attribute=attribute, value=value,
        polarity=polarity, doc_id=doc_id, source=source,
    )


def parse_claims(raw: str, *, doc_id: str, source: str) -> list[Claim]:
    """Pure, tolerant parse of an LLM claims array. Never raises."""
    try:
        try:
            arr = json.loads(raw)
        except json.JSONDecodeError:
            m = re.search(r"\[.*\]", raw or "", re.DOTALL)
            arr = json.loads(m.group(0)) if m else None
        if not isinstance(arr, list):
            return []
        out = []
        for item in arr:
            if isinstance(item, dict) and (c := _one(item, doc_id=doc_id, source=source)):
                out.append(c)
        return out
    except Exception as exc:  # defensive — parsing must never break the batch
        logger.warning("parse_claims failed: {e}", e=exc)
        return []


async def extract_claims(
    text: str, *, doc_id: str, source: str,
    complete: Callable[[str], Awaitable[str]],
) -> list[Claim]:
    """Extract claims from one document. Fail-open ([]) on any LLM error."""
    try:
        raw = await complete(build_extract_prompt(text))
    except Exception as exc:
        logger.warning("extract_claims LLM failed for doc={d}: {e}", d=doc_id, e=exc)
        return []
    return parse_claims(raw, doc_id=doc_id, source=source)

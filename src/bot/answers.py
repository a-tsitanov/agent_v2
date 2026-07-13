"""Shared answer helpers: detect an empty/no-result answer so neither the
search-fallback combinator nor the pipeline leaks the internal marker."""
from __future__ import annotations

NO_RESULT_MESSAGE = "По этому вопросу в базе знаний ничего не нашлось."

# LlamaIndex returns this literal string when synthesis has no nodes.
_EMPTY_MARKERS = frozenset({"empty response"})


def is_empty_answer(answer: str) -> bool:
    """True for a blank answer or a known empty-synthesis marker."""
    text = (answer or "").strip()
    return not text or text.lower() in _EMPTY_MARKERS

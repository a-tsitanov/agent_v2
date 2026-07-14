"""LLM NLI verdict over a pair of claim values (hybrid method B, iteration 4).

Embedding clustering (iter 3) groups claims about the same slot, but two
different value STRINGS may mean the same thing ('дважды' vs 'два раза') or
genuinely conflict ('дважды' vs 'один раз'). NLI resolves that: given two
statements, is it contradiction / agreement / neutral. The LLM is injected as an
async ``complete(prompt) -> str`` so this stays unit-testable; parsing is
tolerant (RU + EN + JSON) and fail-open to NEUTRAL.
"""
from __future__ import annotations

import re
from collections.abc import Awaitable, Callable

from loguru import logger

CONTRADICT = "contradict"
AGREE = "agree"
NEUTRAL = "neutral"

_CONTRADICT_RE = re.compile(r"contradict|противореч|не\s+соглас|конфликт", re.IGNORECASE)
_AGREE_RE = re.compile(r"\bagree\b|entail|same\s+meaning|соглас|совпад|то\s+же|эквивалент", re.IGNORECASE)

_INSTRUCTION = (
    "Даны два утверждения об одном и том же факте из разных источников. Определи "
    "их отношение: 'contradict' — если они противоречат друг другу (несовместимы), "
    "'agree' — если означают одно и то же (пусть разными словами), 'neutral' — если "
    "не связаны или нельзя судить. Ответь ОДНИМ словом.\n\n"
)


def build_nli_prompt(statement_a: str, statement_b: str) -> str:
    return f"{_INSTRUCTION}Утверждение 1: {statement_a}\nУтверждение 2: {statement_b}\nОтношение:"


def parse_nli_verdict(raw: str) -> str:
    """Tolerant parse → CONTRADICT / AGREE / NEUTRAL. contradiction wins over
    agreement if both somehow match (a conflict is the safer flag to keep)."""
    text = raw or ""
    if _CONTRADICT_RE.search(text):
        return CONTRADICT
    if _AGREE_RE.search(text):
        return AGREE
    return NEUTRAL


async def nli_verdict(
    statement_a: str, statement_b: str, *, complete: Callable[[str], Awaitable[str]],
) -> str:
    """Ask the LLM for the NLI relation. Fail-open to NEUTRAL on any error."""
    try:
        raw = await complete(build_nli_prompt(statement_a, statement_b))
    except Exception as exc:
        logger.warning("nli_verdict LLM failed: {e}", e=exc)
        return NEUTRAL
    return parse_nli_verdict(raw)


async def refine_contradictions(
    contradictions: list, *, complete: Callable[[str], Awaitable[str]],
) -> list:
    """Drop structurally-flagged contradictions that NLI judges to be mere
    phrasing differences (top-two values AGREE). A polarity split (asserted vs
    negated same value) is definitive — kept without an NLI call. NEUTRAL is
    kept conservatively (a real value difference we can't disprove)."""
    kept = []
    for c in contradictions:
        if not getattr(c, "polarity_split", ()) and len(getattr(c, "versions", ())) >= 2:
            verdict = await nli_verdict(
                c.versions[0].value, c.versions[1].value, complete=complete)
            if verdict == AGREE:
                continue  # same meaning, different words — not a contradiction
        kept.append(c)
    return kept

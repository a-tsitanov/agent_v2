"""Query decomposition for the plan-execute search flow (R2).

A compound question ("кто такой Иванов и где он работает?") is split
into atomic sub-questions, each answerable by one deterministic
retrieval pass.  The orchestrator runs one ``SubQueryRetrievalWorkflow``
per sub-question in parallel, then synthesizes once over the union.

The split is done by a small-tier LLM (role ``plan``).  Parsing is
deliberately tolerant — small local models emit numbered lists,
bulleted lists, or JSON arrays inconsistently — and EVERY failure path
falls back to ``[question]`` so a flaky planner can never break search:
worst case we just retrieve for the whole question once (legacy
behaviour).
"""

from __future__ import annotations

import json
import re

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms import LLM
from loguru import logger

# Default ceiling on sub-questions — bounds parallel fan-out (and LLM
# load) regardless of what the planner returns.  Callers pass the
# configured ``AgentSettings.max_subqueries``; this is the safety net.
_DEFAULT_MAX_SUBQUERIES = 5

_SYSTEM = (
    "You split a user's search question into the minimal set of "
    "independent sub-questions, each answerable on its own by a single "
    "retrieval over a knowledge base.\n"
    "Rules:\n"
    "- If the question is already atomic (asks ONE thing), return it "
    "unchanged as a single line.\n"
    "- Only split when the question genuinely asks about separate "
    "facts/entities (e.g. joined by 'и'/'and', or listing multiple "
    "things).  Do NOT over-split a single coherent question.\n"
    "- Preserve names, identifiers and numbers verbatim in their "
    "original language.\n"
    "- Output ONE sub-question per line, no numbering needed, no "
    "preamble, no commentary."
)

# Strips a leading "1." / "1)" / "-" / "*" / "•" list marker.
_MARKER_RE = re.compile(r"^\s*(?:\d+[.)]|[-*•])\s*")


def _strip_marker(line: str) -> str:
    return _MARKER_RE.sub("", line).strip()


def _parse_subquestions(text: str, *, original: str) -> list[str]:
    """Parse the planner's reply into a list of sub-questions.

    Tolerant of three shapes, in priority order:
      1. JSON array of strings,
      2. numbered / bulleted list (≥2 list items),
      3. multiple plain lines (≥2 non-empty).

    Anything that yields fewer than 2 usable sub-questions is treated
    as an atomic question → returns ``[original]`` verbatim (we trust
    the caller's exact wording over a single reformulated line).
    """
    raw = (text or "").strip()
    if not raw:
        return [original]

    # 1. JSON array — small models sometimes wrap the list.
    if raw.startswith("["):
        try:
            data = json.loads(raw)
            items = [str(x).strip() for x in data if str(x).strip()]
            if len(items) >= 2:
                return items
        except (json.JSONDecodeError, TypeError):
            pass  # fall through to line parsing

    lines = [s for s in (ln.strip() for ln in raw.splitlines()) if s]
    stripped = [s for s in (_strip_marker(ln) for ln in lines) if s]
    if len(stripped) >= 2:
        return stripped

    # Single line / single item ⇒ atomic.  Return the ORIGINAL question
    # verbatim — we don't want a reformulated single line to drift from
    # what the user actually asked.
    return [original]


async def decompose(
    question: str,
    llm: LLM,
    *,
    max_subqueries: int = _DEFAULT_MAX_SUBQUERIES,
) -> list[str]:
    """Split ``question`` into ≤``max_subqueries`` sub-questions.

    Returns ``[question]`` for atomic questions and on ANY failure
    (LLM error, empty/garbled output).  The result is always a
    non-empty list capped at ``max_subqueries``.
    """
    question = (question or "").strip()
    if not question:
        return [question]

    messages = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM),
        ChatMessage(role=MessageRole.USER, content=question),
    ]
    try:
        resp = await llm.achat(messages)
        raw = resp.message.content or ""
    except Exception as exc:
        # Fail-safe: never let a planner hiccup break search — fall
        # back to retrieving for the whole question once.
        logger.warning(
            "query_planner.decompose failed, using atomic fallback: {e}",
            e=exc,
        )
        return [question]

    subs = _parse_subquestions(raw, original=question)
    # Cap fan-out.  De-dup while preserving order so a planner echoing
    # the same sub-question twice doesn't spawn redundant children.
    seen: set[str] = set()
    deduped: list[str] = []
    for s in subs:
        if s in seen:
            continue
        seen.add(s)
        deduped.append(s)
    return deduped[:max_subqueries] or [question]

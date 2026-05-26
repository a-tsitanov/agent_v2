"""``route_query`` activity — classify a question's search mode (R7a).

Decision C (global search + routing): before search runs, a small-tier
``route`` model classifies the question into one of three modes:

  * ``"local"``  — a specific / factual question (who is X, where does Y
    work) best answered from concrete chunks via the R2–R5 plan-execute
    flow.
  * ``"global"`` — a corpus-level / thematic / aggregate question (main
    themes, overall trends, how-many-across-the-corpus) best answered by
    the GraphRAG global map-reduce over community summaries.
  * ``"drift"``  — a complex / mixed question that needs both: run local
    first, then expand with community context.

Fail-safe by construction: the classifier prompt is tolerant of the
sloppy single-word replies small local models emit, and EVERY failure
path (LLM error, empty / unparseable reply) falls back to ``"local"`` —
the cheapest, safest mode that always returns chunk-grounded answers.

The parse/classify mapping lives in the pure ``classify_route`` helper so
it is unit-testable without a live Temporal env or a real LLM (mirrors the
``query_planner._parse_subquestions`` convention).
"""

from __future__ import annotations

import time

from temporalio import activity

from src.workflow.contracts import RouteLabel, RouteParams, RouteResult

_SYSTEM = (
    "You are a query router for a knowledge-base search system. Classify "
    "the user's question into EXACTLY ONE mode and answer with that single "
    "word only — no punctuation, no explanation:\n"
    "- LOCAL: a specific, factual question about a particular entity, "
    "person, fact or relationship (e.g. 'who is X', 'where does Y work', "
    "'what is the address of Z').\n"
    "- GLOBAL: a corpus-level, thematic or aggregate question about the "
    "whole knowledge base (e.g. 'what are the main themes', 'summarise the "
    "overall trends', 'how many ... across all documents').\n"
    "- DRIFT: a complex or mixed question that needs both specific facts "
    "AND a broad overview (e.g. 'compare these companies and their role in "
    "the wider network').\n"
    "Reply with one word: LOCAL, GLOBAL or DRIFT."
)

_VALID: tuple[RouteLabel, ...] = ("local", "global", "drift")


def classify_route(text: str | None, *, query: str) -> RouteResult:
    """Map a router LLM reply to a ``RouteResult``.  Pure / unit-testable.

    Tolerant of the wrapping prose / punctuation small local models add
    around the label.  Recognises the FIRST of the three known labels to
    appear in the reply.  Anything unrecognised (empty / garbled / None)
    → ``route="local"`` — the safe default that never breaks search.
    """
    raw = (text or "").strip().lower()
    if not raw:
        return RouteResult(route="local", reason="empty router reply")

    # Find which known label appears first in the reply so a wrapped
    # answer ("Route: GLOBAL.") still classifies correctly.
    best: tuple[int, RouteLabel] | None = None
    for label in _VALID:
        idx = raw.find(label)
        if idx != -1 and (best is None or idx < best[0]):
            best = (idx, label)

    if best is None:
        return RouteResult(route="local", reason=f"unparseable: {raw[:40]}")
    return RouteResult(route=best[1], reason=raw[:120])


def _get_route_llm():
    """Small-tier router LLM (role ``route`` → small per ``_DEFAULT_ROLE_TIERS``).

    Indirected through a module-level fn so tests can monkeypatch it
    without touching the LiteLLM factory."""
    from src.retrieval.llm import build_llm

    return build_llm("route")


@activity.defn
async def route_query(params: RouteParams) -> RouteResult:
    """Classify the question into local / global / drift (small tier).

    Fail-safe: ANY error → ``route="local"`` (never raises through the
    Temporal boundary so a flaky router can't break search)."""
    from llama_index.core.base.llms.types import ChatMessage, MessageRole

    t0 = time.monotonic()
    activity.heartbeat({"stage": "route", "query": params.query[:80]})
    try:
        llm = _get_route_llm()
        resp = await llm.achat([
            ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=params.query),
        ])
        raw = resp.message.content or ""
    except Exception as exc:
        activity.logger.warning("route_query  llm err=%s — default local", exc)
        return RouteResult(route="local", reason=f"llm error: {exc}")

    result = classify_route(raw, query=params.query)
    activity.logger.info(
        "route_query  route=%s  ms=%d",
        result.route, int((time.monotonic() - t0) * 1000),
    )
    return result


__all__ = ["classify_route", "route_query"]

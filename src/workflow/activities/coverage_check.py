"""``coverage_check`` activity — pre-synthesis completeness gate.

After the orchestrator merges all sub-question sources, it asks this
activity whether the evidence gathered so far actually covers the
*whole* question — including every part of a multi-part query.  If not,
the orchestrator issues the named gap as one extra
SubQueryRetrievalWorkflow, re-merges, then synthesizes.

Bounded by ``max_coverage_rounds`` in the orchestrator so it can't loop
forever.

Fail-open: any error or unparseable output → ``complete=True`` so a
flaky check never blocks the answer.
"""

from __future__ import annotations

import time

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from temporalio import activity

from src.workflow._search_deps import get_search_llm
from src.workflow.contracts import CoverageParams, CoverageResult

_SYSTEM = (
    "You are a completeness checker for a research agent.  Given the "
    "user QUESTION and the EVIDENCE gathered so far, decide whether the "
    "evidence is enough to fully answer the question — including every "
    "distinct part of a multi-part question.\n"
    "Be strict about parts the question explicitly asks for, but do NOT "
    "demand information the question doesn't request, and do NOT require "
    "perfection — partial corroboration of a sub-claim counts.\n"
    "Output format:\n"
    "  First line exactly 'COMPLETE: yes' or 'COMPLETE: no'.\n"
    "  If 'no', a second line 'MISSING: <one concise phrase naming what "
    "still needs to be retrieved>'.\n"
    "If the evidence is empty, answer 'COMPLETE: no' and name what to "
    "look up first."
)


def _parse(text: str) -> CoverageResult:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    complete = True
    missing = ""
    for ln in lines:
        up = ln.upper()
        if up.startswith("COMPLETE:"):
            verdict = ln.split(":", 1)[1].strip().lower()
            complete = not verdict.startswith("no")
        elif up.startswith("MISSING:"):
            missing = ln.split(":", 1)[1].strip()
    # Incomplete but no gap named ⇒ nothing actionable, treat as done.
    if not complete and not missing:
        complete = True
    return CoverageResult(complete=complete, missing=missing)


@activity.defn
async def coverage_check(params: CoverageParams) -> CoverageResult:
    """Judge whether gathered evidence fully covers the query."""
    t0 = time.monotonic()
    activity.heartbeat({"stage": "init", "ev_len": len(params.evidence)})
    llm = await get_search_llm()

    user = (
        f"QUESTION:\n{params.query}\n\n"
        f"EVIDENCE:\n{params.evidence or '(none gathered yet)'}"
    )
    messages = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM),
        ChatMessage(role=MessageRole.USER, content=user),
    ]
    try:
        resp = await llm.achat(messages)
        result = _parse(resp.message.content or "")
    except Exception as exc:
        # Fail-open: never block the answer on a flaky completeness call.
        activity.logger.warning(
            "coverage_check  err=%s — treating as complete", exc,
        )
        return CoverageResult(complete=True, missing="")

    activity.logger.info(
        "coverage_check  complete=%s  missing=%r  ms=%d",
        result.complete, result.missing[:80],
        int((time.monotonic() - t0) * 1000),
    )
    return result

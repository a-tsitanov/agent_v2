"""``distill_observation`` activity — compress one tool result.

Sits between ``tool_execution`` and the next ``agent_reasoning_step``.
Given the user query and a (large) raw tool observation, one LLM call
returns:

* ``distilled`` — only the query-relevant facts, compact, so the
  agent's reasoning history stops growing unbounded on big corpora.
* ``relevance`` — relevant / partial / irrelevant.  Advisory only:
  recorded in step stats and reflected in the agent's history note so
  it knows a path was a dead end.  It does NOT drop sources from the
  accumulator.

The full ``NodeWithScore`` sources are kept separately in the
workflow accumulator (always, regardless of relevance), so
distillation never costs the final answer detail — it only trims the
agent's working memory.

Output is parsed from a delimited text format (not strict JSON) — a
small local model holds a ``RELEVANCE: ...`` first line + bullets far
more reliably than a JSON schema.  Parsing degrades gracefully:
unparseable output is treated as ``partial`` with the raw text kept.
"""

from __future__ import annotations

import time

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from temporalio import activity

from src.workflow._search_deps import get_search_llm
from src.workflow.contracts import DistillParams, DistillResult, Relevance

# Defensive cap on what we feed the distiller itself, so a pathological
# observation can't overflow the distiller's own context window.
_MAX_INPUT_CHARS = 30000

_SYSTEM = (
    "You compress one tool result for a research agent.  You are given "
    "the user QUERY and the raw TOOL OUTPUT.  Extract ONLY the facts "
    "from the tool output that help answer the query.\n"
    "Rules:\n"
    "- Be concise: short bullet lines, no preamble, no repetition.\n"
    "- Preserve names, identifiers, numbers verbatim in their original "
    "language.\n"
    "- Do NOT invent anything not present in the tool output.\n"
    "Output format — the FIRST line must be exactly one of:\n"
    "  RELEVANCE: relevant\n"
    "  RELEVANCE: partial\n"
    "  RELEVANCE: irrelevant\n"
    "judging how useful this tool output is for the query.  Then the "
    "relevant facts as bullet lines.  If nothing is relevant, output "
    "just 'RELEVANCE: irrelevant' and nothing else."
)

_VALID: set[str] = {"relevant", "partial", "irrelevant"}


def _parse(text: str) -> tuple[str, Relevance]:
    """Split the model output into (distilled facts, relevance verdict).

    Tolerant: a missing/garbled RELEVANCE line falls back to 'partial'
    and keeps the whole text as the distilled body.
    """
    lines = (text or "").strip().splitlines()
    relevance: Relevance = "partial"
    body_start = 0
    for i, ln in enumerate(lines):
        stripped = ln.strip()
        if not stripped:
            continue
        if stripped.upper().startswith("RELEVANCE:"):
            verdict = stripped.split(":", 1)[1].strip().lower()
            if verdict in _VALID:
                relevance = verdict  # type: ignore[assignment]
            body_start = i + 1
        break
    body = "\n".join(lines[body_start:]).strip()
    return body, relevance


@activity.defn
async def distill_observation(params: DistillParams) -> DistillResult:
    """Compress + relevance-grade one tool observation."""
    t0 = time.monotonic()
    activity.heartbeat({"stage": "init", "tool": params.tool_name,
                        "obs_len": len(params.observation)})
    llm = await get_search_llm()

    obs = params.observation[:_MAX_INPUT_CHARS]
    user = (
        f"QUERY:\n{params.query}\n\n"
        f"TOOL ({params.tool_name}) OUTPUT:\n{obs}"
    )
    messages = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM),
        ChatMessage(role=MessageRole.USER, content=user),
    ]
    try:
        resp = await llm.achat(messages)
        raw = resp.message.content or ""
    except Exception as exc:  # noqa: BLE001
        # On distiller failure, fall back to the raw observation so the
        # agent still sees *something* — the loop's hard char cap will
        # truncate it.  Never fail the whole search over compaction.
        activity.logger.warning(
            "distill_observation  tool=%s  err=%s — passing raw through",
            params.tool_name, exc,
        )
        return DistillResult(distilled=params.observation, relevance="partial")

    distilled, relevance = _parse(raw)
    if relevance == "irrelevant":
        # Clear, short marker beats a stray bullet/empty body — tells the
        # agent this path was a dead end so it tries a different query.
        distilled = "(no information relevant to the query)"
    elif len(distilled.strip(" *-•\t")) < 3:
        # Model gave a verdict but effectively no facts — keep the raw so
        # we don't silently drop content the agent might need.
        distilled = params.observation
    activity.logger.info(
        "distill_observation  tool=%s  %d→%d chars  rel=%s  ms=%d",
        params.tool_name, len(params.observation), len(distilled),
        relevance, int((time.monotonic() - t0) * 1000),
    )
    return DistillResult(distilled=distilled, relevance=relevance)

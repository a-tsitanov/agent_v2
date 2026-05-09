"""LLM-judge for ``agentic_search``.

The judge looks at the original query and the accumulated retrieved
context across rounds and decides whether the agent has enough to
answer.  The prompt and JSON contract mirror enterprise-kb's
``_JUDGE_PROMPT`` so the two builds can be benchmarked apples-to-apples.

Defensive parsing: ANY error (LLM exception, malformed JSON,
markdown-fenced JSON, missing fields) collapses to
``sufficient=True`` so the caller exits the loop cleanly instead of
crashing the whole request.  The exception text lands in ``reason``
for diagnostics.
"""

from __future__ import annotations

import json
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms import LLM
from llama_index.core.schema import NodeWithScore
from loguru import logger

_JUDGE_PROMPT = (
    "You are a search quality judge. Given the original query and the "
    "retrieved text chunks, decide whether there is enough information "
    "to fully answer the query.\n\n"
    "Return ONLY valid JSON (no markdown fences):\n"
    '{"sufficient": true/false, "follow_up_query": "...", "reason": "..."}\n\n'
    "Rules:\n"
    "- If sufficient, set follow_up_query to an empty string.\n"
    "- If not sufficient, write a concise follow-up search query that "
    "would retrieve the missing information.\n"
    "- The follow-up query must be different from previous queries.\n"
)


def _build_chunks_str(
    sources: list[NodeWithScore], max_chars: int = 4000,
) -> str:
    parts: list[str] = []
    total = 0
    for src in sources:
        text = src.node.get_content()
        if total + len(text) > max_chars:
            remaining = max_chars - total
            if remaining > 100:
                parts.append(text[:remaining])
            break
        parts.append(text)
        total += len(text)
    return "\n---\n".join(parts)


class LLMJudge:
    """Callable wrapper around the project LLM."""

    def __init__(self, llm: LLM) -> None:
        self._llm = llm

    async def __call__(
        self,
        original_query: str,
        accumulated_sources: list[NodeWithScore],
    ) -> dict[str, Any]:
        """Returns ``{"sufficient": bool, "follow_up_query": str,
        "reason": str}`` — never raises."""
        chunks_ctx = _build_chunks_str(accumulated_sources)
        user = (
            f"Original query: {original_query}\n\n"
            f"Retrieved context (chunks: {len(accumulated_sources)}):\n"
            f"{chunks_ctx}"
        )
        try:
            resp = await self._llm.achat(
                messages=[
                    ChatMessage(role=MessageRole.SYSTEM, content=_JUDGE_PROMPT),
                    ChatMessage(role=MessageRole.USER, content=user),
                ]
            )
            raw = (resp.message.content or "").strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            parsed = json.loads(raw)
            return {
                "sufficient": bool(parsed.get("sufficient", True)),
                "follow_up_query": str(parsed.get("follow_up_query", "")),
                "reason": str(parsed.get("reason", "")),
            }
        except Exception as exc:  # noqa: BLE001 — defensive on purpose
            logger.warning(
                "agentic judge failed, stopping loop: {err}", err=exc,
            )
            return {
                "sufficient": True,
                "follow_up_query": "",
                "reason": str(exc),
            }

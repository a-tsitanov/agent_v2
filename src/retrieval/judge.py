"""LLM-judge for legacy `agentic_search`.

The judge evaluates whether the accumulated retrieved context is
enough to answer the original query.  Two execution paths:

* ``LLMJudge.via_structured`` — preferred.  Uses
  ``llm.astructured_predict(JudgeOutput, prompt)`` (function calling
  on qwen3:8b+).  Returns a ``JudgeOutput`` Pydantic.
* ``LLMJudge.__call__`` (legacy) — text-based path.  Asks the LLM
  to emit JSON in chat completion, strips markdown fences, parses.
  Defensive: any error → ``sufficient=True`` so the caller exits
  cleanly.  Kept for compat with smaller models and as fallback
  when ``LITELLM_FUNCTION_CALLING=false``.

Both paths return the same dict shape (the legacy loop in
``agentic_search`` doesn't care which path was taken).

This module is on the R10 chopping block — when the ReAct +
reflective agents (R7/R8) prove themselves on golden eval, the
judge loop becomes optional baseline and these helpers can move to
a `legacy/` namespace or be deleted.
"""

from __future__ import annotations

import json
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms import LLM
from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import NodeWithScore
from loguru import logger

from src.models.search import JudgeOutput

_JUDGE_PROMPT_TEXT = (
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

_STRUCTURED_PROMPT = PromptTemplate(
    "You are a search quality judge. Given the original query and the "
    "retrieved text chunks, decide whether there is enough information "
    "to fully answer the query.\n\n"
    "Original query: {query}\n\n"
    "Retrieved context (chunks: {chunk_count}):\n{chunks}\n\n"
    "Rules:\n"
    "- If sufficient, set follow_up_query to an empty string.\n"
    "- If not sufficient, write a concise follow-up search query that "
    "would retrieve the missing information.\n"
    "- The follow-up query must be different from previous queries."
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
    """Two-path LLM judge wrapper."""

    def __init__(self, llm: LLM) -> None:
        self._llm = llm

    async def via_structured(
        self,
        original_query: str,
        accumulated_sources: list[NodeWithScore],
    ) -> dict[str, Any]:
        """Function-calling path (qwen3+/gpt-4-class).

        On any failure falls back to the text-based path to preserve
        the "judge never raises" contract.
        """
        chunks_ctx = _build_chunks_str(accumulated_sources)
        try:
            output: JudgeOutput = await self._llm.astructured_predict(
                JudgeOutput,
                _STRUCTURED_PROMPT,
                query=original_query,
                chunk_count=len(accumulated_sources),
                chunks=chunks_ctx,
            )
            return {
                "sufficient": bool(output.sufficient),
                "follow_up_query": str(output.follow_up_query or ""),
                "reason": str(output.reason or ""),
            }
        except Exception as exc:  # noqa: BLE001 — fall back
            logger.warning(
                "structured judge failed → text fallback: {err}", err=exc,
            )
            return await self.__call__(original_query, accumulated_sources)

    async def __call__(
        self,
        original_query: str,
        accumulated_sources: list[NodeWithScore],
    ) -> dict[str, Any]:
        """Text-based path (works with any chat LLM).

        Returns ``{"sufficient": bool, "follow_up_query": str,
        "reason": str}`` — never raises.
        """
        chunks_ctx = _build_chunks_str(accumulated_sources)
        user = (
            f"Original query: {original_query}\n\n"
            f"Retrieved context (chunks: {len(accumulated_sources)}):\n"
            f"{chunks_ctx}"
        )
        try:
            resp = await self._llm.achat(
                messages=[
                    ChatMessage(role=MessageRole.SYSTEM, content=_JUDGE_PROMPT_TEXT),
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

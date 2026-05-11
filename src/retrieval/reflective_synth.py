"""Self-RAG-inspired reflective synthesizer.

Drafts an answer with inline self-reflection markers, parses them
out, retrieves more context for any ``[NEED:topic]`` gaps, and
redrafts.  Loop terminates when the draft has no more ``[NEED]``
markers OR ``max_refinements`` is exhausted.

The marker scheme is **prompt-based** — no fine-tuning, no special
tokens.  qwen3:8b (the project default) follows it reliably on the
project corpus; smaller models may misuse it (see
`docs/MODELS.md` for escalation path).

Marker vocabulary (must appear verbatim in the model output):

- ``[NEED:what's missing]`` — trigger a follow-up retrieve.
- ``[SUPPORTED:chunk_id]`` — claim immediately precedes / contains
  this marker is grounded by the listed chunk.
- ``[UNCERTAIN:reason]`` — the model intentionally didn't claim
  something it couldn't ground.  Kept in the final answer as honest
  signal to the caller (vs. hallucinating).

The final answer (visible to the user) has all ``[NEED:...]``
markers stripped; ``[SUPPORTED:...]`` markers can either be stripped
or kept in compact ``[c1]``-style depending on
``strip_support_markers`` (default: stripped, citations exposed in
``ReflectiveAnswerDetail`` instead).
"""

from __future__ import annotations

import re
import time
from typing import Awaitable, Callable, Protocol

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms import LLM
from llama_index.core.schema import NodeWithScore
from loguru import logger
from pydantic import BaseModel, Field

from src.models.search import ReflectiveCitation, ReflectiveUncertainty
from src.observability.trace import record_event, record_timed
from src.retrieval._common import deduplicate_nodes, strip_thinking


# ── protocols ────────────────────────────────────────────────────────


class RetrieverProtocol(Protocol):
    async def aretrieve(self, query: str) -> list[NodeWithScore]: ...


# ── output shape ─────────────────────────────────────────────────────


class ReflectiveAnswer(BaseModel):
    """Final output of `reflective_synthesize`.

    Exposes BOTH the human-readable answer text (markers stripped)
    and the structured breakdown (citations / uncertainties) for
    downstream consumers (UI, eval).
    """

    text: str
    citations: list[ReflectiveCitation] = Field(default_factory=list)
    uncertainties: list[ReflectiveUncertainty] = Field(default_factory=list)
    refinement_rounds: int = 0

    # Compatibility shim for `agentic_react_search` which expects a
    # synthesize() return that has a `.response` attribute (mirroring
    # LlamaIndex `ResponseSynthesizer.asynthesize`).
    @property
    def response(self) -> str:
        return self.text


# ── prompts ──────────────────────────────────────────────────────────


_SYSTEM_PROMPT = """\
You are a careful research-assistant LLM writing an answer over a
mixed corpus of analytical reports, emails, and support-call
transcripts.

WRITE YOUR ANSWER IN THE USER'S LANGUAGE.  Inline-annotate your
draft with these three markers — verbatim:

- [NEED:what's missing]   — write this when, mid-sentence, you
  realise you'd need more information to support a claim.  Be
  specific: [NEED:dates of Q1 2024 onboarding redesign], NOT
  [NEED:more info].
- [SUPPORTED:chunk_id]    — when a claim is grounded by a specific
  chunk you've seen in the context, append this marker right after
  the claim.  `chunk_id` is the literal id from the context items.
- [UNCERTAIN:why]         — when you can't support a piece of the
  answer from context but the question explicitly asked about it,
  write [UNCERTAIN:why] instead of guessing.  DO NOT hallucinate.

Rules:
- Do NOT answer from prior knowledge — only from the context items.
- Names, organizations, identifiers — preserve verbatim from source
  language.
- Keep the draft tight — answer the question, don't pad.
"""


_REFINE_PROMPT = """\
Your previous draft below was missing information for these gaps:
{gaps_summary}

Additional context was retrieved.  Update the draft so that any
[NEED:...] markers are resolved — either replaced with a grounded
claim + [SUPPORTED:chunk_id] marker, or with [UNCERTAIN:reason]
if the additional context still doesn't cover the gap.

Previous draft:
\"\"\"
{previous_draft}
\"\"\"
"""


# ── parsers ──────────────────────────────────────────────────────────


_NEED_RE = re.compile(r"\[NEED:([^\]]+)\]", re.IGNORECASE)
_SUPPORTED_RE = re.compile(r"\[SUPPORTED:([^\]]+)\]", re.IGNORECASE)
_UNCERTAIN_RE = re.compile(r"\[UNCERTAIN:([^\]]+)\]", re.IGNORECASE)


def parse_markers(draft: str) -> tuple[list[str], list[str], list[str]]:
    """Return (needs, supports, uncertains) — each a list of marker
    bodies in source order.
    """
    needs = [m.group(1).strip() for m in _NEED_RE.finditer(draft)]
    supports = [m.group(1).strip() for m in _SUPPORTED_RE.finditer(draft)]
    uncertains = [m.group(1).strip() for m in _UNCERTAIN_RE.finditer(draft)]
    return needs, supports, uncertains


def strip_markers(draft: str, *, keep_uncertain: bool = True) -> str:
    """Remove the marker syntax from `draft`.

    Default: strip [NEED] and [SUPPORTED] entirely; keep [UNCERTAIN]
    visible since it's user-relevant honesty.
    """
    out = _NEED_RE.sub("", draft)
    out = _SUPPORTED_RE.sub("", out)
    if not keep_uncertain:
        out = _UNCERTAIN_RE.sub("", out)
    # collapse 2+ blank lines / runs of spaces created by stripping
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


# ── context formatting ──────────────────────────────────────────────


def _format_context(nodes: list[NodeWithScore]) -> str:
    """Render context as numbered chunks with their ids — model
    needs to see the id to be able to cite it via [SUPPORTED:id].
    """
    if not nodes:
        return "(no context retrieved)"
    parts = []
    for n in nodes:
        cid = n.node.node_id
        content = n.node.get_content().strip().replace("\n", " ")
        if len(content) > 800:
            content = content[:800] + "…"
        parts.append(f"[chunk_id={cid}] {content}")
    return "\n\n".join(parts)


# ── main entry ──────────────────────────────────────────────────────


SynthesizeFnReflective = Callable[
    [str, list[NodeWithScore]], Awaitable[ReflectiveAnswer]
]


async def reflective_synthesize(
    *,
    llm: LLM,
    query: str,
    context_nodes: list[NodeWithScore],
    retriever: RetrieverProtocol | None = None,
    max_refinements: int = 3,
) -> ReflectiveAnswer:
    """Self-RAG-inspired synthesis.

    Behaviour:
      1. Draft with reflection markers.
      2. Parse `[NEED:...]`.  If empty (or no retriever provided, or
         budget exhausted), finalize.
      3. Retrieve for each NEED, add to context (dedup by chunk_id).
      4. Redraft.  Repeat.

    Even if the first draft has zero markers we still return a
    `ReflectiveAnswer` so the caller has a uniform shape.
    """
    t0 = time.monotonic()
    accumulated = list(context_nodes)
    draft = ""
    round_i = 0

    for round_i in range(max_refinements + 1):
        if round_i == 0:
            user_msg = (
                f"Question: {query}\n\n"
                f"Context items:\n{_format_context(accumulated)}"
            )
        else:
            needs, _, _ = parse_markers(draft)
            gaps_summary = "; ".join(needs) or "(parser found none)"
            user_msg = _REFINE_PROMPT.format(
                gaps_summary=gaps_summary, previous_draft=draft,
            ) + (
                f"\n\nFull current context items:\n"
                f"{_format_context(accumulated)}"
            )

        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM_PROMPT),
            ChatMessage(role=MessageRole.USER, content=user_msg),
        ]

        try:
            with record_timed(
                "llm_call", round=round_i, kind="reflective_draft",
            ):
                response = await llm.achat(messages)
        except Exception as exc:  # noqa: BLE001
            logger.warning("reflective draft failed: {err}", err=exc)
            break

        draft = strip_thinking(response.message.content or "").strip()
        needs, _, _ = parse_markers(draft)
        record_event(
            "refinement_round",
            payload={
                "round": round_i,
                "needs": len(needs),
                "n_context_nodes": len(accumulated),
            },
        )

        if not needs:
            break
        if round_i >= max_refinements:
            break
        if retriever is None:
            logger.info(
                "reflective synth: {n} NEED markers but no retriever — "
                "leaving as UNCERTAIN", n=len(needs),
            )
            break

        for need in needs[:5]:  # don't blow up budget on a draft full of NEEDs
            try:
                with record_timed(
                    "tool_call", tool_name="retrieve_for_need", query=need,
                ):
                    extra = await retriever.aretrieve(need)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "reflective retrieve for need={n} failed: {err}",
                    n=need, err=exc,
                )
                continue
            accumulated.extend(extra)
        accumulated = deduplicate_nodes(accumulated)

    needs, supports, uncertains = parse_markers(draft)
    final_text = strip_markers(draft, keep_uncertain=True)

    citations = _build_citations(supports, accumulated)
    uncertainties = [
        ReflectiveUncertainty(topic="", reason=u) for u in uncertains
    ]

    latency_ms = (time.monotonic() - t0) * 1000.0
    logger.info(
        "reflective synth done  rounds={r}  needs_remaining={n}  "
        "supports={s}  uncertain={u}  latency_ms={ms:.1f}",
        r=round_i, n=len(needs), s=len(supports),
        u=len(uncertains), ms=latency_ms,
    )

    return ReflectiveAnswer(
        text=final_text,
        citations=citations,
        uncertainties=uncertainties,
        refinement_rounds=round_i,
    )


def _build_citations(
    support_markers: list[str], nodes: list[NodeWithScore],
) -> list[ReflectiveCitation]:
    """Map [SUPPORTED:id] markers to nodes the synthesizer actually
    saw.  Drops markers whose id doesn't appear in the context (the
    model hallucinated an id) — eval treats those as un-cited claims.
    """
    valid_ids = {n.node.node_id for n in nodes}
    out: list[ReflectiveCitation] = []
    for m in support_markers:
        chunk_id = m.strip()
        if chunk_id in valid_ids:
            out.append(ReflectiveCitation(claim="", chunk_id=chunk_id))
    return out


__all__ = [
    "ReflectiveAnswer",
    "parse_markers",
    "strip_markers",
    "reflective_synthesize",
]

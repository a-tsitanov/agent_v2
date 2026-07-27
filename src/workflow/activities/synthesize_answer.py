"""``synthesize_answer`` activity — final answer composition.

Plain ResponseSynthesizer compaction over the accumulated context.
Prepends the Russian-output instruction we use in route handlers so the
answer language matches the graph normalisation regardless of source-
language chunks.

(The Self-RAG reflective branch was removed with the R7b cutover — no
search path sets ``mode="selfrag"`` anymore.)
"""

from __future__ import annotations

from temporalio import activity

from src.retrieval.answer_template import build_query
from src.workflow._search_deps import get_synthesis_synthesizer, get_synthesizer
from src.workflow._search_serde import serialized_to_node
from src.workflow.contracts import SerializedNode, SynthesizeParams, SynthesizeResult
from src.workflow.heartbeat import heartbeat_every

# Pulse interval inside the synthesis call.  Compact-and-refine issues one LLM
# request per refinement, so a single ``asynthesize`` await spans minutes and
# used to pass with NO heartbeat at all — a wedged attempt was indistinguishable
# from a slow one and burned the full 1h ``LLM_START_TO_CLOSE`` before Temporal
# reclaimed it, blocking the whole search chain behind it.
_HEARTBEAT_INTERVAL_S = 30.0


def with_group_prefix(sn: SerializedNode) -> SerializedNode:
    """Prepend the channel group so the synthesis LLM sees each source's
    type/trust. Identity when the source has no group."""
    g = (sn.metadata or {}).get("doc_group")
    if not g:
        return sn
    return sn.model_copy(update={"text": f"[{g}] {sn.text}"})


@activity.defn
async def synthesize_answer(params: SynthesizeParams) -> SynthesizeResult:
    """Compose the final answer over accumulated context."""
    activity.heartbeat({"stage": "init", "mode": params.mode,
                        "n_sources": len(params.accumulated)})
    nodes = [serialized_to_node(with_group_prefix(n)) for n in params.accumulated]
    # answer_template (named or inline) shapes the answer; empty → the
    # default Russian-output preamble (unchanged behaviour).
    query = build_query(params.query, params.answer_template)

    # R2 plan-execute flow opts into the large synthesis tier
    # (build_synthesis_llm); other paths keep the small search-tier
    # synthesizer (use_synthesis_llm=False).
    synthesizer = (
        await get_synthesis_synthesizer()
        if params.use_synthesis_llm
        else await get_synthesizer()
    )
    activity.heartbeat({"stage": "plain_synth"})
    async with heartbeat_every(_HEARTBEAT_INTERVAL_S, {"stage": "plain_synth"}):
        response = await synthesizer.asynthesize(query=query, nodes=nodes)
    text = getattr(response, "response", None) or str(response)
    activity.logger.info(
        "synthesize_answer (%s)  text_len=%d", params.mode, len(text or ""),
    )
    return SynthesizeResult(text=text or "")

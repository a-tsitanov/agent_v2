"""``synthesize_answer`` activity — final answer composition.

Modes:
* ``simple`` / ``agent`` — plain ResponseSynthesizer compaction over
  accumulated context.  Prepends the Russian-output instruction we
  use in route handlers so the answer language matches the graph
  normalisation regardless of source-language chunks.
* ``selfrag``            — Self-RAG-inspired reflective loop with
  inline ``[NEED:...]`` markers driving additional retrieval rounds.
"""

from __future__ import annotations

from temporalio import activity

from src.retrieval.reflective_synth import reflective_synthesize
from src.workflow._search_deps import (
    get_retriever, get_search_llm, get_synthesizer,
)
from src.workflow._search_serde import serialized_to_node
from src.workflow.contracts import (
    ReflectiveCitationDict, ReflectiveUncertaintyDict,
    SynthesizeParams, SynthesizeResult,
)


def _ru_query(query: str) -> str:
    """Russian-output instruction — same prefix used in the legacy
    `/search` handler.  Without it LlamaIndex's synthesizer can flip
    to English when most context chunks are English."""
    return (
        "Ответь на следующий вопрос на русском языке, "
        "сохраняя имена собственные и идентификаторы дословно "
        f"из исходного языка контекста: {query}"
    )


@activity.defn
async def synthesize_answer(params: SynthesizeParams) -> SynthesizeResult:
    """Compose the final answer over accumulated context."""
    activity.heartbeat({"stage": "init", "mode": params.mode,
                        "n_sources": len(params.accumulated)})
    nodes = [serialized_to_node(n) for n in params.accumulated]
    query = _ru_query(params.query)

    if params.mode == "selfrag":
        llm = await get_search_llm()
        retriever = await get_retriever()
        activity.heartbeat({"stage": "reflective_synth"})
        answer = await reflective_synthesize(
            llm=llm, query=query,
            context_nodes=nodes,
            retriever=retriever,
            max_refinements=params.max_refinements,
        )
        activity.logger.info(
            "synthesize_answer (reflective)  rounds=%d  citations=%d  "
            "uncertainties=%d",
            answer.refinement_rounds, len(answer.citations),
            len(answer.uncertainties),
        )
        return SynthesizeResult(
            text=answer.text,
            citations=[
                ReflectiveCitationDict(claim=c.claim, chunk_id=c.chunk_id)
                for c in answer.citations
            ],
            uncertainties=[
                ReflectiveUncertaintyDict(topic=u.topic, reason=u.reason)
                for u in answer.uncertainties
            ],
            refinement_rounds=answer.refinement_rounds,
        )

    # mode == "simple" or "agent" — plain ResponseSynthesizer.
    synthesizer = await get_synthesizer()
    activity.heartbeat({"stage": "plain_synth"})
    response = await synthesizer.asynthesize(query=query, nodes=nodes)
    text = getattr(response, "response", None) or str(response)
    activity.logger.info(
        "synthesize_answer (%s)  text_len=%d", params.mode, len(text or ""),
    )
    return SynthesizeResult(text=text or "")

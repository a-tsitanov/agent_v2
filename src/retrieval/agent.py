"""Agentic multi-hop search loop.

Direct port of ``enterprise-kb/src/retrieval/agent_search.py``,
restructured around LlamaIndex primitives:

  * Retriever — replaces ``HybridSearcher.search(skip_rag=True)``.
    Stage 4 ships a vector-only retriever; Stage 5 swaps in a hybrid
    retriever without touching this module.
  * LLMJudge — replaces ``_judge_context``.  Same JSON contract, same
    defensive fallbacks.
  * ResponseSynthesizer — replaces ``HybridSearcher._ask_rag``.

Stage 6 will plug a graph-search tool here (entities / relations
accumulation, ``hl_keywords`` for the final synthesis).  Until then
the agent runs on chunks alone — which is exactly the LightRAG
``mode="naive"`` regime, intentionally simple to make the agentic
control flow the only variable in early benchmarks.

Logic invariants preserved from enterprise-kb:
  * dedup ``all_sources`` by ``node.node_id`` after every round;
  * compute deltas; if round > 1 and all deltas are zero — skip the
    judge LLM call and exit (Stage G);
  * record ``AgenticRoundStat`` per executed round, including a
    skipped-judge entry on early-exit (Stage H);
  * defensive judge — ANY parse / LLM error → sufficient=True;
  * if judge proposes a follow-up identical to the current query —
    break (anti-loop guard);
  * final synthesis runs on the *enriched query* (original + appended
    follow-ups) with the *accumulated* nodes — never starts from
    scratch (Stage F).
"""

from __future__ import annotations

import time
from typing import Awaitable, Callable, Protocol

from llama_index.core.response_synthesizers import BaseSynthesizer
from llama_index.core.schema import NodeWithScore
from loguru import logger

from src.models.search import (
    AgenticRoundStat,
    SearchResponse,
    SourceCitation,
)


# ── protocols (for easy stubbing in tests) ───────────────────────────


class RetrieverProtocol(Protocol):
    async def aretrieve(
        self, query: str,
    ) -> list[NodeWithScore]: ...


JudgeProtocol = Callable[
    [str, list[NodeWithScore]], Awaitable[dict],
]


class SynthesizerProtocol(Protocol):
    async def asynthesize(
        self, query: str, nodes: list[NodeWithScore],
    ) -> object: ...  # llama_index.core.base.response.schema.Response


# ── helpers ──────────────────────────────────────────────────────────


def _deduplicate_nodes(
    nodes: list[NodeWithScore],
) -> list[NodeWithScore]:
    """Keep first occurrence per ``node.node_id``."""
    seen: set[str] = set()
    out: list[NodeWithScore] = []
    for n in nodes:
        nid = n.node.node_id
        if nid in seen:
            continue
        seen.add(nid)
        out.append(n)
    return out


def _build_enriched_query(query: str, follow_ups: list[str]) -> str:
    """Original + appended unique follow-ups.  Empty list → query
    passes through verbatim (no hollow header)."""
    if not follow_ups:
        return query
    seen: set[str] = {query}
    extras: list[str] = []
    for fu in follow_ups:
        fu = (fu or "").strip()
        if not fu or fu in seen:
            continue
        seen.add(fu)
        extras.append(fu)
    if not extras:
        return query
    return query + "\n\nRelated sub-queries:\n- " + "\n- ".join(extras)


def _node_to_citation(n: NodeWithScore) -> SourceCitation:
    md = n.node.metadata or {}
    return SourceCitation(
        doc_id=str(md.get("doc_id") or md.get("file_path") or ""),
        chunk_id=n.node.node_id,
        position=int(md.get("position", 0) or 0),
        content=n.node.get_content(),
        score=float(n.score or 0.0),
        department=str(md.get("department", "") or ""),
        doc_type=str(md.get("doc_type", "") or md.get("file_type", "") or ""),
    )


# ── main ────────────────────────────────────────────────────────────


async def agentic_search(
    *,
    retriever: RetrieverProtocol,
    judge: JudgeProtocol,
    synthesizer: SynthesizerProtocol,
    query: str,
    max_rounds: int = 3,
    mode: str = "agentic",
) -> SearchResponse:
    """Iterative multi-hop search with LLM judgment loop."""
    t0 = time.monotonic()
    all_sources: list[NodeWithScore] = []
    follow_up_queries: list[str] = []
    round_stats: list[AgenticRoundStat] = []
    current_query = query
    rounds = 0

    for round_num in range(1, max_rounds + 1):
        rounds = round_num
        prev_n = len(all_sources)

        # a. vector retrieve
        round_nodes = await retriever.aretrieve(current_query)
        all_sources.extend(round_nodes)
        all_sources = _deduplicate_nodes(all_sources)

        new_sources = len(all_sources) - prev_n
        logger.info(
            "agentic round={r}  query={q!r}  sources={s} (+{ns})",
            r=round_num, q=current_query,
            s=len(all_sources), ns=new_sources,
        )

        # b. early exit on barren follow-up rounds (Stage G)
        if round_num > 1 and new_sources == 0:
            round_stats.append(AgenticRoundStat(
                round=round_num,
                query=current_query,
                new_sources=0,
                sufficient=None,
                judge_reason="no new info",
            ))
            logger.info(
                "agentic round={r}  early exit — no new info",
                r=round_num,
            )
            break

        # c. judge (LLM call)
        judgment = await judge(query, all_sources)

        round_stats.append(AgenticRoundStat(
            round=round_num,
            query=current_query,
            new_sources=new_sources,
            sufficient=bool(judgment["sufficient"]),
            judge_reason=str(judgment.get("reason", "")),
        ))

        if judgment["sufficient"]:
            break

        follow_up = (judgment.get("follow_up_query") or "").strip()
        if not follow_up or follow_up == current_query:
            break
        follow_up_queries.append(follow_up)
        current_query = follow_up

    # d. final synthesis over the accumulated nodes
    enriched_query = _build_enriched_query(query, follow_up_queries)
    response = await synthesizer.asynthesize(
        query=enriched_query, nodes=all_sources,
    )
    answer_text = (
        getattr(response, "response", None)
        or str(response)
        or ""
    )

    latency_ms = (time.monotonic() - t0) * 1000.0
    logger.info(
        "agentic done  rounds={r}  sources={n}  follow_ups={f}  "
        "latency_ms={ms:.1f}",
        r=rounds, n=len(all_sources),
        f=follow_up_queries, ms=latency_ms,
    )

    return SearchResponse(
        query=query,
        answer=answer_text,
        mode=mode,
        sources=[_node_to_citation(n) for n in all_sources],
        latency_ms=latency_ms,
        agentic_rounds=rounds,
        follow_up_queries=follow_up_queries or None,
        agentic_round_stats=round_stats or None,
    )

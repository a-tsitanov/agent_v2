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
from typing import Awaitable, Callable, Protocol, runtime_checkable

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


@runtime_checkable
class GraphRetrieverProtocol(Protocol):
    """Stage-6 graph retriever — returns dataclass with entities,
    relations, and any chunk nodes attached to the matched
    triplets.  Optional: the agent loop falls back to chunks-only
    behaviour when no graph retriever is supplied."""

    async def aretrieve(self, query: str) -> object: ...


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


def _merge_graph(
    accumulated: dict,
    fresh_entities: list[dict],
    fresh_relations: list[dict],
) -> tuple[dict, int, int]:
    """Dedup-merge graph data round-over-round.  Returns the merged
    dict plus deltas (new_entities, new_relations) for telemetry.
    Mirrors enterprise-kb's ``_merge_graph_data`` semantics
    (entities by name, relations by ``src_id+tgt_id+label``)."""
    ents: list[dict] = list(accumulated.get("entities") or [])
    rels: list[dict] = list(accumulated.get("relations") or [])
    seen_ents = {e.get("entity_name", "") for e in ents}
    seen_rels = {
        f"{r.get('src_id', '')}->{r.get('tgt_id', '')}->{r.get('label', '')}"
        for r in rels
    }
    new_e = 0
    for e in fresh_entities:
        name = e.get("entity_name", "")
        if not name or name in seen_ents:
            continue
        seen_ents.add(name)
        ents.append(e)
        new_e += 1
    new_r = 0
    for r in fresh_relations:
        key = (
            f"{r.get('src_id', '')}->{r.get('tgt_id', '')}->{r.get('label', '')}"
        )
        if key in seen_rels:
            continue
        seen_rels.add(key)
        rels.append(r)
        new_r += 1
    return {"entities": ents, "relations": rels}, new_e, new_r


def _accumulated_hl_keywords(
    graph: dict, *, limit: int = 30,
) -> list[str]:
    """Top-N entity names for ``QueryParam.hl_keywords`` style hints
    in the final synthesis.  Order-preserving (first occurrence
    wins) — same selection rule as enterprise-kb."""
    seen: set[str] = set()
    out: list[str] = []
    for e in graph.get("entities") or []:
        name = (e.get("entity_name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
        if len(out) >= limit:
            break
    return out


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
    graph_retriever: GraphRetrieverProtocol | None = None,
) -> SearchResponse:
    """Iterative multi-hop search with LLM judgment loop.

    When ``graph_retriever`` is provided, each round also queries the
    knowledge graph; entities and relations are deduped across rounds
    and contribute to the early-exit decision.  Final synthesis is
    enriched with the original query + appended follow-ups; graph
    chunks are folded into the final node set so they participate in
    response synthesis just like vector hits.
    """
    t0 = time.monotonic()
    all_sources: list[NodeWithScore] = []
    accumulated_graph: dict = {"entities": [], "relations": []}
    follow_up_queries: list[str] = []
    round_stats: list[AgenticRoundStat] = []
    current_query = query
    rounds = 0

    for round_num in range(1, max_rounds + 1):
        rounds = round_num
        prev_sources = len(all_sources)
        prev_entities = len(accumulated_graph["entities"])
        prev_relations = len(accumulated_graph["relations"])

        # a. vector retrieve
        round_nodes = await retriever.aretrieve(current_query)
        all_sources.extend(round_nodes)
        all_sources = _deduplicate_nodes(all_sources)

        new_entities = 0
        new_relations = 0
        if graph_retriever is not None:
            # b. graph retrieve (entities + relations + extra chunks)
            graph_data = await graph_retriever.aretrieve(current_query)
            fresh_ents = getattr(graph_data, "entities", []) or []
            fresh_rels = getattr(graph_data, "relations", []) or []
            extra_chunks = getattr(graph_data, "chunks", []) or []
            accumulated_graph, new_entities, new_relations = _merge_graph(
                accumulated_graph, fresh_ents, fresh_rels,
            )
            if extra_chunks:
                all_sources.extend(extra_chunks)
                all_sources = _deduplicate_nodes(all_sources)

        new_sources = len(all_sources) - prev_sources
        logger.info(
            "agentic round={r}  query={q!r}  sources={s} (+{ns})  "
            "entities={e} (+{ne})  relations={rel} (+{nr})",
            r=round_num, q=current_query,
            s=len(all_sources), ns=new_sources,
            e=len(accumulated_graph["entities"]), ne=new_entities,
            rel=len(accumulated_graph["relations"]), nr=new_relations,
        )

        # c. early exit on barren follow-up rounds (Stage G)
        if round_num > 1 and new_sources == 0 and new_entities == 0 and new_relations == 0:
            round_stats.append(AgenticRoundStat(
                round=round_num,
                query=current_query,
                new_sources=0,
                new_entities=0,
                new_relations=0,
                sufficient=None,
                judge_reason="no new info",
            ))
            logger.info(
                "agentic round={r}  early exit — no new info",
                r=round_num,
            )
            break

        # d. judge (LLM call)
        judgment = await judge(query, all_sources)

        round_stats.append(AgenticRoundStat(
            round=round_num,
            query=current_query,
            new_sources=new_sources,
            new_entities=new_entities,
            new_relations=new_relations,
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

    # e. final synthesis over the accumulated nodes
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
    hl_keywords = _accumulated_hl_keywords(accumulated_graph)
    logger.info(
        "agentic done  rounds={r}  sources={n}  follow_ups={f}  "
        "hl_keywords={hl}  latency_ms={ms:.1f}",
        r=rounds, n=len(all_sources),
        f=follow_up_queries, hl=len(hl_keywords),
        ms=latency_ms,
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

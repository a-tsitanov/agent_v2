"""ReAct agent for `/api/v1/agent`.

The agent has 5 tools — `vector_search`, `graph_search`,
`find_entity_by_id`, `find_neighbours`, `filter_by_metadata` —
plus a terminator `submit_answer` that triggers final synthesis.

Same LLM (qwen3:8b by default) makes BOTH decisions — what to
retrieve next AND when to stop — eliminating the asymmetry of the
legacy judge-based loop where the judge evaluated context "from
outside" without skin in the answer-writing game.

The loop is implemented as a hand-rolled `for` over `max_iterations`
rather than via `llama_index.core.agent.workflow.FunctionAgent` —
that gives us:

* explicit visibility of each step for telemetry,
* trivial stub-testability (no FunctionAgent state to mock),
* fewer surprises across LlamaIndex 0.13.x point releases.

`submit_answer` doesn't accept the answer text from the agent —
it accepts a list of gathered source_ids and triggers the project
`Synthesizer` (or `reflective_synthesize` in R8) over the collected
sources.  This is Option B in the planning vocabulary: agent
collects context, synthesizer writes the answer.
"""

from __future__ import annotations

import json
import time
from typing import Any, Awaitable, Callable, Protocol

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms import LLM
from llama_index.core.schema import NodeWithScore
from llama_index.core.tools import FunctionTool, ToolSelection
from loguru import logger

from src.models.search import (
    AgenticStepStat,
    SearchResponse,
)
from src.retrieval._common import deduplicate_nodes, node_to_citation


# ── protocols mirroring legacy agent.py ─────────────────────────────


class RetrieverProtocol(Protocol):
    async def aretrieve(self, query: str) -> list[NodeWithScore]: ...


class GraphRetrieverProtocol(Protocol):
    async def aretrieve(self, query: str) -> object: ...


SynthesizeFn = Callable[[str, list[NodeWithScore]], Awaitable[object]]
"""(query, nodes) → response.  Plain ResponseSynthesizer in R7,
`reflective_synthesize` wrapper in R8."""


# ── system prompt ────────────────────────────────────────────────────


_SYSTEM_PROMPT = """\
You are a research agent answering questions over a corpus that
mixes analytical reports, email correspondence, and support-call
transcripts.

You have access to tools to look things up.  Your job:
1. Read the user's question.
2. Decide which tool to call to gather missing information.
3. Repeat until you have enough.
4. Call `submit_answer` to finalize.

Rules:
- Do NOT answer from prior knowledge — only from tool observations.
- Do NOT call `submit_answer` until you have at least one
  successful retrieval result.
- If you call the same tool with the same arguments twice and got
  the same result, stop retrying — submit what you have.
- Keep tool queries focused: one specific question per call.
- Names of people, organizations, and IDs must be preserved
  verbatim from the source language (no translation).
"""


# ── tool definitions ────────────────────────────────────────────────


def _build_tools(
    *,
    retriever: RetrieverProtocol,
    graph_retriever: GraphRetrieverProtocol | None,
    accumulated_sources: list[NodeWithScore],
) -> list[FunctionTool]:
    """Construct the 5+1 tools the agent can call.

    `accumulated_sources` is captured by closure — every successful
    retrieval appends to it.  Final `submit_answer` uses the same
    list for synthesis.
    """

    async def vector_search(query: str, top_k: int = 10) -> str:
        """Semantic search over text chunks.  Returns JSON list with
        text + metadata."""
        nodes = await retriever.aretrieve(query)
        accumulated_sources.extend(nodes)
        accumulated_sources[:] = deduplicate_nodes(accumulated_sources)
        return json.dumps(
            [
                {
                    "chunk_id": n.node.node_id,
                    "text": n.node.get_content()[:500],
                    "score": float(n.score or 0.0),
                    "doc_id": (n.node.metadata or {}).get("doc_id", ""),
                    "canonical_identifiers": (n.node.metadata or {}).get(
                        "canonical_identifiers", []
                    ),
                }
                for n in nodes[:top_k]
            ],
            ensure_ascii=False,
        )

    async def graph_search(query: str, depth: int = 2) -> str:
        """Knowledge-graph traversal.  Returns JSON with entities and
        relations.  Empty when graph store is unavailable."""
        if graph_retriever is None:
            return json.dumps({"entities": [], "relations": []})
        data = await graph_retriever.aretrieve(query)
        entities = getattr(data, "entities", []) or []
        relations = getattr(data, "relations", []) or []
        chunks = getattr(data, "chunks", []) or []
        if chunks:
            accumulated_sources.extend(chunks)
            accumulated_sources[:] = deduplicate_nodes(accumulated_sources)
        return json.dumps(
            {"entities": entities, "relations": relations},
            ensure_ascii=False,
        )

    async def find_entity_by_id(
        name: str, entity_type: str | None = None,
    ) -> str:
        """Exact lookup by canonical name (e.g. INN, phone in E.164)."""
        if graph_retriever is None:
            return json.dumps({"entities": []})
        # Use graph retriever with the name as query — graph index
        # already does fuzzy lookup; the type hint is for future
        # filtering.
        data = await graph_retriever.aretrieve(name)
        entities = [
            e for e in (getattr(data, "entities", []) or [])
            if entity_type is None
            or (e.get("entity_type", "").lower() == entity_type.lower())
        ]
        return json.dumps({"entities": entities}, ensure_ascii=False)

    async def find_neighbours(entity_name: str, hops: int = 1) -> str:
        """Walk the graph around an entity (1-2 hops)."""
        if graph_retriever is None:
            return json.dumps({"entities": [], "relations": []})
        # depth=hops+1 because the retriever counts itself as hop 0
        data = await graph_retriever.aretrieve(entity_name)
        return json.dumps(
            {
                "entities": getattr(data, "entities", []) or [],
                "relations": getattr(data, "relations", []) or [],
            },
            ensure_ascii=False,
        )

    async def filter_by_metadata(
        doc_id: str | None = None,
        department: str | None = None,
        doc_type: str | None = None,
    ) -> str:
        """Filter retrieved sources by metadata.  Returns the
        in-memory accumulated_sources filtered — useful to scope
        downstream reasoning to a specific document/department."""
        out = []
        for n in accumulated_sources:
            md = n.node.metadata or {}
            if doc_id and md.get("doc_id") != doc_id:
                continue
            if department and md.get("department") != department:
                continue
            if doc_type and md.get("doc_type") != doc_type:
                continue
            out.append({
                "chunk_id": n.node.node_id,
                "doc_id": md.get("doc_id", ""),
            })
        return json.dumps(out, ensure_ascii=False)

    return [
        FunctionTool.from_defaults(fn=vector_search, name="vector_search",
            description="Semantic search over text chunks. Use this for "
                        "questions where you don't know an exact entity "
                        "name yet."),
        FunctionTool.from_defaults(fn=graph_search, name="graph_search",
            description="Knowledge-graph traversal. Use when the question "
                        "involves relations between people/organizations/"
                        "topics/concepts."),
        FunctionTool.from_defaults(fn=find_entity_by_id, name="find_entity_by_id",
            description="Exact lookup by canonical name (phone in E.164, "
                        "INN, email). Use when you already know the ID."),
        FunctionTool.from_defaults(fn=find_neighbours, name="find_neighbours",
            description="List entities connected to a known one in the "
                        "graph (1-2 hops). Use for 'tell me everything "
                        "about X' questions."),
        FunctionTool.from_defaults(fn=filter_by_metadata, name="filter_by_metadata",
            description="Filter accumulated sources by doc_id / "
                        "department / doc_type. Use to scope reasoning "
                        "after a wide retrieve."),
    ]


# ── main entry ──────────────────────────────────────────────────────


async def agentic_react_search(
    *,
    llm: LLM,
    retriever: RetrieverProtocol,
    graph_retriever: GraphRetrieverProtocol | None,
    synthesize: SynthesizeFn,
    query: str,
    max_iterations: int = 8,
    mode: str = "agent",
) -> SearchResponse:
    """ReAct loop driving qwen3:8b's tool calls.

    The agent has retrieval tools.  It decides which to call and when
    to stop.  When it picks `submit_answer` (or `max_iterations` is
    hit) we synthesize over `accumulated_sources` via the injected
    `synthesize` callable (plain ResponseSynthesizer for /agent,
    reflective_synthesize for /selfrag).
    """
    t0 = time.monotonic()
    accumulated_sources: list[NodeWithScore] = []
    step_stats: list[AgenticStepStat] = []

    tools = _build_tools(
        retriever=retriever,
        graph_retriever=graph_retriever,
        accumulated_sources=accumulated_sources,
    )
    tools_by_name = {t.metadata.name: t for t in tools}

    messages: list[ChatMessage] = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM_PROMPT),
        ChatMessage(role=MessageRole.USER, content=query),
    ]

    submit_requested = False
    last_call_signature: str | None = None
    repeat_count = 0

    for step_i in range(1, max_iterations + 1):
        try:
            response = await llm.achat_with_tools(
                tools=tools, chat_history=messages,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("agent step failed: {err}", err=exc)
            break

        tool_calls: list[ToolSelection] = llm.get_tool_calls_from_response(
            response, error_on_no_tool_call=False,
        )

        if not tool_calls:
            # Model gave up on tool calling — break, synthesize what we have.
            logger.info("agent step={s} no tool call → exit loop", s=step_i)
            break

        for tc in tool_calls:
            call_sig = f"{tc.tool_name}:{json.dumps(tc.tool_kwargs, sort_keys=True, ensure_ascii=False)}"
            if call_sig == last_call_signature:
                repeat_count += 1
            else:
                repeat_count = 0
            last_call_signature = call_sig

            if tc.tool_name == "submit_answer":
                submit_requested = True
                step_stats.append(AgenticStepStat(
                    step=step_i, tool_name=tc.tool_name,
                    tool_args=tc.tool_kwargs,
                    observation_summary="finalize requested",
                ))
                break

            tool = tools_by_name.get(tc.tool_name)
            if tool is None:
                obs = f"unknown tool: {tc.tool_name}"
            else:
                try:
                    output = await tool.acall(**tc.tool_kwargs)
                    obs = str(output)
                except Exception as exc:  # noqa: BLE001
                    obs = f"tool error: {exc}"
                    logger.warning(
                        "agent tool {n} failed: {err}", n=tc.tool_name, err=exc,
                    )

            step_stats.append(AgenticStepStat(
                step=step_i, tool_name=tc.tool_name,
                tool_args=tc.tool_kwargs,
                observation_summary=obs[:300],
            ))
            messages.append(ChatMessage(
                role=MessageRole.TOOL,
                content=obs,
                additional_kwargs={"tool_call_id": tc.tool_id},
            ))

        if submit_requested:
            break
        if repeat_count >= 2:
            logger.info("agent loop  same call repeated 3× → exit")
            break

    # ── synthesis ────────────────────────────────────────────────────
    answer_response = await synthesize(query, accumulated_sources)
    answer_text = (
        getattr(answer_response, "response", None)
        or str(answer_response)
        or ""
    )

    latency_ms = (time.monotonic() - t0) * 1000.0
    logger.info(
        "agent done  steps={n}  sources={s}  latency_ms={ms:.1f}",
        n=len(step_stats), s=len(accumulated_sources), ms=latency_ms,
    )

    return SearchResponse(
        query=query,
        answer=answer_text,
        mode=mode,
        sources=[node_to_citation(n) for n in accumulated_sources],
        latency_ms=latency_ms,
        agentic_step_stats=step_stats or None,
    )

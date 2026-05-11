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
from src.observability.trace import record_event, record_timed
from src.retrieval._common import deduplicate_nodes, node_to_citation
from src.storage.chunk_repository import ChunkRepository


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
transcripts.  User questions arrive in Russian; the knowledge
graph (entities, descriptions, relations) is normalised to
Russian, while raw chunk text may be in any source language.

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
- Tool queries may use Russian terms — the graph is Russian.  When
  querying for source-language strings (proper names, identifiers),
  preserve them verbatim.
- Final answer goes through a separate synthesizer that writes
  in Russian; you don't need to translate the tool outputs yourself.
"""


# ── tool definitions ────────────────────────────────────────────────


def _build_tools(
    *,
    retriever: RetrieverProtocol,
    graph_retriever: GraphRetrieverProtocol | None,
    chunk_repository: ChunkRepository | None,
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

    async def get_chunks_by_doc_id(
        doc_id: str, limit: int = 50, offset: int = 0,
    ) -> str:
        """Fetch ALL chunks of a single document by `doc_id`,
        ordered by their position in the source.  Use this when:
        - vector_search returned one promising chunk and you need
          the surrounding context within the same document;
        - the user asks "everything from this thread / file";
        - you need to scope reasoning to a single document the user
          already cited.
        Pages via `limit`/`offset` so a 1000-chunk doc doesn't
        blow up the conversation.  Returns JSON list."""
        if chunk_repository is None:
            return json.dumps({"error": "chunk_repository unavailable"})
        try:
            chunks = await chunk_repository.aget_chunks_by_doc_id(
                doc_id, limit=limit, offset=offset,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "get_chunks_by_doc_id failed doc={d} err={e}",
                d=doc_id, e=exc,
            )
            return json.dumps({"error": str(exc), "chunks": []})
        # Mark these as accumulated sources so submit_answer's
        # synthesizer sees them.  Wrap each in a minimal NodeWithScore.
        from llama_index.core.schema import NodeWithScore, TextNode

        for c in chunks:
            tn = TextNode(
                id_=c["chunk_id"] or f"{doc_id}#{c['position']}",
                text=c["text"],
                metadata={
                    "doc_id": c["doc_id"],
                    "file_path": c["file_path"],
                    "position": c["position"],
                },
            )
            accumulated_sources.append(NodeWithScore(node=tn, score=0.0))
        accumulated_sources[:] = deduplicate_nodes(accumulated_sources)
        return json.dumps([
            {
                "chunk_id": c["chunk_id"],
                "position": c["position"],
                "text": c["text"][:400],
                "doc_id": c["doc_id"],
            }
            for c in chunks
        ], ensure_ascii=False)

    async def read_full_document(
        doc_id: str, max_chars: int = 20000,
    ) -> str:
        """Read the original source file of one document (as
        uploaded — pre-chunking, pre-translation).  Capped at
        `max_chars` to protect the context.  Use SPARINGLY — vector
        / chunk-level tools are cheaper and more focused.  Good for:
        - short documents (< 20k chars) the user wants summarised
          in full;
        - verifying citation against original verbatim;
        - documents whose structure (table, code) suffers in chunked
          retrieval.  Returns the raw text or an error message."""
        if chunk_repository is None:
            return "Error: chunk_repository unavailable"
        try:
            text = await chunk_repository.aread_document_text(
                doc_id, max_chars=max_chars,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "read_full_document failed doc={d} err={e}",
                d=doc_id, e=exc,
            )
            return f"Error: {exc}"
        if text is None:
            return f"Error: document {doc_id} not found"
        return text

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
        FunctionTool.from_defaults(fn=get_chunks_by_doc_id, name="get_chunks_by_doc_id",
            description="Fetch ALL chunks of one document by doc_id, "
                        "ordered by position. Use when a single chunk "
                        "isn't enough and you need surrounding context "
                        "from the same source."),
        FunctionTool.from_defaults(fn=read_full_document, name="read_full_document",
            description="Read the raw uploaded source file (pre-chunk, "
                        "pre-translation) by doc_id, capped at max_chars. "
                        "Use only when chunk-level retrieval can't surface "
                        "what you need — table / code / short doc cases."),
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
    chunk_repository: ChunkRepository | None = None,
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
        chunk_repository=chunk_repository,
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
            with record_timed("llm_call", step=step_i, kind="agent_reasoning"):
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
                    with record_timed(
                        "tool_call", step=step_i,
                        tool_name=tc.tool_name, tool_args=tc.tool_kwargs,
                    ):
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
    with record_timed("synthesize", n_sources=len(accumulated_sources)):
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

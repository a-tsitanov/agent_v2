"""Unit tests for `agentic_react_search`.

Stubs LLM, retriever, synthesizer.  Validates the loop control —
tool routing, repetition guard, submit_answer termination,
max_iterations cap, synthesizer is called with accumulated sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from llama_index.core.base.llms.types import ChatMessage, ChatResponse
from llama_index.core.llms import ChatResponseAsyncGen, MessageRole
from llama_index.core.schema import NodeWithScore, TextNode
from llama_index.core.tools import ToolSelection

from src.retrieval.react_agent import agentic_react_search


# ── stubs ───────────────────────────────────────────────────────────


@dataclass
class _ScriptedLLM:
    """Plays back a queue of (tool_name, tool_kwargs) pairs as if
    the model emitted them in successive turns.  Empty tuple
    means "no tool call" — terminates the loop.
    """

    plan: list[tuple[str, dict[str, Any]] | None]
    calls: list[ChatMessage] = field(default_factory=list)

    async def achat_with_tools(self, tools, chat_history, **_):
        self.calls.append(chat_history[-1])
        if not self.plan:
            # Default: no tool call
            return _ChatRespFake(tool_calls=[])
        next_step = self.plan.pop(0)
        if next_step is None:
            return _ChatRespFake(tool_calls=[])
        name, kwargs = next_step
        return _ChatRespFake(
            tool_calls=[
                ToolSelection(
                    tool_id=f"call-{len(self.calls)}",
                    tool_name=name,
                    tool_kwargs=kwargs,
                ),
            ]
        )

    def get_tool_calls_from_response(self, response, **_):
        return response.tool_calls


@dataclass
class _ChatRespFake:
    tool_calls: list[ToolSelection]
    message: ChatMessage = field(
        default_factory=lambda: ChatMessage(role=MessageRole.ASSISTANT, content=""),
    )


@dataclass
class _StubRetriever:
    responses: list[list[NodeWithScore]] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    async def aretrieve(self, query: str) -> list[NodeWithScore]:
        self.calls.append(query)
        if not self.responses:
            return []
        return self.responses.pop(0)


@dataclass
class _GraphData:
    entities: list[dict] = field(default_factory=list)
    relations: list[dict] = field(default_factory=list)
    chunks: list[NodeWithScore] = field(default_factory=list)


@dataclass
class _StubGraph:
    response: _GraphData | None = None

    async def aretrieve(self, query: str) -> _GraphData:
        return self.response or _GraphData()


@dataclass
class _StubSynth:
    answer: str = "synthesized"
    received: dict | None = None

    async def __call__(self, query, nodes):
        self.received = {"query": query, "n_nodes": len(nodes)}

        class _Resp:
            response = self.answer

        return _Resp()


def _node(node_id: str, text: str = "chunk content") -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(id_=node_id, text=text, metadata={"doc_id": "d1"}),
        score=0.9,
    )


# ── tests ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_one_tool_then_submit() -> None:
    llm = _ScriptedLLM(plan=[
        ("vector_search", {"query": "X"}),
        ("submit_answer", {"query_recap": "X", "gathered_source_ids": []}),
    ])
    retriever = _StubRetriever(responses=[[_node("c1"), _node("c2")]])
    synth = _StubSynth()

    result = await agentic_react_search(
        llm=llm, retriever=retriever, graph_retriever=None,
        synthesize=synth, query="X", max_iterations=5,
    )

    assert result.answer == "synthesized"
    assert result.agentic_step_stats is not None
    assert len(result.agentic_step_stats) == 2
    assert result.agentic_step_stats[0].tool_name == "vector_search"
    assert result.agentic_step_stats[1].tool_name == "submit_answer"
    assert synth.received["n_nodes"] == 2  # type: ignore[index]


@pytest.mark.asyncio
async def test_max_iterations_cap() -> None:
    """If agent never calls submit_answer, loop terminates after
    max_iterations and synth still runs."""
    llm = _ScriptedLLM(plan=[
        ("vector_search", {"query": "X"}),
        ("vector_search", {"query": "Y"}),
        ("vector_search", {"query": "Z"}),
    ])  # 3 plan steps, max_iterations=3
    retriever = _StubRetriever(responses=[[_node("c1")], [_node("c2")], [_node("c3")]])
    synth = _StubSynth()

    result = await agentic_react_search(
        llm=llm, retriever=retriever, graph_retriever=None,
        synthesize=synth, query="X", max_iterations=3,
    )

    assert len(result.agentic_step_stats) == 3
    # All 3 vector_search; none was submit_answer
    assert all(s.tool_name == "vector_search" for s in result.agentic_step_stats)
    # Synth still called over accumulated context
    assert synth.received["n_nodes"] == 3  # type: ignore[index]


@pytest.mark.asyncio
async def test_repetition_guard() -> None:
    """Same tool with same args 3× in a row → break out of loop."""
    llm = _ScriptedLLM(plan=[
        ("vector_search", {"query": "same"}),
        ("vector_search", {"query": "same"}),
        ("vector_search", {"query": "same"}),
        ("vector_search", {"query": "same"}),  # never reached
    ])
    retriever = _StubRetriever(responses=[[_node("c1")]] * 4)
    synth = _StubSynth()

    result = await agentic_react_search(
        llm=llm, retriever=retriever, graph_retriever=None,
        synthesize=synth, query="X", max_iterations=10,
    )

    # 3 calls happened, then repetition guard triggers
    assert len(result.agentic_step_stats) == 3


@pytest.mark.asyncio
async def test_no_tool_call_breaks_loop() -> None:
    """If model returns no tool call, loop exits (model gave up)."""
    llm = _ScriptedLLM(plan=[None])
    retriever = _StubRetriever()
    synth = _StubSynth()

    result = await agentic_react_search(
        llm=llm, retriever=retriever, graph_retriever=None,
        synthesize=synth, query="X", max_iterations=5,
    )

    # No tool steps recorded
    assert result.agentic_step_stats is None
    # Synth still called (with zero nodes)
    assert synth.received["n_nodes"] == 0  # type: ignore[index]


@pytest.mark.asyncio
async def test_graph_search_tool_runs_when_graph_retriever_provided() -> None:
    llm = _ScriptedLLM(plan=[
        ("graph_search", {"query": "topic X"}),
        ("submit_answer", {"query_recap": "X", "gathered_source_ids": []}),
    ])
    retriever = _StubRetriever()
    graph = _StubGraph(response=_GraphData(
        entities=[{"entity_name": "Topic X", "entity_type": "Topic"}],
        relations=[{"src_id": "Topic X", "label": "MENTIONED_BY", "tgt_id": "Doc 1"}],
    ))
    synth = _StubSynth()

    result = await agentic_react_search(
        llm=llm, retriever=retriever, graph_retriever=graph,
        synthesize=synth, query="X", max_iterations=5,
    )

    assert len(result.agentic_step_stats) == 2
    assert result.agentic_step_stats[0].tool_name == "graph_search"
    obs = result.agentic_step_stats[0].observation_summary
    assert "Topic X" in obs
    assert "MENTIONED_BY" in obs

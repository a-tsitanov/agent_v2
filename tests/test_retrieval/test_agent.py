"""Stage-4 tests for the agentic search loop.

Stubs every collaborator (retriever, judge, synthesizer) so the suite
runs without a live LLM / Milvus / embeddings.  The tests intentionally
mirror the matrix in
``enterprise-kb/tests/test_retrieval/test_agent_search.py`` — when both
agents ship, behaviour parity is the easiest property to compare.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from src.retrieval.agent import (
    _build_enriched_query,
    _deduplicate_nodes,
    agentic_search,
)


# ── stubs ────────────────────────────────────────────────────────────


def _node(node_id: str, text: str = "chunk") -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(id_=node_id, text=text, metadata={"doc_id": "d1"}),
        score=0.9,
    )


@dataclass
class StubRetriever:
    responses: list[list[NodeWithScore]] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    async def aretrieve(self, query: str) -> list[NodeWithScore]:
        self.calls.append(query)
        if not self.responses:
            return []
        return self.responses.pop(0)


@dataclass
class StubJudge:
    """Returns canned judge replies; counts calls."""

    replies: list[dict[str, Any]] = field(default_factory=list)
    calls: list[tuple[str, int]] = field(default_factory=list)

    async def __call__(self, original_query: str, sources):
        self.calls.append((original_query, len(sources)))
        if not self.replies:
            return {"sufficient": True, "follow_up_query": "", "reason": ""}
        return self.replies.pop(0)


@dataclass
class StubSynthesizer:
    answer: str = "synthesized"
    calls: list[dict] = field(default_factory=list)

    async def asynthesize(self, query: str, nodes):
        self.calls.append({"query": query, "n_nodes": len(nodes)})

        class _Resp:
            response = self.answer

        return _Resp()


# ── helper unit tests ────────────────────────────────────────────────


def test_deduplicate_nodes_keeps_first() -> None:
    a = _node("c1")
    b = _node("c2")
    c = _node("c1")  # dup
    assert [n.node.node_id for n in _deduplicate_nodes([a, b, c])] == ["c1", "c2"]


def test_build_enriched_query_no_followups() -> None:
    assert _build_enriched_query("q", []) == "q"


def test_build_enriched_query_appends_unique() -> None:
    out = _build_enriched_query("q", ["a", "q", "", "a", "b"])
    assert "Related sub-queries:" in out
    assert "- a" in out
    assert "- b" in out
    assert out.count("- a") == 1


# ── end-to-end via stubs ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_one_round_sufficient() -> None:
    retriever = StubRetriever(responses=[[_node("c1"), _node("c2")]])
    judge = StubJudge(replies=[{"sufficient": True, "follow_up_query": "", "reason": "ok"}])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3,
    )

    assert result.agentic_rounds == 1
    assert result.follow_up_queries is None
    assert result.answer == "synthesized"
    assert len(retriever.calls) == 1
    assert len(judge.calls) == 1
    assert len(synth.calls) == 1
    # synthesizer received the original query (no enrichment)
    assert synth.calls[0]["query"] == "q"


@pytest.mark.asyncio
async def test_two_rounds_with_followup() -> None:
    retriever = StubRetriever(responses=[
        [_node("c1")],
        [_node("c2"), _node("c3")],
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "next", "reason": "more"},
        {"sufficient": True, "follow_up_query": "", "reason": ""},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3,
    )

    assert result.agentic_rounds == 2
    assert result.follow_up_queries == ["next"]
    # synthesizer saw enriched query
    assert "Related sub-queries:" in synth.calls[0]["query"]
    assert synth.calls[0]["n_nodes"] == 3  # accumulated, deduped


@pytest.mark.asyncio
async def test_max_rounds_reached() -> None:
    retriever = StubRetriever(responses=[
        [_node("c1")], [_node("c2")], [_node("c3")],
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q2", "reason": "r"},
        {"sufficient": False, "follow_up_query": "q3", "reason": "r"},
        {"sufficient": False, "follow_up_query": "q4", "reason": "r"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q1", max_rounds=3,
    )

    assert result.agentic_rounds == 3
    assert len(judge.calls) == 3
    assert result.follow_up_queries == ["q2", "q3", "q4"]


@pytest.mark.asyncio
async def test_early_exit_on_no_new_info() -> None:
    """Round 2 returns the SAME nodes as round 1 — judge must not be
    called a second time, loop exits with round-1 context."""
    same = [_node("c1"), _node("c2")]
    retriever = StubRetriever(responses=[list(same), list(same)])
    judge = StubJudge(replies=[
        # Only one reply — second judge call would error trying to pop
        # from empty list, but stub handles that defensively.  We
        # verify by counting calls.
        {"sufficient": False, "follow_up_query": "q2", "reason": "r"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q1", max_rounds=3,
    )

    assert result.agentic_rounds == 2
    assert len(judge.calls) == 1  # judge skipped on round 2
    # round_stats records the skipped round
    assert result.agentic_round_stats is not None
    assert len(result.agentic_round_stats) == 2
    assert result.agentic_round_stats[1].sufficient is None
    assert result.agentic_round_stats[1].judge_reason == "no new info"
    assert result.agentic_round_stats[1].new_sources == 0


@pytest.mark.asyncio
async def test_followup_equal_to_current_breaks_loop() -> None:
    retriever = StubRetriever(responses=[[_node("c1")]])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q1", "reason": "loop"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q1", max_rounds=3,
    )

    assert result.agentic_rounds == 1
    assert result.follow_up_queries is None
    assert len(judge.calls) == 1


@pytest.mark.asyncio
async def test_round_stats_populated_per_round() -> None:
    retriever = StubRetriever(responses=[
        [_node("c1"), _node("c2")],
        [_node("c3")],
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q2", "reason": "more"},
        {"sufficient": True, "follow_up_query": "", "reason": "complete"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3,
    )

    stats = result.agentic_round_stats
    assert stats is not None and len(stats) == 2
    assert stats[0].round == 1 and stats[0].new_sources == 2
    assert stats[0].sufficient is False
    assert "more" in stats[0].judge_reason
    assert stats[1].round == 2 and stats[1].new_sources == 1
    assert stats[1].sufficient is True


@pytest.mark.asyncio
async def test_invalid_judge_json_defaults_to_sufficient() -> None:
    """``LLMJudge`` swallows parse errors, but a custom judge raising
    is also handled — agentic_search trusts the dict it receives.
    Test the contract via a stub that returns sufficient=True on
    parse error (matches LLMJudge defensive behavior)."""
    retriever = StubRetriever(responses=[[_node("c1")]])
    # judge returns the defensive default
    judge = StubJudge(replies=[
        {"sufficient": True, "follow_up_query": "", "reason": "parse error: ..."},
    ])
    synth = StubSynthesizer()
    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3,
    )
    assert result.agentic_rounds == 1
    assert result.answer == "synthesized"


@pytest.mark.asyncio
async def test_sources_dedup_across_rounds() -> None:
    """Same node returned across two rounds should appear once in
    final sources."""
    retriever = StubRetriever(responses=[
        [_node("c1")],
        [_node("c1"), _node("c2")],  # c1 duplicated
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q2", "reason": "r"},
        {"sufficient": True, "follow_up_query": "", "reason": ""},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3,
    )

    chunk_ids = [s.chunk_id for s in result.sources]
    assert chunk_ids == ["c1", "c2"]


# ── LLMJudge unit tests ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_llm_judge_parses_plain_json() -> None:
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse
    from src.retrieval.judge import LLMJudge

    class _StubLLM:
        async def achat(self, messages):
            payload = {"sufficient": False, "follow_up_query": "x", "reason": "y"}
            return ChatResponse(
                message=ChatMessage(role="assistant", content=json.dumps(payload))
            )

    out = await LLMJudge(_StubLLM())("q", [])  # type: ignore[arg-type]
    assert out["sufficient"] is False
    assert out["follow_up_query"] == "x"
    assert out["reason"] == "y"


@pytest.mark.asyncio
async def test_llm_judge_strips_markdown_fence() -> None:
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse
    from src.retrieval.judge import LLMJudge

    class _StubLLM:
        async def achat(self, messages):
            content = "```json\n" + json.dumps({"sufficient": True}) + "\n```"
            return ChatResponse(
                message=ChatMessage(role="assistant", content=content)
            )

    out = await LLMJudge(_StubLLM())("q", [])  # type: ignore[arg-type]
    assert out["sufficient"] is True


@pytest.mark.asyncio
async def test_llm_judge_invalid_json_defaults_to_sufficient() -> None:
    from llama_index.core.base.llms.types import ChatMessage, ChatResponse
    from src.retrieval.judge import LLMJudge

    class _StubLLM:
        async def achat(self, messages):
            return ChatResponse(
                message=ChatMessage(role="assistant", content="not json {[}")
            )

    out = await LLMJudge(_StubLLM())("q", [])  # type: ignore[arg-type]
    assert out["sufficient"] is True


@pytest.mark.asyncio
async def test_llm_judge_exception_defaults_to_sufficient() -> None:
    from src.retrieval.judge import LLMJudge

    class _RaisingLLM:
        async def achat(self, messages):
            raise RuntimeError("LLM down")

    out = await LLMJudge(_RaisingLLM())("q", [])  # type: ignore[arg-type]
    assert out["sufficient"] is True
    assert "LLM down" in out["reason"]

"""Unit tests for the reflective synthesizer (R8).

Stubs LLM and retriever. Validates marker parsing, citation
mapping against context, NEED-driven refinement loop, max_refinements
budget, no-retriever short-circuit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    MessageRole,
)
from llama_index.core.schema import NodeWithScore, TextNode

from src.retrieval.reflective_synth import (
    parse_markers,
    reflective_synthesize,
    strip_markers,
)


# ── stubs ───────────────────────────────────────────────────────────


@dataclass
class _ScriptedLLM:
    """Returns a queued draft string per achat() call."""

    drafts: list[str]
    received_user_msgs: list[str] = field(default_factory=list)

    async def achat(self, messages: list[ChatMessage], **_: Any) -> ChatResponse:
        self.received_user_msgs.append(messages[-1].content or "")
        draft = self.drafts.pop(0) if self.drafts else ""
        return ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content=draft),
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


def _node(node_id: str, text: str = "chunk content") -> NodeWithScore:
    return NodeWithScore(
        node=TextNode(id_=node_id, text=text, metadata={"doc_id": "d1"}),
        score=0.9,
    )


# ── marker parsing ──────────────────────────────────────────────────


def test_parse_markers_all_three_kinds() -> None:
    draft = (
        "Conversion grew 12% in Q1 [SUPPORTED:c1] thanks to onboarding "
        "redesign [NEED:exact launch date]. The Q2 number is "
        "[UNCERTAIN:not in retrieved context]."
    )
    needs, supports, uncertains = parse_markers(draft)
    assert needs == ["exact launch date"]
    assert supports == ["c1"]
    assert uncertains == ["not in retrieved context"]


def test_strip_markers_keeps_uncertain_drops_others() -> None:
    draft = (
        "Conversion grew 12% [SUPPORTED:c1] in Q1 [NEED:date]. "
        "Q2 is [UNCERTAIN:no data]."
    )
    out = strip_markers(draft, keep_uncertain=True)
    assert "[SUPPORTED:c1]" not in out
    assert "[NEED:" not in out
    assert "[UNCERTAIN:no data]" in out


def test_strip_markers_drops_all_when_keep_uncertain_false() -> None:
    draft = "X [SUPPORTED:c1] [NEED:y] [UNCERTAIN:z]."
    out = strip_markers(draft, keep_uncertain=False)
    assert "[" not in out


# ── reflective_synthesize loop ──────────────────────────────────────


@pytest.mark.asyncio
async def test_zero_needs_terminates_immediately() -> None:
    """If first draft has no [NEED], skip refinement loop."""
    llm = _ScriptedLLM(drafts=["Final answer [SUPPORTED:c1] done."])
    retriever = _StubRetriever()

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=retriever,
        max_refinements=3,
    )

    # Only one LLM call (initial draft), no retrieve
    assert len(llm.received_user_msgs) == 1
    assert retriever.calls == []
    assert answer.refinement_rounds == 0
    assert len(answer.citations) == 1
    assert answer.citations[0].chunk_id == "c1"


@pytest.mark.asyncio
async def test_need_triggers_retrieve_and_redraft() -> None:
    llm = _ScriptedLLM(drafts=[
        "Partial [NEED:Q1 launch date]",
        "Resolved on 2024-02-15 [SUPPORTED:c2]",
    ])
    retriever = _StubRetriever(responses=[[_node("c2", "launch date is Feb 15")]])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=retriever,
        max_refinements=3,
    )

    assert len(retriever.calls) == 1
    assert retriever.calls[0] == "Q1 launch date"
    assert answer.refinement_rounds == 1
    assert any(c.chunk_id == "c2" for c in answer.citations)
    assert "Resolved on 2024-02-15" in answer.text


@pytest.mark.asyncio
async def test_max_refinements_caps_loop() -> None:
    """If LLM keeps emitting [NEED:...] markers, stop after budget."""
    llm = _ScriptedLLM(drafts=[
        "[NEED:x]",  # round 0
        "[NEED:y]",  # round 1
        "[NEED:z]",  # round 2 — last allowed
        "should not run",
    ])
    retriever = _StubRetriever(responses=[[_node("c2")], [_node("c3")], [_node("c4")]])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=retriever,
        max_refinements=2,
    )

    # 3 LLM calls (round 0, 1, 2), then budget exhausted
    assert len(llm.received_user_msgs) == 3
    assert answer.refinement_rounds == 2


@pytest.mark.asyncio
async def test_no_retriever_short_circuits_refinement() -> None:
    """If no retriever provided, NEED markers exit loop after the
    first draft — no point in re-drafting without new context."""
    llm = _ScriptedLLM(drafts=[
        "Partial [NEED:x]",
        "should not run",
    ])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=None,
        max_refinements=3,
    )

    assert len(llm.received_user_msgs) == 1
    assert answer.refinement_rounds == 0


@pytest.mark.asyncio
async def test_hallucinated_chunk_id_dropped() -> None:
    """If model cites a [SUPPORTED:zzz] where zzz isn't in context,
    the citation is dropped from the structured detail (not silently
    propagated as a phantom claim)."""
    llm = _ScriptedLLM(drafts=["X [SUPPORTED:c1] Y [SUPPORTED:zzz]."])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=None,
        max_refinements=0,
    )

    ids = [c.chunk_id for c in answer.citations]
    assert "c1" in ids
    assert "zzz" not in ids


@pytest.mark.asyncio
async def test_uncertainty_preserved_in_answer_text() -> None:
    """[UNCERTAIN:...] markers stay visible to the caller."""
    llm = _ScriptedLLM(drafts=[
        "Known fact [SUPPORTED:c1]. Q2 number [UNCERTAIN:no data in corpus]."
    ])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=None,
        max_refinements=0,
    )

    assert "[UNCERTAIN:no data in corpus]" in answer.text
    assert len(answer.uncertainties) == 1
    assert answer.uncertainties[0].reason == "no data in corpus"


@pytest.mark.asyncio
async def test_response_property_for_react_compat() -> None:
    """ReflectiveAnswer.response mirrors text — react_agent.py
    treats both the plain ResponseSynthesizer return and
    ReflectiveAnswer the same way."""
    llm = _ScriptedLLM(drafts=["Final."])

    answer = await reflective_synthesize(
        llm=llm, query="Q",
        context_nodes=[_node("c1")],
        retriever=None,
        max_refinements=0,
    )

    assert answer.response == answer.text == "Final."

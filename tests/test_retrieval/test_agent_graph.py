"""Stage-6 tests for the graph-augmented agentic loop.

Stubs ``GraphRetrieverProtocol`` to verify the agent dedupes
entities/relations across rounds, exits early when *neither* sources
nor graph data grew, and folds graph chunks into the final
synthesis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from llama_index.core.schema import NodeWithScore, TextNode

from src.retrieval.agent import (
    _accumulated_hl_keywords,
    _merge_graph,
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

    async def aretrieve(self, query: str):
        if not self.responses:
            return []
        return self.responses.pop(0)


@dataclass
class _GraphData:
    entities: list[dict] = field(default_factory=list)
    relations: list[dict] = field(default_factory=list)
    chunks: list[NodeWithScore] = field(default_factory=list)


@dataclass
class StubGraphRetriever:
    responses: list[_GraphData] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    async def aretrieve(self, query: str) -> _GraphData:
        self.calls.append(query)
        if not self.responses:
            return _GraphData()
        return self.responses.pop(0)


@dataclass
class StubJudge:
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


def test_merge_graph_dedupes_entities_by_name() -> None:
    acc = {"entities": [{"entity_name": "X"}], "relations": []}
    fresh_ents = [{"entity_name": "X"}, {"entity_name": "Y"}]
    merged, ne, nr = _merge_graph(acc, fresh_ents, [])
    assert [e["entity_name"] for e in merged["entities"]] == ["X", "Y"]
    assert ne == 1
    assert nr == 0


def test_merge_graph_dedupes_relations_by_endpoints_and_label() -> None:
    acc = {
        "entities": [],
        "relations": [{"src_id": "A", "tgt_id": "B", "label": "L1"}],
    }
    fresh_rels = [
        {"src_id": "A", "tgt_id": "B", "label": "L1"},  # dup
        {"src_id": "A", "tgt_id": "B", "label": "L2"},  # new (different label)
        {"src_id": "B", "tgt_id": "C", "label": "L1"},  # new
    ]
    merged, _, nr = _merge_graph(acc, [], fresh_rels)
    assert nr == 2
    assert len(merged["relations"]) == 3


def test_accumulated_hl_keywords_picks_top_n_by_first_occurrence() -> None:
    graph = {
        "entities": [
            {"entity_name": "Alpha"},
            {"entity_name": "Beta"},
            {"entity_name": ""},
            {"entity_name": "Alpha"},  # dup
            {"entity_name": "Gamma"},
        ],
        "relations": [],
    }
    assert _accumulated_hl_keywords(graph, limit=2) == ["Alpha", "Beta"]
    assert _accumulated_hl_keywords(graph) == ["Alpha", "Beta", "Gamma"]


# ── end-to-end tests with graph retriever ────────────────────────────


@pytest.mark.asyncio
async def test_two_round_with_graph_retriever() -> None:
    retriever = StubRetriever(responses=[
        [_node("c1")],
        [_node("c2")],
    ])
    graph = StubGraphRetriever(responses=[
        _GraphData(
            entities=[{"entity_name": "Alpha", "entity_type": "Organization"}],
            relations=[{"src_id": "Alpha", "tgt_id": "INN-1", "label": "TAX_ID_OF"}],
            chunks=[],
        ),
        _GraphData(
            entities=[{"entity_name": "Beta", "entity_type": "Organization"}],
            relations=[],
            chunks=[],
        ),
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "next", "reason": "more"},
        {"sufficient": True, "follow_up_query": "", "reason": ""},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3, graph_retriever=graph,
    )

    assert result.agentic_rounds == 2
    stats = result.agentic_round_stats
    assert stats is not None
    assert stats[0].new_entities == 1
    assert stats[0].new_relations == 1
    assert stats[1].new_entities == 1
    assert stats[1].new_relations == 0


@pytest.mark.asyncio
async def test_early_exit_when_no_new_sources_and_no_new_graph() -> None:
    same_nodes = [_node("c1")]
    same_graph = _GraphData(
        entities=[{"entity_name": "Alpha"}],
        relations=[],
    )
    retriever = StubRetriever(responses=[list(same_nodes), list(same_nodes)])
    graph = StubGraphRetriever(responses=[same_graph, same_graph])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q2", "reason": "r"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q1", max_rounds=3, graph_retriever=graph,
    )

    assert result.agentic_rounds == 2
    assert len(judge.calls) == 1  # second judge skipped
    assert result.agentic_round_stats[1].sufficient is None
    assert result.agentic_round_stats[1].judge_reason == "no new info"


@pytest.mark.asyncio
async def test_early_exit_does_not_fire_when_graph_grows() -> None:
    """Sources frozen but graph adds new entity → judge still runs."""
    same_nodes = [_node("c1")]
    retriever = StubRetriever(responses=[list(same_nodes), list(same_nodes)])
    graph = StubGraphRetriever(responses=[
        _GraphData(entities=[{"entity_name": "X"}]),
        _GraphData(entities=[{"entity_name": "Y"}]),  # new entity
    ])
    judge = StubJudge(replies=[
        {"sufficient": False, "follow_up_query": "q2", "reason": "r"},
        {"sufficient": True, "follow_up_query": "", "reason": "ok"},
    ])
    synth = StubSynthesizer()

    result = await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q1", max_rounds=3, graph_retriever=graph,
    )

    assert result.agentic_rounds == 2
    assert len(judge.calls) == 2  # judge called BOTH rounds


@pytest.mark.asyncio
async def test_graph_chunks_fold_into_final_synthesis() -> None:
    retriever = StubRetriever(responses=[[_node("c1")]])
    graph = StubGraphRetriever(responses=[
        _GraphData(chunks=[_node("graph-chunk-1")]),
    ])
    judge = StubJudge(replies=[
        {"sufficient": True, "follow_up_query": "", "reason": ""},
    ])
    synth = StubSynthesizer()

    await agentic_search(
        retriever=retriever, judge=judge, synthesizer=synth,
        query="q", max_rounds=3, graph_retriever=graph,
    )

    assert synth.calls[0]["n_nodes"] == 2  # vector + graph chunk

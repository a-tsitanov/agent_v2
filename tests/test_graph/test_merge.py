"""Tests for `src/graph/merge.py:merge_kg_extraction`."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from llama_index.core.base.llms.types import ChatMessage
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
    EntityNode,
    Relation,
)
from llama_index.core.schema import TextNode

from src.graph.merge import merge_kg_extraction


@dataclass
class _StubLLM:
    """Echoes a canned summary string; counts calls."""

    summary: str = "SUMMARY"
    calls: list[list[ChatMessage]] = field(default_factory=list)

    async def achat(self, messages: list[ChatMessage], **_) -> object:
        self.calls.append(messages)

        class _Resp:
            class _Msg:
                content = self.summary

            message = _Msg()

        return _Resp()


def _chunk(chunk_id: str, entities: list[EntityNode],
           relations: list[Relation]) -> TextNode:
    node = TextNode(id_=chunk_id, text=f"chunk {chunk_id}")
    node.metadata[KG_NODES_KEY] = entities
    node.metadata[KG_RELATIONS_KEY] = relations
    return node


def _ent(name: str, label: str, desc: str,
         source_chunk_id: str | None = None) -> EntityNode:
    return EntityNode(
        name=name, label=label,
        properties={
            "description": desc,
            **({"source_chunk_id": source_chunk_id} if source_chunk_id else {}),
        },
    )


def _rel(src: EntityNode, tgt: EntityNode, label: str,
         desc: str, keywords: str = "", weight: float = 1.0) -> Relation:
    return Relation(
        label=label, source_id=src.id, target_id=tgt.id,
        properties={
            "description": desc,
            "keywords": keywords,
            "weight": weight,
        },
    )


# ── single occurrence: pass-through ─────────────────────────────────


@pytest.mark.asyncio
async def test_single_chunk_no_summary_call() -> None:
    e = _ent("Alpha", "Concept", "Only mention.")
    llm = _StubLLM()
    ents, rels = await merge_kg_extraction(
        [_chunk("c1", [e], [])], llm,
    )
    assert len(ents) == 1
    assert ents[0].name == "Alpha"
    assert ents[0].properties["description"] == "Only mention."
    assert ents[0].properties["mention_count"] == 1
    assert llm.calls == []  # no summary call


# ── small batch: concat, no LLM call ────────────────────────────────


@pytest.mark.asyncio
async def test_small_batch_concatenates() -> None:
    e1 = _ent("Alpha", "Concept", "First mention.")
    e2 = _ent("Alpha", "Concept", "Second mention.")
    e3 = _ent("Alpha", "Concept", "Third mention.")
    llm = _StubLLM()
    ents, _ = await merge_kg_extraction(
        [_chunk("c1", [e1], []),
         _chunk("c2", [e2], []),
         _chunk("c3", [e3], [])],
        llm,
    )
    assert len(ents) == 1
    desc = ents[0].properties["description"]
    assert "First mention." in desc
    assert "Second mention." in desc
    assert "Third mention." in desc
    assert "\n---\n" in desc
    assert ents[0].properties["mention_count"] == 3
    assert llm.calls == []


# ── large batch by count → LLM summary ──────────────────────────────


@pytest.mark.asyncio
async def test_summary_triggered_by_count() -> None:
    # 9 chunks each mentioning Alpha — force_count default is 8.
    chunks = [
        _chunk(f"c{i}", [_ent("Alpha", "Concept", f"Mention {i}.")], [])
        for i in range(9)
    ]
    llm = _StubLLM(summary="Consolidated Alpha description.")
    ents, _ = await merge_kg_extraction(chunks, llm)
    assert len(ents) == 1
    assert ents[0].properties["description"] == "Consolidated Alpha description."
    assert ents[0].properties["mention_count"] == 9
    assert len(llm.calls) == 1


# ── large batch by chars → LLM summary ──────────────────────────────


@pytest.mark.asyncio
async def test_summary_triggered_by_char_threshold() -> None:
    long_desc = "X" * 5000  # 5KB per mention × 3 mentions = 15KB > 12K
    chunks = [
        _chunk(f"c{i}", [_ent("Alpha", "Concept", f"{long_desc}-{i}")], [])
        for i in range(3)
    ]
    llm = _StubLLM(summary="One concise summary.")
    ents, _ = await merge_kg_extraction(chunks, llm)
    assert ents[0].properties["description"] == "One concise summary."
    assert len(llm.calls) == 1


# ── majority-vote on entity type ────────────────────────────────────


@pytest.mark.asyncio
async def test_majority_type_vote() -> None:
    # 3 chunks: Concept, Concept, RiskFactor → majority Concept.
    chunks = [
        _chunk("c1", [_ent("UV", "Concept", "a.")], []),
        _chunk("c2", [_ent("UV", "Concept", "b.")], []),
        _chunk("c3", [_ent("UV", "Other", "c.")], []),
    ]
    ents, _ = await merge_kg_extraction(chunks, _StubLLM())
    assert ents[0].label == "Concept"


# ── relation merge by undirected pair ───────────────────────────────


@pytest.mark.asyncio
async def test_relation_merge_undirected_pair() -> None:
    # Two chunks express the same relation with src/tgt swapped.
    a1 = _ent("A", "Concept", "a.")
    b1 = _ent("B", "Concept", "b.")
    r1 = _rel(a1, b1, "CAUSATION", "A causes B.", keywords="causation")

    a2 = _ent("A", "Concept", "a.")
    b2 = _ent("B", "Concept", "b.")
    r2 = _rel(b2, a2, "CAUSATION", "B is caused by A.",
              keywords="causation, risk_factor")

    chunks = [_chunk("c1", [a1, b1], [r1]),
              _chunk("c2", [a2, b2], [r2])]
    ents, rels = await merge_kg_extraction(chunks, _StubLLM())
    assert len(ents) == 2          # A and B merged once
    assert len(rels) == 1          # (A,B) and (B,A) merged into one
    merged = rels[0]
    assert merged.label == "CAUSATION"
    assert "causation" in merged.properties["keywords"]
    assert "risk_factor" in merged.properties["keywords"]
    assert merged.properties["mention_count"] == 2
    assert "A causes B." in merged.properties["description"]
    assert "B is caused by A." in merged.properties["description"]


# ── relation with missing endpoint dropped ───────────────────────────


@pytest.mark.asyncio
async def test_relation_skipped_if_endpoint_missing_post_merge() -> None:
    a = _ent("A", "Concept", "a.")
    b = _ent("B", "Concept", "b.")
    r = _rel(a, b, "CAUSATION", "A causes B.")
    # Same relation, but the second chunk's relation references an
    # entity NOT in that chunk's KG_NODES_KEY — should be skipped.
    a2 = _ent("A", "Concept", "second.")
    rogue_rel = Relation(
        label="X", source_id=a2.id, target_id="unknown-id",
        properties={"description": "Rogue."},
    )
    chunks = [_chunk("c1", [a, b], [r]),
              _chunk("c2", [a2], [rogue_rel])]
    _, rels = await merge_kg_extraction(chunks, _StubLLM())
    # Only the valid A-B relation survives.
    assert len(rels) == 1


# ── empty descriptions handled gracefully ──────────────────────────


@pytest.mark.asyncio
async def test_empty_descriptions() -> None:
    e1 = _ent("Alpha", "Concept", "")
    e2 = _ent("Alpha", "Concept", "")
    ents, _ = await merge_kg_extraction(
        [_chunk("c1", [e1], []), _chunk("c2", [e2], [])],
        _StubLLM(),
    )
    assert ents[0].properties["description"] == ""


# ── source_chunks tracking ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_source_chunks_deduped() -> None:
    e1 = _ent("Alpha", "Concept", "x.")
    e2 = _ent("Alpha", "Concept", "y.")
    ents, _ = await merge_kg_extraction(
        [_chunk("c1", [e1], []), _chunk("c2", [e2], [])],
        _StubLLM(),
    )
    assert ents[0].properties["source_chunks"] == ["c1", "c2"]

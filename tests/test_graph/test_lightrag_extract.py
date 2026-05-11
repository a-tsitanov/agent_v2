"""Tests for `src/graph/lightrag_extract.py:LightRAGExtractor`."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
    EntityNode,
)
from llama_index.core.schema import TextNode

from src.graph.lightrag_extract import LightRAGExtractor
from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM


# ── stub LLM ────────────────────────────────────────────────────────


@dataclass
class _ScriptedLLM:
    """Plays back canned responses in order.  Records every call."""

    responses: list[str]
    calls: list[list[ChatMessage]] = field(default_factory=list)

    async def achat(self, messages: list[ChatMessage], **_) -> object:
        self.calls.append(messages)
        text = self.responses.pop(0) if self.responses else ""

        class _Resp:
            class _Msg:
                content = text

            message = _Msg()

        return _Resp()


def _build_payload(entities: list[tuple[str, str, str]],
                   relations: list[tuple[str, str, str, str]]) -> str:
    lines = []
    for name, etype, desc in entities:
        lines.append(TUPLE_DELIM.join(["entity", name, etype, desc]))
    for src, tgt, kw, desc in relations:
        lines.append(TUPLE_DELIM.join(["relation", src, tgt, kw, desc]))
    lines.append(COMPLETE_DELIM)
    return "\n".join(lines)


# ── tests ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_single_chunk_extraction() -> None:
    payload = _build_payload(
        entities=[
            ("Basal Cell Carcinoma", "Concept", "Most common skin cancer."),
            ("UV Radiation", "Other", "Sunlight UV damages DNA."),
        ],
        relations=[
            ("UV Radiation", "Basal Cell Carcinoma",
             "causation, risk_factor", "UV is a primary risk factor for BCC."),
        ],
    )
    llm = _ScriptedLLM(responses=[payload])
    extractor = LightRAGExtractor(llm=llm, num_workers=1)

    node = TextNode(id_="c1", text="dummy chunk text",
                    metadata={"file_path": "med.txt"})
    out = await extractor.acall([node])

    ents = out[0].metadata[KG_NODES_KEY]
    rels = out[0].metadata[KG_RELATIONS_KEY]
    assert len(ents) == 2
    assert {e.name for e in ents} == {"Basal Cell Carcinoma", "Uv Radiation"}
    assert all(isinstance(e, EntityNode) for e in ents)
    assert ents[0].properties["source_chunk_id"] == "c1"
    assert ents[0].properties["file_path"] == "med.txt"

    assert len(rels) == 1
    assert rels[0].label == "CAUSATION"
    assert rels[0].properties["weight"] == 1.0


@pytest.mark.asyncio
async def test_gleaning_pass_adds_missed_entities() -> None:
    initial = _build_payload(
        entities=[("Alpha", "Concept", "First.")],
        relations=[],
    )
    glean = _build_payload(
        entities=[("Beta", "Concept", "Missed on first pass.")],
        relations=[],
    )
    llm = _ScriptedLLM(responses=[initial, glean])
    extractor = LightRAGExtractor(llm=llm, num_workers=1, gleaning_passes=1)

    node = TextNode(id_="c2", text="text")
    out = await extractor.acall([node])
    ents = out[0].metadata[KG_NODES_KEY]
    assert {e.name for e in ents} == {"Alpha", "Beta"}
    # 2 LLM calls happened: initial + gleaning
    assert len(llm.calls) == 2
    # Gleaning includes the prior conversation history (system, user,
    # assistant, plus the new gleaning user message = 4 messages)
    assert len(llm.calls[1]) == 4
    assert llm.calls[1][0].role == MessageRole.SYSTEM
    assert llm.calls[1][2].role == MessageRole.ASSISTANT


@pytest.mark.asyncio
async def test_gleaning_dedups_against_initial() -> None:
    """Gleaning must not double-count entities already extracted."""
    initial = _build_payload(
        entities=[("Alpha", "Concept", "First."), ("Beta", "Concept", "Second.")],
        relations=[],
    )
    glean = _build_payload(
        entities=[("Alpha", "Concept", "Repeated."), ("Gamma", "Concept", "New.")],
        relations=[],
    )
    extractor = LightRAGExtractor(
        llm=_ScriptedLLM(responses=[initial, glean]),
        num_workers=1, gleaning_passes=1,
    )
    out = await extractor.acall([TextNode(id_="c3", text="t")])
    ents = out[0].metadata[KG_NODES_KEY]
    assert {e.name for e in ents} == {"Alpha", "Beta", "Gamma"}


@pytest.mark.asyncio
async def test_llm_failure_returns_empty_metadata() -> None:
    class _RaisingLLM:
        async def achat(self, *a, **kw):
            raise RuntimeError("boom")

    extractor = LightRAGExtractor(llm=_RaisingLLM(), num_workers=1)
    out = await extractor.acall([TextNode(id_="c4", text="t")])
    assert out[0].metadata[KG_NODES_KEY] == []
    assert out[0].metadata[KG_RELATIONS_KEY] == []


@pytest.mark.asyncio
async def test_orphan_endpoint_synthesises_entity() -> None:
    """LightRAG-style: if a relation references an endpoint the
    model didn't list as an entity, we synthesise an Other entity
    so the edge survives."""
    payload = _build_payload(
        entities=[("Alpha", "Concept", "Only Alpha is declared.")],
        relations=[
            ("Alpha", "Ghost", "mentions", "Alpha mentions Ghost."),
        ],
    )
    extractor = LightRAGExtractor(
        llm=_ScriptedLLM(responses=[payload]), num_workers=1,
    )
    out = await extractor.acall([TextNode(id_="c5", text="t")])
    ents = out[0].metadata[KG_NODES_KEY]
    rels = out[0].metadata[KG_RELATIONS_KEY]
    assert {e.name for e in ents} == {"Alpha", "Ghost"}
    ghost = next(e for e in ents if e.name == "Ghost")
    assert ghost.label == "Other"
    assert ghost.properties["orphan"] is True
    assert len(rels) == 1


@pytest.mark.asyncio
async def test_multi_chunk_parallel_extraction() -> None:
    """Verify multiple chunks run through the extractor cleanly."""
    payloads = [
        _build_payload(entities=[(f"E{i}", "Concept", f"d{i}.")], relations=[])
        for i in range(3)
    ]
    extractor = LightRAGExtractor(
        llm=_ScriptedLLM(responses=payloads), num_workers=2,
    )
    nodes = [TextNode(id_=f"c{i}", text=f"chunk {i}") for i in range(3)]
    out = await extractor.acall(nodes)
    all_names = {e.name for n in out for e in n.metadata[KG_NODES_KEY]}
    assert all_names == {"E0", "E1", "E2"}

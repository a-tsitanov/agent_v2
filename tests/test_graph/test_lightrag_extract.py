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

from src.graph.lightrag_extract import LightRAGExtractor, _extraction_text
from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM
from src.ingestion.identifier_transform import _AUGMENT_METADATA_KEY


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
async def test_reads_translated_text_when_present() -> None:
    """When `IngestionPipeline` runs `TranslateToRussianTransform`,
    each chunk carries `node.metadata['translated_text']` (Russian).
    LightRAGExtractor must feed THAT into the LLM, not the original
    `node.text`."""
    captured: dict[str, str] = {}

    @dataclass
    class _SpyLLM:
        responses: list[str]

        async def achat(self, messages: list[ChatMessage], **_) -> object:
            # Capture what the extractor sent so the test can assert
            # it pulled from translated_text.
            captured["user"] = messages[-1].content or ""
            text = self.responses.pop(0) if self.responses else ""

            class _R:
                class _M:
                    content = text

                message = _M()

            return _R()

    payload = _build_payload(
        entities=[("Foo", "Concept", "Some russian description.")],
        relations=[],
    )
    extractor = LightRAGExtractor(llm=_SpyLLM(responses=[payload]), num_workers=1)
    node = TextNode(id_="c-tr", text="ORIGINAL ENGLISH TEXT")
    node.metadata["translated_text"] = "RU_TRANSLATED_BODY"
    await extractor.acall([node])
    assert "RU_TRANSLATED_BODY" in captured["user"]
    assert "ORIGINAL ENGLISH TEXT" not in captured["user"]


@pytest.mark.asyncio
async def test_falls_back_to_node_text_without_translation() -> None:
    """Without `translated_text` metadata (e.g. translation off, or
    chunk skipped) extractor must read `node.text` as before."""
    captured: dict[str, str] = {}

    @dataclass
    class _SpyLLM:
        responses: list[str]

        async def achat(self, messages: list[ChatMessage], **_) -> object:
            captured["user"] = messages[-1].content or ""
            text = self.responses.pop(0) if self.responses else ""

            class _R:
                class _M:
                    content = text

                message = _M()

            return _R()

    payload = _build_payload(
        entities=[("X", "Concept", "d.")],
        relations=[],
    )
    extractor = LightRAGExtractor(llm=_SpyLLM(responses=[payload]), num_workers=1)
    node = TextNode(id_="c-noTrans", text="english only")
    await extractor.acall([node])
    assert "english only" in captured["user"]


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


# ── _extraction_text: canonical augment on both paths ───────────────


def test_extraction_text_appends_augment_on_translated_path() -> None:
    """When translated_text is set, the augment block must be appended
    even though it is absent from translated_text itself.  This ensures
    the KG-extraction LLM receives the canonical-identifier nudge on the
    common (translated) code path."""
    augment = "Канонические идентификаторы: +79001234567 (PHONE)"
    node = TextNode(id_="t1", text="ORIGINAL ENGLISH TEXT")
    node.metadata["translated_text"] = "RU_TRANSLATED_BODY"
    node.metadata[_AUGMENT_METADATA_KEY] = augment

    result = _extraction_text(node)

    assert "RU_TRANSLATED_BODY" in result
    assert augment in result
    assert "ORIGINAL ENGLISH TEXT" not in result


def test_extraction_text_no_duplication_on_llm_metadata_path() -> None:
    """When there is no translated_text, _extraction_text falls back to
    get_content(MetadataMode.LLM) — which already includes the augment
    block via llm-visible metadata.  The augment must appear exactly once
    (no double-inclusion)."""
    from llama_index.core.schema import MetadataMode

    augment = "Канонические идентификаторы: 7707083893 (INN)"
    node = TextNode(id_="t2", text="some chunk text")
    node.metadata[_AUGMENT_METADATA_KEY] = augment
    # Make the augment appear in MetadataMode.LLM output by NOT adding it to
    # excluded_llm_metadata_keys (mirroring production behaviour from
    # IdentifierCanonicalizationTransform._exclude_augment_from_embed).
    # Verify the LLM content already contains the augment before asserting
    # that _extraction_text doesn't double-include it.
    llm_content = node.get_content(metadata_mode=MetadataMode.LLM)
    assert augment in llm_content, (
        "pre-condition: LLM metadata view must already include the augment"
    )

    result = _extraction_text(node)

    assert result.count(augment) == 1


def test_extraction_text_no_augment_unchanged() -> None:
    """When no augment key is present, output equals the base chunk text."""
    node = TextNode(id_="t3", text="plain chunk")
    result = _extraction_text(node)
    assert "plain chunk" in result
    assert _AUGMENT_METADATA_KEY not in result

"""Tests for `EntityDescriptionEnricher`."""

from __future__ import annotations

from llama_index.core.base.llms.types import ChatMessage, ChatResponse
from llama_index.core.graph_stores.types import KG_NODES_KEY, EntityNode
from llama_index.core.schema import TextNode

from src.graph.enrich import EntityDescriptionEnricher


class _ScriptedLLM:
    """Returns descriptions from a name → text dict."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping
        self.calls: list[str] = []

    async def achat(self, messages):
        user_content = messages[1].content
        # Match on the prompt's leading "Entity: <name>" line so two
        # entities in the same source text don't collide.
        first_line = user_content.split("\n", 1)[0]
        for name, desc in self.mapping.items():
            if first_line.endswith(name):
                self.calls.append(name)
                return ChatResponse(
                    message=ChatMessage(role="assistant", content=desc)
                )
        return ChatResponse(
            message=ChatMessage(role="assistant", content="NO_INFO")
        )


def _node_with_entities(text: str, entity_names: list[str]) -> TextNode:
    node = TextNode(text=text, metadata={"doc_id": "doc-1"})
    node.metadata[KG_NODES_KEY] = [
        EntityNode(name=n, label="entity") for n in entity_names
    ]
    return node


def test_enricher_writes_descriptions_into_entity_properties() -> None:
    llm = _ScriptedLLM({
        "Alice": "Alice is the project lead overseeing onboarding redesign.",
        "Bob": "Bob is the customer who reported the bug.",
    })
    enricher = EntityDescriptionEnricher(llm=llm)  # type: ignore[arg-type]

    node = _node_with_entities(
        "Alice led the team. Bob filed a ticket.",
        ["Alice", "Bob"],
    )
    out = enricher([node])

    descs = {
        e.name: (e.properties or {}).get("description")
        for e in out[0].metadata[KG_NODES_KEY]
    }
    assert "project lead" in descs["Alice"]
    assert "customer" in descs["Bob"]


def test_enricher_skips_entities_with_existing_description() -> None:
    llm = _ScriptedLLM({"X": "should not be called"})
    node = TextNode(text="some text", metadata={"doc_id": "d"})
    pre_described = EntityNode(
        name="X", label="entity",
        properties={"description": "already filled"},
    )
    node.metadata[KG_NODES_KEY] = [pre_described]

    enricher = EntityDescriptionEnricher(llm=llm)  # type: ignore[arg-type]
    enricher([node])

    assert llm.calls == []
    assert node.metadata[KG_NODES_KEY][0].properties["description"] == "already filled"


def test_enricher_handles_no_info_response() -> None:
    llm = _ScriptedLLM({})  # everything → NO_INFO
    node = _node_with_entities("text", ["Mystery"])

    enricher = EntityDescriptionEnricher(llm=llm)  # type: ignore[arg-type]
    out = enricher([node])

    ent = out[0].metadata[KG_NODES_KEY][0]
    desc = (ent.properties or {}).get("description")
    assert not desc


def test_enricher_dedupes_within_a_chunk() -> None:
    llm = _ScriptedLLM({"Alpha": "Alpha is an example concept."})
    node = _node_with_entities("Alpha is mentioned twice.", ["Alpha", "Alpha"])

    enricher = EntityDescriptionEnricher(llm=llm)  # type: ignore[arg-type]
    enricher([node])

    assert llm.calls == ["Alpha"]

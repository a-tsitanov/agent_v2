"""Tests for entity-description handling.

Two layers covered:
  * Canonical identifier injection — `inject_canonical_entities`
    now writes a real text snippet around the identifier span
    (±80 chars) instead of a templated string.  Verified by
    feeding text + spans through `IdentifierCanonicalizationTransform`
    → `inject_canonical_entities` → checking properties["description"].
  * Schema-mode extractor declares the description property to the
    LLM (covered in `test_index.py`).
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode
from llama_index.core.schema import TextNode

from src.ingestion.identifier_transform import (
    IdentifierCanonicalizationTransform,
    inject_canonical_entities,
)


class _FakeGraphStore:
    """In-memory graph_store stub capturing upserted nodes."""

    supports_vector_queries = False
    supports_structured_queries = False

    def __init__(self) -> None:
        self.upserted: list[EntityNode] = []

    def upsert_nodes(self, nodes):
        self.upserted.extend(nodes)


def test_inject_uses_text_snippet_as_description() -> None:
    text = (
        "Договор поставки № ДП-2024/178-К заключён между "
        "ООО «Северные технологии» (ИНН 7707083893) и АО «Промсервис». "
        "Контакт: +7 (495) 234-56-78."
    )
    nodes = IdentifierCanonicalizationTransform()(
        [TextNode(text=text, metadata={"doc_id": "doc-1"})]
    )

    store = _FakeGraphStore()
    inject_canonical_entities(store, nodes)

    desc_by_canonical = {e.name: e.properties["description"] for e in store.upserted}

    # INN ноды: description должен включать «ИНН 7707083893» или
    # фрагмент со словом «Северные технологии».
    assert "7707083893" in desc_by_canonical
    inn_desc = desc_by_canonical["7707083893"]
    assert "Северные технологии" in inn_desc or "ИНН 7707083893" in inn_desc
    # Не должна остаться шаблонная строка
    assert "extracted from" not in inn_desc

    # Phone ноды: description должен содержать сам телефон в исходном
    # написании.
    assert "+74952345678" in desc_by_canonical
    phone_desc = desc_by_canonical["+74952345678"]
    assert "234-56-78" in phone_desc


def test_inject_falls_back_to_template_when_span_missing() -> None:
    """If the identifier dict has no `span` field, description is
    the legacy templated string — a safety net so the contract
    never returns empty descriptions."""
    from llama_index.core.schema import TextNode

    fake_node = TextNode(text="some text", metadata={"doc_id": "doc-x"})
    fake_node.metadata["canonical_identifiers"] = [
        {
            "entity_type": "INN",
            "canonical": "7707083893",
            "original": "7707083893",
            "span": None,
        }
    ]
    store = _FakeGraphStore()
    inject_canonical_entities(store, [fake_node])

    assert len(store.upserted) == 1
    desc = store.upserted[0].properties["description"]
    assert "INN" in desc and "doc-x" in desc

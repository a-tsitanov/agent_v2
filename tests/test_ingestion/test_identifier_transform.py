"""Stage-7 tests for the LlamaIndex transform + graph injection.

Uses an in-memory stub graph store so the suite stays hermetic.
The transform itself is a pure function over Node objects so tests
exercise it with hand-built TextNodes.
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode
from llama_index.core.schema import TextNode

from src.ingestion.identifier_transform import (
    IdentifierCanonicalizationTransform,
    inject_canonical_entities,
)

SBER_INN = "7707083893"


def _node(text: str, doc_id: str = "doc-1") -> TextNode:
    return TextNode(text=text, metadata={"doc_id": doc_id})


def test_transform_attaches_canonical_metadata_and_augments_text() -> None:
    nodes = [
        _node(
            "Поставщик ООО «Тест» (ИНН 7707083893), "
            "тел. +7 (495) 234-56-78."
        )
    ]
    transformed = IdentifierCanonicalizationTransform()(nodes)
    assert len(transformed) == 1

    md_idents = transformed[0].metadata["canonical_identifiers"]
    types = {i["entity_type"] for i in md_idents}
    canon = {i["canonical"] for i in md_idents}

    assert "PhoneNumber" in types
    assert "INN" in types
    assert SBER_INN in canon
    assert "+74952345678" in canon

    # text now carries the augment block — Stage-6 LLM extractor will
    # see canonical names in-band
    assert "Канонические идентификаторы" in transformed[0].get_content()


def test_transform_no_op_on_text_without_identifiers() -> None:
    n = _node("Просто абзац без структурных данных.")
    transformed = IdentifierCanonicalizationTransform()([n])
    assert "canonical_identifiers" not in transformed[0].metadata
    assert "Канонические идентификаторы" not in transformed[0].get_content()


# ── injection ────────────────────────────────────────────────────────


class _FakeGraphStore:
    """Captures upserted nodes so tests can assert."""

    supports_vector_queries = False
    supports_structured_queries = False

    def __init__(self) -> None:
        self.upserted: list[EntityNode] = []

    def upsert_nodes(self, nodes):
        self.upserted.extend(nodes)


def test_inject_canonical_entities_dedupes_across_nodes() -> None:
    store = _FakeGraphStore()
    transform = IdentifierCanonicalizationTransform()
    nodes = transform([
        _node(f"ИНН {SBER_INN}, тел +7 (495) 234-56-78"),
        _node(
            f"Дублирующая запись: ИНН {SBER_INN}, "
            "новый телефон 8 921 100-20-30"
        ),
    ])

    n_inserted = inject_canonical_entities(store, nodes)
    # one INN dedup'd, two phones (one E.164, one different number)
    upserted_names = {e.name for e in store.upserted}
    assert SBER_INN in upserted_names
    assert "+74952345678" in upserted_names
    assert "+79211002030" in upserted_names
    assert n_inserted == len(store.upserted)


def test_inject_canonical_entities_handles_missing_metadata() -> None:
    """Nodes that the transform skipped (no identifiers) must not
    crash injection."""
    store = _FakeGraphStore()
    plain = _node("plain text, no identifiers")
    n = inject_canonical_entities(store, [plain])
    assert n == 0
    assert store.upserted == []


def test_transform_pluggable_into_ingestion_pipeline() -> None:
    """Stage 2's pipeline factory accepts ``extra_transformations``;
    confirm the canonicalizer composes with the default
    sentence-splitter without surprises."""
    from llama_index.core import Document

    from src.ingestion.pipeline import build_ingestion_pipeline

    docs = [
        Document(
            text=(
                "Договор № ДП-2024/178-К от 15.03.2024. "
                "ИНН 7707083893. Тел: +7 495 234-56-78."
            ),
            metadata={"file_path": "contract.txt"},
        ),
    ]
    pipeline = build_ingestion_pipeline(
        extra_transformations=[IdentifierCanonicalizationTransform()],
    )
    out = pipeline.run(documents=docs)
    assert len(out) >= 1
    enriched = next(
        n for n in out if "canonical_identifiers" in n.metadata
    )
    canon_set = {
        i["canonical"]
        for i in enriched.metadata["canonical_identifiers"]
    }
    assert "ДП-2024/178-К" in canon_set
    assert SBER_INN in canon_set
    assert "+74952345678" in canon_set

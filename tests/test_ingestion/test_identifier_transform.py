"""Stage-7 tests for the LlamaIndex transform + graph injection.

Uses an in-memory stub graph store so the suite stays hermetic.
The transform itself is a pure function over Node objects so tests
exercise it with hand-built TextNodes.
"""

from __future__ import annotations

from llama_index.core.graph_stores.types import EntityNode
from llama_index.core.schema import MetadataMode, TextNode

from src.ingestion.identifier_transform import (
    _AUGMENT_METADATA_KEY,
    IdentifierCanonicalizationTransform,
    inject_canonical_entities,
)

SBER_INN = "7707083893"
# Phrase that appears only inside the LLM-instruction augment block.
_AUGMENT_INSTRUCTION = "используй ИМЕННО ТАКУЮ форму"
_AUGMENT_HEADER = "Канонические идентификаторы"


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

    # The augment block now lives in a dedicated metadata key, NOT in
    # the node's own text (so it can't pollute embeddings / Milvus text
    # / citations).
    node = transformed[0]
    assert _AUGMENT_HEADER not in node.get_content(metadata_mode=MetadataMode.NONE)
    assert _AUGMENT_INSTRUCTION not in node.get_content(
        metadata_mode=MetadataMode.NONE,
    )
    assert _AUGMENT_HEADER in node.metadata[_AUGMENT_METADATA_KEY]


def test_augment_visible_to_llm_but_not_embeddings() -> None:
    """The KG extractor reads MetadataMode.LLM — the canonical block
    must be visible there.  Embeddings / stored text use
    MetadataMode.EMBED / NONE — the block must be absent there."""
    nodes = IdentifierCanonicalizationTransform()(
        [_node("Поставщик ООО «Тест» (ИНН 7707083893).")]
    )
    node = nodes[0]

    llm_view = node.get_content(metadata_mode=MetadataMode.LLM)
    embed_view = node.get_content(metadata_mode=MetadataMode.EMBED)
    none_view = node.get_content(metadata_mode=MetadataMode.NONE)

    # Visible to the LLM extractor (canonical forms reach the prompt).
    assert _AUGMENT_HEADER in llm_view
    assert _AUGMENT_INSTRUCTION in llm_view
    # Hidden from embeddings + raw stored text (no retrieval pollution).
    assert _AUGMENT_HEADER not in embed_view
    assert _AUGMENT_INSTRUCTION not in embed_view
    assert _AUGMENT_HEADER not in none_view
    # The exclusion is registered on the node for downstream consumers.
    assert _AUGMENT_METADATA_KEY in node.excluded_embed_metadata_keys
    assert _AUGMENT_METADATA_KEY not in node.excluded_llm_metadata_keys


def test_transform_is_idempotent() -> None:
    """Re-running the transform must not stack augment blocks, must not
    grow the stored text, and must keep metadata stable."""
    transform = IdentifierCanonicalizationTransform()
    text = "Поставщик ООО «Тест» (ИНН 7707083893), тел. +7 (495) 234-56-78."
    node = _node(text)

    transform([node])
    text_after_1 = node.get_content(metadata_mode=MetadataMode.NONE)
    idents_1 = node.metadata["canonical_identifiers"]
    augment_1 = node.metadata[_AUGMENT_METADATA_KEY]
    excl_1 = list(node.excluded_embed_metadata_keys)

    transform([node])
    text_after_2 = node.get_content(metadata_mode=MetadataMode.NONE)
    idents_2 = node.metadata["canonical_identifiers"]
    augment_2 = node.metadata[_AUGMENT_METADATA_KEY]
    excl_2 = list(node.excluded_embed_metadata_keys)

    # Raw stored text never mutated, so it cannot grow / stack.
    assert text_after_1 == text == text_after_2
    # Metadata identical across runs (no duplication / re-extraction).
    assert idents_1 == idents_2
    assert augment_1 == augment_2
    # Exclusion list does not accumulate duplicates.
    assert excl_1 == excl_2
    assert excl_2.count(_AUGMENT_METADATA_KEY) == 1


def test_transform_no_op_on_text_without_identifiers() -> None:
    n = _node("Просто абзац без структурных данных.")
    transformed = IdentifierCanonicalizationTransform()([n])
    assert "canonical_identifiers" not in transformed[0].metadata
    assert _AUGMENT_METADATA_KEY not in transformed[0].metadata
    assert _AUGMENT_HEADER not in transformed[0].get_content(
        metadata_mode=MetadataMode.NONE,
    )


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
        translate_to_russian=False,
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

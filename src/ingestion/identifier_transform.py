"""LlamaIndex ``TransformComponent`` for identifier canonicalization.

Inserted into ``IngestionPipeline`` BEFORE the property-graph
extractor (``SchemaLLMPathExtractor``).  For each chunk:

1. Runs the deterministic detectors from ``identifiers.py``.
2. Stores the canonical forms in ``node.metadata['canonical_identifiers']``
   so downstream stages (graph injection, debugging) can read them.
3. Appends a ``Канонические идентификаторы:`` block to the node text
   so the LLM extractor (Stage 6) receives the canonical strings
   in-band and uses them when constructing entity names.

A companion helper ``inject_canonical_entities(graph_store, nodes)``
pre-populates the property-graph store with canonical entity nodes
before ``SchemaLLMPathExtractor`` runs — guarantees that
``+74952345678``, ``7707083893`` etc. exist in Neo4j with the right
type even if the LLM later picks a verbatim form.
"""

from __future__ import annotations

from typing import Any

from llama_index.core.graph_stores.types import EntityNode, PropertyGraphStore
from llama_index.core.schema import BaseNode, TransformComponent

from src.ingestion.identifiers import (
    NormalizedIdentifier,
    build_augment_block,
    dedupe_by_canonical,
    extract_identifiers,
)


_METADATA_KEY = "canonical_identifiers"


def _ident_to_dict(ident: NormalizedIdentifier) -> dict[str, Any]:
    return {
        "entity_type": ident.entity_type,
        "canonical": ident.canonical,
        "original": ident.original,
        "span": list(ident.span),
    }


class IdentifierCanonicalizationTransform(TransformComponent):
    """Augment each chunk with canonical identifiers.

    Pure transformation — does NOT touch the graph store.  Injection
    is a separate step (``inject_canonical_entities``) so the
    transform stays callable in unit tests that don't care about
    Neo4j.
    """

    def __call__(
        self, nodes: list[BaseNode], **kwargs: Any,
    ) -> list[BaseNode]:
        for node in nodes:
            idents = extract_identifiers(node.get_content())
            if not idents:
                continue
            deduped = dedupe_by_canonical(idents)
            node.metadata[_METADATA_KEY] = [
                _ident_to_dict(i) for i in deduped
            ]
            node.set_content(node.get_content() + build_augment_block(idents))
        return nodes

    async def acall(
        self, nodes: list[BaseNode], **kwargs: Any,
    ) -> list[BaseNode]:
        # No async work to do — delegate.
        return self.__call__(nodes, **kwargs)


def _description_for(
    canonical: str, entity_type: str, original: str, doc_id: str,
) -> str:
    if original and original != canonical:
        return (
            f"{entity_type} extracted from {doc_id}; "
            f"canonical={canonical}; original={original!r}."
        )
    return f"{entity_type} extracted from {doc_id}; canonical={canonical}."


def _snippet_around(
    text: str, span: list[int] | tuple[int, int] | None,
    window: int = 80,
) -> str:
    """Cut ±`window` chars around the identifier span; collapse whitespace."""
    if not text or not span or len(span) < 2:
        return ""
    start = max(0, int(span[0]) - window)
    end = min(len(text), int(span[1]) + window)
    snippet = text[start:end].replace("\n", " ").strip()
    return " ".join(snippet.split())  # collapse runs of spaces


def inject_canonical_entities(
    graph_store: PropertyGraphStore,
    nodes: list[BaseNode],
) -> int:
    """Push canonical identifier nodes into the property-graph store.

    Reads `canonical_identifiers` placed on each node by
    `IdentifierCanonicalizationTransform` and upserts a deduplicated
    set of `EntityNode`.  Idempotent at the graph-store level —
    `upsert_nodes` merges by name.

    `description` on each entity is a real text snippet (±80 chars
    around the original mention) rather than a templated string —
    this is what makes the canonical node useful at query time and
    matches LightRAG's behaviour where every entity carries its
    source-context.

    Returns the count of unique entity nodes upserted.
    """
    seen: dict[tuple[str, str], EntityNode] = {}
    for node in nodes:
        idents = node.metadata.get(_METADATA_KEY) or []
        node_text = node.get_content() if hasattr(node, "get_content") else ""
        doc_id = (
            node.metadata.get("doc_id")
            or node.metadata.get("file_path")
            or ""
        )
        for ident in idents:
            key = (ident["entity_type"], ident["canonical"])
            if key in seen:
                continue
            snippet = _snippet_around(node_text, ident.get("span"))
            fallback_desc = _description_for(
                ident["canonical"],
                ident["entity_type"],
                ident.get("original", ""),
                str(doc_id),
            )
            description = snippet or fallback_desc
            seen[key] = EntityNode(
                name=ident["canonical"],
                label=ident["entity_type"],
                properties={
                    "source_id": doc_id,
                    "description": description,
                    "original": ident.get("original", ""),
                },
            )
    if seen:
        graph_store.upsert_nodes(list(seen.values()))
    return len(seen)


__all__ = [
    "IdentifierCanonicalizationTransform",
    "inject_canonical_entities",
]

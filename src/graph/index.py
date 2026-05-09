"""``PropertyGraphIndex`` factory + ``SchemaLLMPathExtractor`` wiring.

Stage 6 KG path:
  * Extractor — ``SchemaLLMPathExtractor`` with our typed entity /
    relation schemas (``src/graph/schema.py``).  Strict
    extraction → predictable Neo4j labels, easier downstream
    Cypher.
  * Store — Neo4j in production, SimplePropertyGraphStore in tests.
  * Index — composes both, attached to the IngestionPipeline as
    a sink alongside the vector index.

The extractor uses an LLM under the hood, so heavy under unit
tests.  Tests pass ``MockLLM`` to verify the wiring; live extraction
happens in the worker (Stage 8) against the real LiteLLM proxy.
"""

from __future__ import annotations

from typing import get_args

from llama_index.core import PropertyGraphIndex
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.core.indices.property_graph import SchemaLLMPathExtractor
from llama_index.core.llms import LLM

from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)


def build_kg_extractor(
    llm: LLM,
    *,
    strict: bool = True,
    num_workers: int = 2,
) -> SchemaLLMPathExtractor:
    """SchemaLLMPathExtractor over our typed entity/relation set.

    ``strict=True`` discards triples that don't fit
    ``DEFAULT_VALIDATION_SCHEMA`` — recommended for the production
    path where we want consistent Neo4j labels.  Set ``False`` for
    bring-up to inspect what the LLM proposes.
    """
    return SchemaLLMPathExtractor(
        llm=llm,
        possible_entities=list(get_args(EntityType)),  # type: ignore[arg-type]
        possible_relations=list(get_args(RelationType)),  # type: ignore[arg-type]
        kg_validation_schema=DEFAULT_VALIDATION_SCHEMA if strict else None,
        strict=strict,
        num_workers=num_workers,
    )


def build_property_graph_index(
    *,
    graph_store: PropertyGraphStore,
    embed_model: BaseEmbedding,
    extractor: SchemaLLMPathExtractor,
    nodes: list | None = None,
) -> PropertyGraphIndex:
    """Compose a PropertyGraphIndex from store + embed + extractor.

    Pass ``nodes`` to build from chunks (used in Stage 8 worker);
    omit to attach to an existing populated store.
    """
    if nodes is None:
        return PropertyGraphIndex.from_existing(
            property_graph_store=graph_store,
            embed_model=embed_model,
            kg_extractors=[extractor],
        )
    return PropertyGraphIndex(
        nodes=nodes,
        property_graph_store=graph_store,
        embed_model=embed_model,
        kg_extractors=[extractor],
        show_progress=False,
    )

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

from typing import Literal, get_args

from llama_index.core import PropertyGraphIndex
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.core.indices.property_graph import (
    SchemaLLMPathExtractor,
    SimpleLLMPathExtractor,
)
from llama_index.core.llms import LLM
from llama_index.core.schema import TransformComponent

from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)

KGExtractor = TransformComponent


ExtractorMode = Literal["simple", "schema"]


def build_kg_extractor(
    llm: LLM,
    *,
    mode: ExtractorMode = "simple",
    strict: bool = False,
    num_workers: int = 2,
    extract_prompt: str | None = None,
) -> KGExtractor:
    """Build a KG path extractor.

    Two modes:

    - ``simple`` (default) — ``SimpleLLMPathExtractor``.  Uses a
      plain prompt + regex parsing for triples.  Tolerant of
      small-model output (llama3.1:8b, qwen2.5:3b...).  Entity
      types end up as generic ``EntityNode``; relations as plain
      labels — the deterministic identifier transform in
      Stage 7 still gives us typed nodes for phones/INNs/etc.

    - ``schema`` — ``SchemaLLMPathExtractor`` over our typed
      ``EntityType``/``RelationType`` Literal unions.  Requires a
      function-calling-capable LLM (GPT-4-class, Claude, large
      Llama 3.1+).  Small models choke on the strict JSON schema —
      LlamaIndex's validator swallows ``TypeError`` from
      malformed triples and silently returns zero triples.  We
      discovered this empirically with llama3.1:8b; switch
      ``mode="schema"`` only with a stronger backend.

    ``strict`` only affects schema mode.

    ``extract_prompt`` overrides the default multilingual template
    (Simple mode only).  Pass a string template — see
    ``_MULTILINGUAL_TRIPLET_EXTRACT_PROMPT`` for the placeholder
    contract (must include ``{max_knowledge_triplets}`` and
    ``{text}``).  Use this to lock to a single language if the
    multilingual default leaks predicates across languages.
    """
    if mode == "schema":
        return SchemaLLMPathExtractor(
            llm=llm,
            possible_entities=list(get_args(EntityType)),  # type: ignore[arg-type]
            possible_relations=list(get_args(RelationType)),  # type: ignore[arg-type]
            kg_validation_schema=DEFAULT_VALIDATION_SCHEMA if strict else None,
            strict=strict,
            num_workers=num_workers,
        )
    return SimpleLLMPathExtractor(
        llm=llm,
        num_workers=num_workers,
        max_paths_per_chunk=10,
        extract_prompt=extract_prompt or _MULTILINGUAL_TRIPLET_EXTRACT_PROMPT,
    )


# Multilingual triplet prompt.  Instructions are in English (best
# small-model compliance), few-shot examples cover three languages
# so the model anchors on the «keep entity names in original
# language» pattern.  The stock LlamaIndex prompt only had English
# Alice/Bob/Philz which (a) broke RU/JA/DE input and (b) made
# llama3.1:8b sometimes echo "Subject/Predicate/Object" verbatim.
#
# To swap for a single-language workload, override
# ``extract_prompt=`` when calling ``build_kg_extractor`` (or pass
# ``SimpleLLMPathExtractor`` your own template directly).
_MULTILINGUAL_TRIPLET_EXTRACT_PROMPT = (
    "Extract up to {max_knowledge_triplets} knowledge triplets from the text below.\n"
    "Each triplet must be on its own line in the format: "
    "(subject, predicate, object)\n"
    "\n"
    "Rules:\n"
    "1. Subject and object MUST be concrete entities from the text "
    "(people, organizations, phone numbers, IDs, contract numbers, "
    "addresses, dates, amounts, locations, events, concepts).\n"
    "2. Predicate is a short verb phrase describing the relation.\n"
    "3. Keep entity names in the ORIGINAL language of the source text "
    "(do NOT translate company names, person names, addresses, etc.).\n"
    "4. The predicate itself can be in English OR in the source language "
    "— prefer the source language for readability.\n"
    "5. Do not invent entities not present in the text.\n"
    "6. Skip stop-words and pronouns as standalone subjects/objects.\n"
    "7. Do NOT output literal placeholders like \"Subject\"/\"Object\" — "
    "they are template markers, not values to copy.\n"
    "\n"
    "--- Examples covering multiple languages ---\n"
    "Text: Alice is Bob's mother and works at Acme Corp.\n"
    "Triplets:\n"
    "(Alice, is mother of, Bob)\n"
    "(Alice, works at, Acme Corp)\n"
    "\n"
    "Text: ООО Альфа заключило договор № 17-К с ИП Иванов на сумму 500000 руб.\n"
    "Triplets:\n"
    "(ООО Альфа, заключило договор, № 17-К)\n"
    "(№ 17-К, между, ИП Иванов)\n"
    "(№ 17-К, сумма, 500000 руб)\n"
    "\n"
    "Text: Die Firma Müller GmbH hat ihren Sitz in München und wurde 2010 gegründet.\n"
    "Triplets:\n"
    "(Müller GmbH, Sitz in, München)\n"
    "(Müller GmbH, gegründet in, 2010)\n"
    "\n"
    "--- Now extract from the following text ---\n"
    "Text: {text}\n"
    "Triplets:\n"
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

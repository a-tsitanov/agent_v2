"""`PropertyGraphIndex` factory and KG extractor wiring.

Layers:
  * **Extractor** — typed `SchemaLLMPathExtractor` (default) or the
    looser `SimpleLLMPathExtractor` (fallback).  Schema mode emits
    typed entities + a `description` property per entity — the
    foundation of LightRAG-style rich semantic graphs.
  * **Store** — Neo4j in production, `SimplePropertyGraphStore` in
    unit tests.
  * **Index** — composes both; attached to the ingestion pipeline
    alongside the vector index.

Both extractors run an LLM internally — unit tests stub or mock.
Live runs use the project LLM via LiteLLM proxy (qwen3:8b by
default, which reliably emits structured output for schema mode).
"""

from __future__ import annotations

from typing import Literal

from llama_index.core import PropertyGraphIndex
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.graph_stores.types import PropertyGraphStore
from llama_index.core.indices.property_graph import (
    SchemaLLMPathExtractor,
    SimpleLLMPathExtractor,
)
from llama_index.core.indices.property_graph.utils import (
    default_parse_triplets_fn,
)
from llama_index.core.llms import LLM
from llama_index.core.schema import TransformComponent

from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)
from src.retrieval._common import strip_thinking


def _parse_triplets_strip_thinking(response: str, **kwargs):
    """Wrap the upstream parser with a `<think>...</think>` stripper.

    Qwen3 emits thinking blocks where it rehearses the few-shot
    examples we pass — the upstream parser is naive line-level regex,
    so it pulls those rehearsals as if they were extracted triplets.
    Stripping first gives clean output and reduces false positives
    to near-zero on qwen3:8b.
    """
    return default_parse_triplets_fn(strip_thinking(response), **kwargs)

KGExtractor = TransformComponent


ExtractorMode = Literal["simple", "schema"]


_ENTITY_DESCRIPTION_PROP: tuple[str, str] = (
    "description",
    "A 1-2 sentence factual description of the entity drawn ONLY from "
    "the source text. State what the entity is, what it does, and any "
    "specific attributes mentioned. Do NOT invent facts. Keep names "
    "and quotes in the original language of the source.",
)


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

    - ``simple`` (default) — ``SimpleLLMPathExtractor``: plain prompt
      + regex parsing.  Works reliably with qwen3:8b and the
      baseline llama3.1:8b.  Entity types collapse to `entity`;
      descriptions are added in a separate second pass by
      `enrich_entities_with_descriptions` (see
      `src/graph/enrich.py`).
    - ``schema`` (experimental) — ``SchemaLLMPathExtractor`` over the
      universal typed `EntityType` / `RelationType` Literal unions
      from `src/graph/schema.py`. Requires a function-calling-
      capable LLM, but empirically qwen3:8b via Ollama → LiteLLM
      sometimes silently returns zero triplets when the underlying
      tool-call payload isn't well-formed.  Use with a stronger
      backend (gpt-4o-class) or larger qwen3 (14b+).  `strict=True`
      enforces `DEFAULT_VALIDATION_SCHEMA` triple templates.

    ``extract_prompt`` overrides the default multilingual template
    (Simple mode only).  Pass a string template that includes the
    `{max_knowledge_triplets}` and `{text}` placeholders.
    """
    if mode == "schema":
        return SchemaLLMPathExtractor(
            llm=llm,
            # Pass the Literal type itself — SchemaLLMPathExtractor
            # builds a dynamic Pydantic model from it.  Passing a list
            # silently works only when strict=False; with validation
            # schema it can break Pydantic dynamic-class generation.
            possible_entities=EntityType,  # type: ignore[arg-type]
            possible_relations=RelationType,  # type: ignore[arg-type]
            possible_entity_props=[_ENTITY_DESCRIPTION_PROP],
            kg_validation_schema=DEFAULT_VALIDATION_SCHEMA if strict else None,
            strict=strict,
            num_workers=num_workers,
        )
    return SimpleLLMPathExtractor(
        llm=llm,
        num_workers=num_workers,
        max_paths_per_chunk=10,
        extract_prompt=extract_prompt or _MULTILINGUAL_TRIPLET_EXTRACT_PROMPT,
        parse_fn=_parse_triplets_strip_thinking,
    )


# Multilingual triplet prompt — used ONLY by Simple mode.  English
# instructions + few-shots in four flavours covering the project's
# heterogeneous corpus: B2B contract, support transcript, email,
# analytical report.  The model anchors on "keep entity names in
# the source language" + "use concrete values, not template
# placeholders".
_MULTILINGUAL_TRIPLET_EXTRACT_PROMPT = (
    # `/no_think` is qwen3's directive to skip chain-of-thought
    # generation.  Other models will see it as inert text — safe.
    "/no_think\n"
    "Extract up to {max_knowledge_triplets} knowledge triplets from the text below.\n"
    "Each triplet must be on its own line in the format: "
    "(subject, predicate, object)\n"
    "\n"
    "Rules:\n"
    "1. Subject and object MUST be concrete entities from the text — "
    "people, organizations, topics, concepts, products, issues, "
    "events, IDs, addresses, dates, amounts.\n"
    "2. Predicate is a short verb phrase describing the relation.\n"
    "3. Keep entity names in the ORIGINAL language of the source text "
    "(do NOT translate company / person / topic names).\n"
    "4. Predicate can be in source language or English — prefer source.\n"
    "5. Do not invent entities not present in the text.\n"
    "6. Skip stop-words and pronouns as standalone subjects/objects.\n"
    "7. Do NOT output literal placeholders like \"Subject\"/\"Object\" — "
    "those are template markers, not values to copy.\n"
    "\n"
    "--- Examples (multiple languages, document types) ---\n"
    "Text: ООО Альфа заключило договор № 17-К с ИП Иванов на сумму 500000 руб.\n"
    "Triplets:\n"
    "(ООО Альфа, заключило договор, № 17-К)\n"
    "(№ 17-К, между, ИП Иванов)\n"
    "(№ 17-К, сумма, 500000 руб)\n"
    "\n"
    "Text: From: alice@example.com, Subject: Q1 review. The team agreed "
    "that the launch should be postponed to April.\n"
    "Triplets:\n"
    "(alice@example.com, discussed, Q1 review)\n"
    "(the team, agreed on, postponing the launch to April)\n"
    "\n"
    "Text: Agent: Hello, how can I help? Customer: My order #4521 "
    "never arrived. Agent: I'll issue a refund.\n"
    "Triplets:\n"
    "(Customer, reported, order #4521 never arrived)\n"
    "(Agent, resolved, refund for order #4521)\n"
    "\n"
    "Text: The report shows conversion grew 12% in Q1 driven by "
    "onboarding redesign.\n"
    "Triplets:\n"
    "(conversion, grew 12% in, Q1)\n"
    "(conversion growth, driven by, onboarding redesign)\n"
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

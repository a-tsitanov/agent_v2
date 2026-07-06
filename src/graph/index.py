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
from loguru import logger

from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)
from src.retrieval._common import strip_thinking

# Full-text index over entity names — backs partial-name lookup
# (``GraphRetriever.afind_entities_by_name``).  Idempotent DDL, safe to
# run repeatedly / concurrently.
ENTITY_FULLTEXT_INDEX_CYPHER = (
    "CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS FOR (e:__Entity__) ON EACH [e.name]"
)


def ensure_entity_fulltext_index(store) -> bool:
    """Idempotently create the entity-name full-text index.

    Returns True on success, False (logged) on any error — never raises,
    so callers on the ingest / retriever-bootstrap paths stay fail-open
    (a store without full-text support just keeps the old behaviour)."""
    try:
        store.structured_query(ENTITY_FULLTEXT_INDEX_CYPHER)
        return True
    except Exception as exc:  # broad by design — fail-open
        logger.warning("ensure_entity_fulltext_index failed: {e}", e=exc)
        return False


# Range index on `__Entity__.name`.  The llama-index Neo4j store creates a
# UNIQUE constraint on `.id` (the node identity), but the project's own
# Cypher matches entities by the separate `.name` property
# (``GraphRetriever.awalk`` seed lookup, ER stored-loser cleanup).  Without
# this index those are full label scans — O(N) at 250k+ entities.
ENTITY_NAME_INDEX_CYPHER = "CREATE INDEX entity_name IF NOT EXISTS FOR (e:__Entity__) ON (e.name)"

# Range index on `__Entity__.mention_count`.  Backs the incremental-ER
# window's ``ORDER BY n.mention_count DESC`` so the planner can return the
# most-mentioned canonicals without sorting the whole label.
ENTITY_MENTION_COUNT_INDEX_CYPHER = (
    "CREATE INDEX entity_mention_count IF NOT EXISTS FOR (e:__Entity__) ON (e.mention_count)"
)


def ensure_entity_lookup_indexes(store) -> bool:
    """Idempotently create the range indexes that keep entity lookups and
    the incremental-ER window scalable at 250k+ entities.

    Fail-open like ``ensure_entity_fulltext_index``: any error is logged
    and swallowed (a store/version without these indexes just keeps the
    old full-scan behaviour).  Returns True only if both succeeded."""
    ok = True
    for cypher in (ENTITY_NAME_INDEX_CYPHER, ENTITY_MENTION_COUNT_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_entity_lookup_indexes failed: {e}", e=exc)
            ok = False
    return ok


# Native vector index over `__Entity__.er_vec` — backs the opt-in
# entity-resolution kNN (`ERConfig.use_native_vector_knn`) that replaces
# the bounded 5000-entity window with a per-entity nearest-neighbour
# lookup across the whole graph.  Cosine to match the ER embeddings.
ER_VECTOR_INDEX_CYPHER = (
    "CREATE VECTOR INDEX er_embedding_vec IF NOT EXISTS "
    "FOR (e:__Entity__) ON e.er_vec "
    "OPTIONS {indexConfig: {`vector.dimensions`: $dim, "
    "`vector.similarity_function`: 'cosine'}}"
)


def ensure_er_vector_index(store, dim: int) -> bool:
    """Idempotently create the ER vector index on ``__Entity__.er_vec``.

    Fail-open: logs and returns False on any error (e.g. a Neo4j version
    without vector indexes), so the ER path can fall back to the window.
    """
    try:
        store.structured_query(ER_VECTOR_INDEX_CYPHER, param_map={"dim": int(dim)})
        return True
    except Exception as exc:  # broad by design — fail-open
        logger.warning("ensure_er_vector_index failed: {e}", e=exc)
        return False


# Native vector index over `Community.report_vec` — backs the structured
# community-report retrieval (Phase 2a of the hierarchical-communities
# track).  Reports are embedded from `title + summary`; cosine to match
# the project embedding model (mirrors `er_embedding_vec` on entities).
COMMUNITY_REPORT_VECTOR_INDEX_CYPHER = (
    "CREATE VECTOR INDEX community_report_vec IF NOT EXISTS "
    "FOR (c:Community) ON c.report_vec "
    "OPTIONS {indexConfig: {`vector.dimensions`: $dim, "
    "`vector.similarity_function`: 'cosine'}}"
)


def ensure_community_report_vector_index(store, dim: int) -> bool:
    """Idempotently create the community-report vector index on
    ``Community.report_vec``.

    Fail-open: logs and returns False on any error (e.g. a Neo4j version
    without vector indexes), so the report-build path can persist reports
    without the native index and fall back to lexical/summary search.
    """
    try:
        store.structured_query(COMMUNITY_REPORT_VECTOR_INDEX_CYPHER, param_map={"dim": int(dim)})
        return True
    except Exception as exc:  # broad by design — fail-open
        logger.warning("ensure_community_report_vector_index failed: {e}", e=exc)
        return False


# Range indexes for the community / global read paths.
#   * community_level — backs `MATCH (c:Community {level: $level})`
#     (global_search summary read).  NOT redundant with the
#     `community_key` UNIQUE constraint on `(c.id, c.level)`: a composite
#     index is only usable when its LEADING column (id) is bound, so it
#     can't serve a level-only lookup — this standalone index is required.
#   * chunk_doc_id — backs the `(:Chunk)-[:MENTIONS]->…->(:Community)`
#     traversal returning `c.doc_id` (verified live: Chunk nodes carry
#     `doc_id`).
COMMUNITY_LEVEL_INDEX_CYPHER = (
    "CREATE INDEX community_level IF NOT EXISTS FOR (c:Community) ON (c.level)"
)
CHUNK_DOC_ID_INDEX_CYPHER = "CREATE INDEX chunk_doc_id IF NOT EXISTS FOR (c:Chunk) ON (c.doc_id)"


def ensure_community_indexes(store) -> bool:
    """Idempotent range indexes for the community/global read paths
    (Community.level filter, Chunk.doc_id traversal).  Fail-open."""
    ok = True
    for cypher in (COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_community_indexes failed: {e}", e=exc)
            ok = False
    return ok


# Range indexes on the per-chunk date epochs (search date-filter feature).
# Chunks carry ``doc_date_epoch`` / ``inserted_at_epoch`` (stamped in
# parse_and_chunk); the graph post-filter reads them off retrieved chunks,
# and these indexes keep any future chunk-level date predicate scalable.
CHUNK_DOC_DATE_INDEX_CYPHER = (
    "CREATE INDEX chunk_doc_date_epoch IF NOT EXISTS FOR (c:Chunk) ON (c.doc_date_epoch)"
)
CHUNK_INSERTED_AT_INDEX_CYPHER = (
    "CREATE INDEX chunk_inserted_at_epoch IF NOT EXISTS FOR (c:Chunk) ON (c.inserted_at_epoch)"
)


def ensure_chunk_date_indexes(store) -> bool:
    """Idempotent range indexes on ``:Chunk(doc_date_epoch)`` and
    ``:Chunk(inserted_at_epoch)`` for date-filtered search.  Fail-open
    like the other ensure-index helpers."""
    ok = True
    for cypher in (CHUNK_DOC_DATE_INDEX_CYPHER, CHUNK_INSERTED_AT_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_chunk_date_indexes failed: {e}", e=exc)
            ok = False
    return ok


# Range index on ``__Entity__.created_at`` — E1 (Wave 0 first-seen feature).
# Backs the "what's new" query path that filters entities by epoch-day.
ENTITY_CREATED_AT_INDEX_CYPHER = (
    "CREATE INDEX entity_created_at IF NOT EXISTS FOR (e:__Entity__) ON (e.created_at)"
)

# Relationship temporal indexes. Neo4j rel-property indexes are PER-TYPE
# (``FOR ()-[r:TYPE]-()``), so we cover the dominant extractor type only:
# RELATED carries the majority of edges. They serve TYPED query paths
# (relationship_timeline with rel_type, future typed dynamics); the untyped
# ``-[r]-`` scan in whats_changed can NOT use them — acceptable while the
# graph is ~10^4 edges, revisit (enumerate top types) beyond that.
REL_TEMPORAL_INDEX_CYPHERS = (
    "CREATE INDEX rel_related_created_at IF NOT EXISTS "
    "FOR ()-[r:RELATED]-() ON (r.created_at)",
    "CREATE INDEX rel_related_valid_from IF NOT EXISTS "
    "FOR ()-[r:RELATED]-() ON (r.valid_from)",
)


def ensure_first_seen_indexes(store) -> bool:
    """Idempotently create the E1 temporal indexes: ``created_at`` on
    entities + per-type temporal indexes on RELATED relationships.

    Fail-open like the other ensure-index helpers: any error is logged
    and swallowed.  Returns True only if all DDL succeeded.
    """
    ok = True
    for cypher in (ENTITY_CREATED_AT_INDEX_CYPHER, *REL_TEMPORAL_INDEX_CYPHERS):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_first_seen_indexes failed: {e}", e=exc)
            ok = False
    return ok


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


ExtractorMode = Literal["lightrag", "simple", "schema", "gliner", "gliner+llm"]


class NoOpKGExtractor(TransformComponent):
    """Identity extractor used when KG_NODES_KEY metadata is already
    populated upstream and we just want `PropertyGraphIndex` to persist
    + embed those triples without re-running the LLM.

    Background: `PropertyGraphIndex._insert_nodes` pops `KG_NODES_KEY`
    off each input node *after* running its `kg_extractors`.  That
    pop happens regardless of who put the data there, so the worker
    flow needs to populate the metadata first, then pass a noop
    extractor:

        1. LightRAGExtractor.acall(nodes)  → fills KG_NODES_KEY /
           KG_RELATIONS_KEY with descriptions inline
        2. merge_kg_extraction(nodes, llm)  → returns deduplicated
           cross-chunk EntityNode/Relation lists (separate concern)
        3. PropertyGraphIndex(nodes=..., kg_extractors=[NoOpKGExtractor()])
           → pops per-chunk metadata, creates Chunk(:MENTIONS)→Entity
             edges, embeds entities for the vector retriever
        4. graph_store.upsert_nodes(merged_entities)
           graph_store.upsert_relations(merged_relations)
           → overwrites the per-chunk descriptions with cross-chunk
             merged ones (upsert merges by name).
    """

    def __call__(self, nodes, **kwargs):  # type: ignore[override]
        return nodes

    async def acall(self, nodes, **kwargs):  # type: ignore[override]
        return nodes


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
    mode: ExtractorMode = "lightrag",
    strict: bool = False,
    num_workers: int = 4,
    extract_prompt: str | None = None,
    gleaning_passes: int = 0,
) -> KGExtractor:
    """Build a KG path extractor.

    Three modes:

    - ``lightrag`` (default) — ``LightRAGExtractor``: one LLM call
      per chunk produces entities (name + type + description) +
      relations (src + tgt + keywords + description) in a single
      structured response.  Algorithm ported from HKUDS/LightRAG
      (see ``src/graph/lightrag_prompts.py``).  Descriptions are
      populated inline — NO separate enrichment pass needed.
      Cross-chunk consolidation lives in
      ``src/graph/merge.py:merge_kg_extraction``.
    - ``simple`` — ``SimpleLLMPathExtractor``: plain prompt + regex
      parsing.  Kept as the R9 regression baseline.  Entity types
      collapse to ``entity``; descriptions are empty without a
      follow-up enrichment step (no longer wired in the worker).
    - ``schema`` (experimental) — ``SchemaLLMPathExtractor`` over the
      universal typed ``EntityType`` / ``RelationType`` Literal unions.
      Requires a function-calling-capable LLM; reliable on
      gpt-4o-mini, flaky on qwen3:8b via LiteLLM.

    ``gleaning_passes`` is only consumed by the ``lightrag`` mode
    (LightRAG default is 1; we default to 0 to keep cost down and
    let R9 eval decide).

    ``extract_prompt`` overrides the default multilingual template
    (Simple mode only).
    """
    if mode == "lightrag":
        from src.graph.lightrag_extract import LightRAGExtractor

        return LightRAGExtractor(
            llm=llm,
            num_workers=num_workers,
            gleaning_passes=gleaning_passes,
        )
    if mode == "gliner":
        from src.config import settings
        from src.graph.gliner_extract import GLiNERExtractor

        return GLiNERExtractor(model_name=settings.ingestion.gliner_model)
    if mode == "gliner+llm":
        # Placeholder: LLM relation/description enrichment is TODO. For now
        # this aliases plain "gliner" (span detection only) so the pipeline
        # can A/B GLiNER without a code switch; compose with LightRAG in the
        # ingest activity until a relations-only LightRAG mode lands.
        from loguru import logger

        from src.config import settings
        from src.graph.gliner_extract import GLiNERExtractor

        logger.warning(
            "build_kg_extractor mode='gliner+llm' currently aliases plain "
            "'gliner' (span detection only); no LLM relation/description "
            "enrichment yet — relations will be empty."
        )
        return GLiNERExtractor(model_name=settings.ingestion.gliner_model)
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
    '7. Do NOT output literal placeholders like "Subject"/"Object" — '
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
    llm: LLM | None = None,
) -> PropertyGraphIndex:
    """Compose a PropertyGraphIndex from store + embed + extractor.

    Pass ``nodes`` to build from chunks (used in Stage 8 worker);
    omit to attach to an existing populated store.

    ``llm`` is threaded onto the index so the default retriever's
    ``LLMSynonymRetriever`` uses the project (LiteLLM) model.  Without it
    the index's ``llm`` is unset and ``as_retriever()`` falls back to
    LlamaIndex's global ``Settings.llm`` (OpenAI), which crashes a
    local-only deploy that has no ``OPENAI_API_KEY``.  Omit for the ingest
    path (NoOp extractor, no retrieval) — only the retrieval paths need it.
    """
    if nodes is None:
        return PropertyGraphIndex.from_existing(
            property_graph_store=graph_store,
            embed_model=embed_model,
            kg_extractors=[extractor],
            llm=llm,
        )
    return PropertyGraphIndex(
        nodes=nodes,
        property_graph_store=graph_store,
        embed_model=embed_model,
        kg_extractors=[extractor],
        llm=llm,
        show_progress=False,
    )

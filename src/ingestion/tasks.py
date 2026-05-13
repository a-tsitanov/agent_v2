"""Taskiq broker + `process_document` task.

The worker is intentionally thin — it composes existing factory
helpers (`build_ingestion_pipeline`, `build_vector_index`,
`build_kg_extractor`, `merge_kg_extraction`) against live
storage backends.  Identifier canonicalization is built into
`build_ingestion_pipeline` by default — no need to pass it as an
extra transformation any more.

Run::

    uv run taskiq worker src.ingestion.tasks:broker --workers 1
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from loguru import logger
from taskiq_aio_pika import AioPikaBroker

from src.config import settings
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
)

from src.graph.entity_resolution import ERConfig, resolve_entities
from src.graph.index import (
    NoOpKGExtractor,
    build_kg_extractor,
    build_property_graph_index,
)
from src.graph.merge import merge_kg_extraction
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.ingestion.identifier_transform import inject_canonical_entities
from src.ingestion.pipeline import build_ingestion_pipeline, read_documents
from src.retrieval.llm import build_llm
from src.retrieval.vector_index import (
    build_vector_index,
    build_vector_store,
    index_nodes,
)
from src.storage.postgres import AsyncPostgres

broker = AioPikaBroker(settings.rabbitmq.url)


# Neo4j accepts only primitive properties + arrays of primitives —
# nested maps / lists-of-maps cause `Neo.ClientError.Statement.TypeError`
# when PropertyGraphIndex writes a `:Chunk` node.  Our pipeline
# attaches `canonical_identifiers` as `list[dict]` for downstream
# retrievers (Milvus tolerates it via JSON serialisation), so we
# scrub the offending keys off the in-memory nodes right before the
# graph step.  Milvus has already received the full metadata in
# step 2; the strip is graph-store-only.
_NEO4J_UNSAFE_METADATA_KEYS: frozenset[str] = frozenset({
    "canonical_identifiers",
})

# These metadata keys carry LlamaIndex objects (EntityNode, Relation)
# that PropertyGraphIndex.`_insert_nodes` pops BEFORE writing the
# chunk to Neo4j.  They look "neo4j-unsafe" to a naive value check,
# but stripping them breaks PropertyGraphIndex's own assertion
# (`metadata.get(KG_NODES_KEY) is not None`).
_PRESERVE_METADATA_KEYS: frozenset[str] = frozenset({
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
})


def _is_neo4j_safe(value):
    """A value Neo4j will accept as a node property — primitives
    or flat arrays of primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(
            v is None or isinstance(v, (str, int, float, bool))
            for v in value
        )
    return False


def _consolidate_phone_entities(
    entities: "list[Any]",
    relations: "list[Any]",
    nodes: "list[Any] | None" = None,
) -> "tuple[list[Any], list[Any], dict[str, str]]":
    """Collapse LLM-extracted PhoneNumber duplicates onto their
    canonical E.164 form.

    Why a separate pass: ER excludes PhoneNumber from semantic merge
    on purpose (close cosine between two different numbers is
    expected — same country / area code).  But two non-semantic
    paths legitimately produce duplicates of the SAME phone:

      * `inject_canonical_entities` writes deterministic canonical
        nodes ("+74951234567") with label=PhoneNumber.
      * `LightRAGExtractor` reads the augment block ("Канонические
        идентификаторы: +74951234567 ...") + the original text and
        emits its own PhoneNumber entities ("Телефон +7 (495)...",
        "Горячая линия 8-800-...").

    Both end up as separate Neo4j nodes because they have different
    names.  This helper parses digits via libphonenumber, builds a
    canonical E.164, and merges every PhoneNumber entity whose
    digits resolve to the same canonical into one.
    """
    import phonenumbers
    from llama_index.core.graph_stores.types import EntityNode, Relation

    # name → canonical_phone_or_None.  We only consolidate when
    # libphonenumber can parse a valid number.
    name_to_canonical: dict[str, str] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        if (ent.label or "") != "PhoneNumber":
            continue
        # libphonenumber tolerates noisy prefixes ("Телефон ...") —
        # try the whole name first, then digits-only as fallback.
        canon: str | None = None
        for region in ("RU", "GB", None):
            try:
                matches = list(phonenumbers.PhoneNumberMatcher(ent.name, region))
            except Exception:  # noqa: BLE001
                continue
            if matches:
                canon = phonenumbers.format_number(
                    matches[0].number, phonenumbers.PhoneNumberFormat.E164,
                )
                break
        if canon is None:
            continue
        name_to_canonical[ent.name] = canon

    if not name_to_canonical:
        return entities, relations, {}

    # Group entities by canonical.  Pick the entity ALREADY in
    # canonical form as the survivor; otherwise create one.
    by_canonical: dict[str, list[EntityNode]] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        canon = name_to_canonical.get(ent.name)
        if canon is None:
            continue
        by_canonical.setdefault(canon, []).append(ent)

    # Build the merge map: old_entity_id → survivor_entity_id.
    id_remap: dict[str, str] = {}
    survivors_by_canonical: dict[str, EntityNode] = {}
    consolidated_ids: set[str] = set()
    for canon, group in by_canonical.items():
        if len(group) < 2:
            # Unique entry — only rename to canonical for consistency.
            ent = group[0]
            if ent.name != canon:
                aliases = list((ent.properties or {}).get("aliases", []))
                if ent.name not in aliases:
                    aliases.append(ent.name)
                ent.name = canon
                (ent.properties or {})["aliases"] = aliases
            survivors_by_canonical[canon] = ent
            continue
        # Prefer the entity whose name IS the canonical form.
        survivor = next(
            (e for e in group if e.name == canon), group[0],
        )
        aliases = list((survivor.properties or {}).get("aliases", []))
        mention_count = int((survivor.properties or {}).get("mention_count", 1) or 1)
        descs = [str((survivor.properties or {}).get("description", "") or "")]
        source_chunks: list[str] = list(
            (survivor.properties or {}).get("source_chunks", []) or [],
        )
        file_paths: list[str] = list(
            (survivor.properties or {}).get("file_paths", []) or [],
        )
        for other in group:
            if other is survivor:
                continue
            if other.name not in aliases and other.name != canon:
                aliases.append(other.name)
            mention_count += int(
                (other.properties or {}).get("mention_count", 1) or 1
            )
            d = str((other.properties or {}).get("description", "") or "")
            if d and d not in descs:
                descs.append(d)
            for cid in (other.properties or {}).get("source_chunks", []) or []:
                if cid not in source_chunks:
                    source_chunks.append(cid)
            for fp in (other.properties or {}).get("file_paths", []) or []:
                if fp not in file_paths:
                    file_paths.append(fp)
            id_remap[other.id] = survivor.id
            consolidated_ids.add(other.id)
        survivor.name = canon
        if survivor.properties is None:
            survivor.properties = {}
        survivor.properties["aliases"] = aliases
        survivor.properties["mention_count"] = mention_count
        survivor.properties["description"] = "\n---\n".join(d for d in descs if d)
        survivor.properties["source_chunks"] = source_chunks
        survivor.properties["file_paths"] = file_paths
        survivors_by_canonical[canon] = survivor

    # Drop consolidated entities from the list.
    new_entities = [
        e for e in entities
        if not (isinstance(e, EntityNode) and e.id in consolidated_ids)
    ]

    # Rewrite relations: any source_id / target_id that was merged
    # away points at the survivor now.  Drop self-loops.
    new_relations = []
    seen_pairs: set[tuple[str, str, str]] = set()
    for rel in relations:
        if not isinstance(rel, Relation):
            new_relations.append(rel)
            continue
        src = id_remap.get(rel.source_id, rel.source_id)
        tgt = id_remap.get(rel.target_id, rel.target_id)
        if src == tgt:
            continue
        key = (src, tgt, rel.label or "")
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        rel.source_id = src
        rel.target_id = tgt
        new_relations.append(rel)

    # Build name_map (old → canonical) so the caller can rewrite
    # chunk-level KG_NODES_KEY metadata.  Without this rewrite,
    # PropertyGraphIndex creates Neo4j nodes from the chunks'
    # original names (e.g. "Телефон +7 (495)...") parallel to the
    # canonical ones we just upserted — producing duplicates again.
    phone_name_map: dict[str, str] = {
        name: canon for name, canon in name_to_canonical.items()
        if name != canon
    }
    if nodes and phone_name_map:
        from llama_index.core.graph_stores.types import (
            KG_NODES_KEY as _KG_NODES_KEY,
        )
        for node in nodes:
            md = getattr(node, "metadata", None)
            ents = (md or {}).get(_KG_NODES_KEY) or []
            for ent in ents:
                if not isinstance(ent, EntityNode):
                    continue
                canonical = phone_name_map.get(ent.name)
                if canonical:
                    ent.name = canonical

    logger.info(
        "phone consolidation  total_phones={t}  consolidated={c}  "
        "surviving={s}  renamed_chunks={r}",
        t=len(name_to_canonical), c=len(consolidated_ids),
        s=len(survivors_by_canonical), r=len(phone_name_map),
    )
    return new_entities, new_relations, phone_name_map


def _strip_neo4j_unsafe_metadata(nodes) -> None:
    """In-place: drop metadata keys whose values Neo4j would reject.

    Preserves `KG_NODES_KEY` / `KG_RELATIONS_KEY` (LlamaIndex's
    extraction artifacts) — PropertyGraphIndex itself pops those
    before writing to Neo4j.
    """
    for n in nodes:
        md = getattr(n, "metadata", None)
        if not md:
            continue
        for key in list(md.keys()):
            if key in _PRESERVE_METADATA_KEYS:
                continue
            if key in _NEO4J_UNSAFE_METADATA_KEYS or not _is_neo4j_safe(md[key]):
                md.pop(key, None)


@broker.task
async def process_document(doc_id: str, path: str) -> None:
    """Run the full ingestion chain for one uploaded file.

    Flow:
      1. Read → split → identifier-canon (built into pipeline).
      2. Insert chunks into Milvus.
      3. (best-effort) Inject canonical entities into Neo4j.
      4. (best-effort) Run KG extractor over chunks → triples land
         in Neo4j.
      5. (best-effort) Enrich entities with LLM-generated
         descriptions.
      6. Mark `completed` in Postgres (or `failed` with error).

    Steps 3-5 are wrapped in try/except so a Neo4j or LLM outage
    doesn't block the vector-only path from completing.
    """
    pg = AsyncPostgres()
    target = Path(path)
    job_uuid = uuid.UUID(doc_id)
    llm = build_llm()
    embed_model = build_embedding_model()

    try:
        await pg.update_status(job_uuid, status="processing")

        # 1. parse + chunk + identifier-canon + (optional) RU translation.
        # `translator_llm` is the same project LLM as the KG extractor:
        # cheap on gpt-4o-mini and centralises the LiteLLM proxy hop.
        # `embed_model` is required only when semantic chunking is on
        # — passed unconditionally so toggling INGESTION_SEMANTIC_CHUNKING
        # at runtime doesn't need a code change.
        pipeline = build_ingestion_pipeline(
            embed_model=embed_model,
            translator_llm=llm,
        )
        docs = read_documents(target.parent, recursive=False)
        docs = [d for d in docs if d.metadata.get("file_path") == str(target)]
        if not docs:
            raise FileNotFoundError(f"file not in reader output: {target}")

        # `pipeline.arun` is the async variant — sync `.run` internally
        # calls `asyncio.run` and explodes inside the taskiq event loop.
        nodes = await pipeline.arun(documents=docs)

        # Belt-and-suspenders: ensure the doc-level translation
        # scaffolding never reaches Milvus.  LlamaIndex's SentenceSplitter
        # copies the parent Document's metadata into each chunk
        # AND into each chunk's `relationships[SOURCE].metadata`
        # (a `RelatedNodeInfo` pointing back at the parent).  Milvus
        # serialises the whole `_node_content` field — including
        # relationship metadata — and rejects dynamic fields > 65k
        # chars.  TranslateToRussianTransform drops these from
        # `node.metadata`; we additionally clean every relationship
        # here so a 95k full-translation never lands in the row.
        from src.ingestion.translate_transform import (
            FULL_TRANSLATED_TEXT_KEY,
            ORIGINAL_DOC_LENGTH_KEY,
        )

        def _scrub(md: dict | None) -> None:
            if not md:
                return
            md.pop(FULL_TRANSLATED_TEXT_KEY, None)
            md.pop(ORIGINAL_DOC_LENGTH_KEY, None)

        for n in nodes:
            _scrub(getattr(n, "metadata", None))
            for rel in (getattr(n, "relationships", {}) or {}).values():
                _scrub(getattr(rel, "metadata", None))

        # 2. vector indexing
        store = build_vector_store()
        index = build_vector_index(store, embed_model)
        index_nodes(index, nodes)

        # 3-5. graph build — best-effort
        try:
            graph_store = build_neo4j_graph_store()
            inject_canonical_entities(graph_store, nodes)
            try:
                # LightRAG-style flow (see NoOpKGExtractor docstring):
                #   1. extractor: one LLM call/chunk → KG_NODES_KEY /
                #      KG_RELATIONS_KEY with entity descriptions inline
                #   2. cross-chunk merger: dedup by name, concat or
                #      LLM-summary descriptions, dedup relations
                #   3. PropertyGraphIndex with NoOp extractor: pops
                #      per-chunk metadata, creates Chunk(:MENTIONS)→
                #      Entity edges, embeds entities for retrieval
                #   4. upsert merged entities+relations: overwrites
                #      per-chunk descriptions with cross-chunk merged
                extractor = build_kg_extractor(llm, mode="lightrag")
                nodes = await extractor.acall(nodes)
                merged_entities, merged_relations = await merge_kg_extraction(
                    nodes, llm, language="Russian",
                )
                # Phone consolidation: LightRAG often re-extracts
                # the same phone number with different surface forms
                # ("Телефон +7 (495)...", "Горячая линия 8-800-...",
                # "8-916-555-77-89") that DUPLICATE the canonical
                # E.164 nodes produced by `inject_canonical_entities`.
                # ER excludes PhoneNumber from semantic merging
                # (legitimately — two different numbers can embed
                # close), so we collapse them here deterministically:
                # parse digits, re-canonicalise to E.164, merge any
                # collisions into one PhoneNumber entity.
                merged_entities, merged_relations, _phone_name_map = (
                    _consolidate_phone_entities(
                        merged_entities, merged_relations, nodes,
                    )
                )

                # Entity Resolution: collapses cross-language /
                # multi-form duplicates and matches against entities
                # already in Neo4j from previous ingests.  Best-effort:
                # if the embed model or LLM fail, returns the inputs
                # unchanged — no impact on ingest correctness.
                if settings.agent.er_enabled:
                    merged_entities, merged_relations, _er_name_map = (
                        await resolve_entities(
                            merged_entities,
                            merged_relations,
                            nodes,
                            llm=llm,
                            embed_model=embed_model,
                            graph_store=graph_store,
                            config=ERConfig(
                                language="Russian",
                                judge_batch=settings.agent.er_judge_batch_size,
                                # Require at least one shared content
                                # token in same-script candidate pairs.
                                # Cross-script pairs (RU vs EN) bypass
                                # this check and rely on cosine + LLM
                                # judge — those legitimately have zero
                                # token overlap on the surface form.
                                name_token_min_overlap=0.1,
                            ),
                        )
                    )
                    logger.info(
                        "ER complete  doc_id={d}  merged_aliases={m}",
                        d=doc_id, m=len(_er_name_map),
                    )
                # PropertyGraphIndex writes every chunk's metadata
                # onto its `:Chunk` node in Neo4j.  Neo4j rejects
                # nested types ("Property values can only be of
                # primitive types or arrays thereof") so strip any
                # metadata value that isn't a Neo4j-friendly scalar
                # right before that call.  Milvus already received
                # the original metadata in step 2; this only affects
                # the graph store.
                _strip_neo4j_unsafe_metadata(nodes)
                await asyncio.to_thread(
                    build_property_graph_index,
                    graph_store=graph_store,
                    embed_model=embed_model,
                    extractor=NoOpKGExtractor(),
                    nodes=nodes,
                )
                if merged_entities:
                    graph_store.upsert_nodes(merged_entities)
                if merged_relations:
                    graph_store.upsert_relations(merged_relations)
                logger.info(
                    "graph done  doc_id={d}  entities={e}  relations={r}",
                    d=doc_id,
                    e=len(merged_entities),
                    r=len(merged_relations),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "graph LLM extraction failed: {err}", err=exc,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph injection failed: {err}", err=exc)

        await pg.update_status(job_uuid, status="completed")
        logger.info(
            "ingestion done  doc_id={d}  nodes={n}",
            d=doc_id, n=len(nodes),
        )
    except Exception as exc:  # noqa: BLE001 — surface to client
        logger.exception("ingestion failed  doc_id={d}", d=doc_id)
        await pg.update_status(job_uuid, status="failed", error=str(exc))

"""`merge_and_resolve` — cross-chunk dedup + phone consolidation + ER.

Pulls the post-KG nodes from staging, runs the three dedup passes
(LightRAG merge -> phone consolidation -> entity resolution), then
writes a tuple ``(entities, relations, nodes)`` to a fresh staging
blob.  Nodes are written too because phone/ER passes can rewrite
chunk-level `KG_NODES_KEY` metadata in-place.
"""

from __future__ import annotations

from loguru import logger
from temporalio import activity

from src.config import settings
from src.graph.entity_resolution import ERConfig, resolve_entities
from src.graph.merge import merge_kg_extraction
from src.graph.phone_consolidation import consolidate_phone_entities
from src.graph.store import build_neo4j_graph_store
from src.ingestion.embeddings import build_embedding_model
from src.retrieval.llm import build_llm
from src.workflow.contracts import KGExtracted, Merged
from src.workflow.staging import build_staging_store


@activity.defn
async def merge_and_resolve(kg: KGExtracted) -> Merged:
    staging = build_staging_store()
    nodes = staging.read_pickle(kg.nodes_with_kg_uri)
    llm = build_llm()

    merged_entities, merged_relations = await merge_kg_extraction(
        nodes, llm, language="Russian",
    )
    merged_entities, merged_relations, _phone_map = consolidate_phone_entities(
        merged_entities, merged_relations, nodes,
    )
    if settings.agent.er_enabled:
        embed_model = build_embedding_model()
        graph_store = build_neo4j_graph_store()
        merged_entities, merged_relations, _er_map = await resolve_entities(
            merged_entities, merged_relations, nodes,
            llm=llm, embed_model=embed_model, graph_store=graph_store,
            config=ERConfig(
                language="Russian",
                judge_batch=settings.agent.er_judge_batch_size,
                name_token_min_overlap=0.1,
            ),
        )

    uri = staging.write_pickle(
        kg.parsed.ctx.workflow_run_id, "merged",
        (merged_entities, merged_relations, nodes),
    )
    logger.info(
        "merge_and_resolve done  doc={d}  entities={e}  relations={r}",
        d=kg.parsed.ctx.doc_id,
        e=len(merged_entities), r=len(merged_relations),
    )
    return Merged(kg=kg, merged_entities_uri=uri)

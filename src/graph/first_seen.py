"""E1 — emulate ON CREATE stamping (created_at/first_doc_id) post-upsert.

The entity/relationship MERGE lives inside llama_index, so we cannot set
ON CREATE there. Instead, after upsert, stamp only elements that have no
created_at yet, scoped to this ingest's elements. Combined with the one-time
sentinel backfill (scripts/backfill_first_seen.py), this means: a node
created this pass gets stamped now; a re-mentioned old node keeps its
original stamp.

Backend dispatch (``settings.graph.backend``):
- neo4j (default): the Cypher templates below via ``structured_query``.
- nebula: per-entity ``UPDATE VERTEX ... WHEN created_at == 0`` (atomic,
  first-write-wins). Relationship first-seen is a no-op under nebula — the
  RELATED edge type lacks created_at/first_doc_id columns (deferred
  follow-up; see docs/superpowers/specs/2026-07-11-nebula-first-seen-design.md).
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.config import settings
from src.graph.nebula_store import _q, entity_vid

# ── Cypher templates (neo4j) ─────────────────────────────────────────────────
# Both use WHERE created_at IS NULL so that re-mentioned existing elements
# keep their original first_seen stamp (ON CREATE semantics without an
# explicit APOC / ON CREATE SET clause, which is unavailable from the
# llama_index upsert path).

_STAMP_ENTITIES = (
    "UNWIND $names AS nm "
    "MATCH (e:__Entity__ {name: nm}) "
    "WHERE e.created_at IS NULL "
    "SET e.created_at = $ts, e.first_doc_id = $doc_id"
)

_STAMP_RELS = (
    "UNWIND $rels AS rel "
    "MATCH (a:__Entity__ {name: rel.src})-[r]->(b:__Entity__ {name: rel.tgt}) "
    "WHERE type(r) = rel.label AND r.created_at IS NULL "
    "SET r.created_at = $ts, r.first_doc_id = $doc_id"
)


def stamp_first_seen(
    store: Any,
    *,
    entity_names: list[str],
    relations: list[tuple[str, str, str]],
    ingest_epoch: int,
    doc_id: str,
) -> None:
    """Stamp created_at/first_doc_id on newly-created graph elements.

    Sets the fields ONLY on elements that have no created_at yet, so
    existing elements (re-mentioned in a later document) keep their
    original first_seen stamp. Best-effort/fail-open — a stamping failure
    never blocks ingest.

    Args:
        store: A graph store that exposes ``structured_query(cypher, param_map)``.
        entity_names: Display names of the entities upserted in this pass.
        relations: Triples ``(src_name, label, tgt_name)`` for relations upserted.
        ingest_epoch: Days since 1970-01-01 (UTC) — use ``today_epoch_days()``.
        doc_id: Identifier of the source document (best-effort provenance).
    """
    if not entity_names and not relations:
        return

    try:
        if settings.graph.backend == "nebula":
            _stamp_first_seen_nebula(
                store,
                entity_names=entity_names,
                relations=relations,
                ingest_epoch=ingest_epoch,
                doc_id=doc_id,
            )
        else:
            _stamp_first_seen_neo4j(
                store,
                entity_names=entity_names,
                relations=relations,
                ingest_epoch=ingest_epoch,
                doc_id=doc_id,
            )
    except Exception as exc:
        logger.warning("stamp_first_seen failed (non-fatal): {e}", e=exc)


def _stamp_first_seen_neo4j(
    store: Any,
    *,
    entity_names: list[str],
    relations: list[tuple[str, str, str]],
    ingest_epoch: int,
    doc_id: str,
) -> None:
    if entity_names:
        store.structured_query(
            _STAMP_ENTITIES,
            param_map={"names": list(entity_names), "ts": ingest_epoch, "doc_id": doc_id},
        )

    if relations:
        rels = [{"src": src, "label": lbl, "tgt": tgt} for src, lbl, tgt in relations]
        store.structured_query(
            _STAMP_RELS,
            param_map={"rels": rels, "ts": ingest_epoch, "doc_id": doc_id},
        )


def _stamp_first_seen_nebula(
    store: Any,
    *,
    entity_names: list[str],
    relations: list[tuple[str, str, str]],
    ingest_epoch: int,
    doc_id: str,
) -> None:
    for name in entity_names:
        vid = entity_vid(name)
        stmt = (
            f'UPDATE VERTEX ON `Entity` "{vid}" '
            f"SET created_at = {int(ingest_epoch)}, first_doc_id = {_q(doc_id)} "
            "WHEN created_at == 0;"
        )
        store.structured_query(stmt)

    if relations:
        # RELATED lacks created_at/first_doc_id under nebula — deferred
        # follow-up (see design doc §Out of scope). No-op, not a loop.
        logger.debug(
            "stamp_first_seen: nebula relation first-seen is a no-op "
            "(RELATED has no created_at/first_doc_id); {n} relation(s) skipped",
            n=len(relations),
        )


__all__ = ["stamp_first_seen"]

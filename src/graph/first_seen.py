"""E1 — emulate ON CREATE stamping (created_at/first_doc_id) post-upsert.

The entity/relationship MERGE lives inside llama_index, so we cannot set
ON CREATE there. Instead, after upsert, stamp only elements that have no
created_at yet, scoped to this ingest's elements. Combined with the one-time
sentinel backfill (scripts/backfill_first_seen.py), this means: a node
created this pass gets stamped now; a re-mentioned old node keeps its
original stamp.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

# ── Cypher templates ─────────────────────────────────────────────────────────
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

    Sets the fields ONLY WHERE created_at IS NULL, so existing elements
    (re-mentioned in a later document) keep their original first_seen stamp.

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
    except Exception as exc:
        logger.warning("stamp_first_seen failed (non-fatal): {e}", e=exc)


__all__ = ["stamp_first_seen"]

# scripts/backfill_er_vec_milvus.py
"""Backfill entity_er_vec (Milvus) from existing Neo4j __Entity__ er_vec.

    python -m scripts.backfill_er_vec_milvus            # dry-run (counts only)
    python -m scripts.backfill_er_vec_milvus --no-dry-run

Greenfield nebula needs no backfill (ER writes to Milvus from the start).
"""
from __future__ import annotations

import argparse
import json

from loguru import logger

from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore
from src.graph.store import build_neo4j_graph_store

_READ = """
MATCH (e:__Entity__) WHERE e.er_canonical_name IS NOT NULL
RETURN e.name AS name, labels(e) AS labels, e.er_vec AS er_vec,
       e.er_embedding AS er_embedding,
       coalesce(e.mention_count,1) AS mention_count,
       coalesce(e.description,'') AS description
"""


def _emb(row):
    v = row.get("er_vec")
    if v:
        return list(v)
    raw = row.get("er_embedding") or "[]"
    try:
        return json.loads(raw) if isinstance(raw, str) else list(raw)
    except Exception:
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args()

    store = build_neo4j_graph_store()
    rows = store.structured_query(_READ) or []
    cands = []
    for r in rows:
        emb = _emb(r)
        if not r.get("name") or not emb:
            continue
        labels = [x for x in (r.get("labels") or []) if x not in ("__Entity__", "__Node__")]
        cands.append({"name": r["name"], "label": labels[0] if labels else "Other",
                      "embedding": emb, "mention_count": int(r.get("mention_count") or 1),
                      "description": r.get("description") or ""})
    logger.info("backfill: {n} canonical entities with vectors", n=len(cands))
    if not args.no_dry_run:
        logger.info("dry-run — pass --no-dry-run to write to Milvus")
        return
    ms = MilvusEntityVectorStore()
    for i in range(0, len(cands), args.batch):
        ms.upsert(cands[i:i + args.batch])
    logger.info("backfill: upserted {n} to entity_er_vec", n=len(cands))


if __name__ == "__main__":
    main()

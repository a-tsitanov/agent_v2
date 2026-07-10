# scripts/backfill_report_vec_milvus.py
"""Backfill community_report_vec (Milvus) from existing Neo4j Community report_vec.

    python -m scripts.backfill_report_vec_milvus            # dry-run (counts only)
    python -m scripts.backfill_report_vec_milvus --no-dry-run

Greenfield nebula needs no backfill (Milvus writes from the start).
"""
from __future__ import annotations

import argparse
import json

from loguru import logger

from src.graph.community_vector_store_milvus import MilvusCommunityReportVectorStore
from src.graph.store import build_neo4j_graph_store

_READ = """
MATCH (c:Community) WHERE c.report_vec IS NOT NULL AND c.summary IS NOT NULL AND trim(c.summary) <> ''
RETURN c.id AS community_id, c.level AS level, c.summary AS summary, c.report_vec AS report_vec
"""


def _emb(row):
    v = row.get("report_vec")
    if v:
        return list(v)
    raw = row.get("embedding") or "[]"
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
        if not r.get("community_id") or not emb:
            continue
        cands.append({"community_id": r["community_id"], "level": int(r.get("level") or 0),
                      "summary": (r.get("summary") or "").strip(), "embedding": emb})
    logger.info("backfill: {n} community reports with vectors", n=len(cands))
    if not args.no_dry_run:
        logger.info("dry-run — pass --no-dry-run to write to Milvus")
        return
    ms = MilvusCommunityReportVectorStore()
    for i in range(0, len(cands), args.batch):
        ms.upsert(cands[i:i + args.batch])
    logger.info("backfill: upserted {n} to community_report_vec", n=len(cands))


if __name__ == "__main__":
    main()

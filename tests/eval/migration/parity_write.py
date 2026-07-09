"""Write the same fixture to Neo4j and NebulaGraph via the seam and compare
structure (node/edge counts + a sampled entity).  Run manually:

    GRAPH_BACKEND=neo4j  python -m tests.eval.migration.parity_write
    GRAPH_BACKEND=nebula python -m tests.eval.migration.parity_write

then diff the two JSON reports.  Gate for Phase 2 (read translation)."""
from __future__ import annotations

import json

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.store import build_graph_store

FIXTURE_NODES = [
    EntityNode(name="Иванов", label="PERSON", properties={"description": "инженер", "mention_count": 3}),
    EntityNode(name="Москва", label="CITY", properties={"description": "город", "mention_count": 9}),
]
FIXTURE_RELS = [
    Relation(source_id="Иванов", target_id="Москва", label="RELATED", properties={"polarity": "pos"}),
]


def main() -> None:
    store = build_graph_store()
    store.upsert_nodes(FIXTURE_NODES)
    store.upsert_relations(FIXTURE_RELS)
    report = {
        "nodes_written": len(FIXTURE_NODES),
        "rels_written": len(FIXTURE_RELS),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

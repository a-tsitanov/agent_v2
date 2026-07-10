# tests/eval/migration/parity_read.py
"""Live read-slice gate: write the fixture, then find-by-name + walk under
nebula and print the RoundGraphData. Manual, needs a live cluster:

    GRAPH_BACKEND=nebula API_ENV=development \
      python -m tests.eval.migration.parity_read
"""
from __future__ import annotations

import asyncio
import json

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.retriever import GraphRetriever
from src.graph.store import build_graph_store

NODES = [
    EntityNode(name="Иванов", label="PERSON", properties={"description": "инженер", "mention_count": 3}),
    EntityNode(name="Москва", label="CITY", properties={"description": "город", "mention_count": 9}),
]
RELS = [Relation(source_id="Иванов", target_id="Москва", label="WORKS_AT",
                 properties={"polarity": "pos"})]


async def main() -> None:
    store = build_graph_store()
    store.upsert_nodes(NODES)
    store.upsert_relations(RELS)
    r = GraphRetriever.for_store(store)
    found = await r.afind_entities_by_name("Иванов")
    walked = await r.awalk("Иванов", hops=2)
    print(json.dumps({
        "found_entities": found.entities,
        "walk_entities": walked.entities,
        "walk_relations": walked.relations,
    }, ensure_ascii=False, indent=2))
    assert any(e["entity_name"] == "Иванов" for e in found.entities), "find-by-name failed"
    assert {e["entity_name"] for e in walked.entities} >= {"Иванов", "Москва"}, "walk entities"
    assert any(rl["label"] == "WORKS_AT" for rl in walked.relations), "walk rel_type lost"
    print("PARITY READ OK")


if __name__ == "__main__":
    asyncio.run(main())

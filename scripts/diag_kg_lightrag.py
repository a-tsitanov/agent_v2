"""Offline probe of the LightRAG-style extract + merge stack.

Runs the live project LLM through `LightRAGExtractor` +
`merge_kg_extraction` on the first N chunks of the medical corpus,
printing per-chunk and post-merge counts.  Doesn't touch Postgres
/ RabbitMQ / Milvus / Neo4j — purely a development sanity check
for the extraction pipeline.

Usage::

    uv run python -m scripts.diag_kg_lightrag                 # 3 chunks
    uv run python -m scripts.diag_kg_lightrag --chunks 10     # 10 chunks
    uv run python -m scripts.diag_kg_lightrag --gleaning 1    # with gleaning
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY  # noqa: E402
from llama_index.core.node_parser import SentenceSplitter  # noqa: E402
from llama_index.core.schema import Document  # noqa: E402

from src.graph.lightrag_extract import LightRAGExtractor  # noqa: E402
from src.graph.merge import merge_kg_extraction  # noqa: E402
from src.retrieval.llm import build_llm  # noqa: E402
from tests.eval.medical_fixture import load_medical_source  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--chunks", type=int, default=3,
                   help="how many leading chunks to probe (default 3)")
    p.add_argument("--gleaning", type=int, default=0,
                   help="gleaning passes per chunk (default 0)")
    p.add_argument("--workers", type=int, default=2,
                   help="parallel chunks (default 2)")
    return p.parse_args()


async def main(n_chunks: int, gleaning: int, workers: int) -> None:
    text = load_medical_source()
    splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
    nodes = splitter.get_nodes_from_documents(
        [Document(text=text, metadata={"doc_id": "medical-diag"})]
    )
    probe_nodes = nodes[:n_chunks]
    print(f"corpus: {len(nodes)} total chunks; probing first {n_chunks}\n")

    llm = build_llm()
    extractor = LightRAGExtractor(
        llm=llm, num_workers=workers, gleaning_passes=gleaning,
    )
    extracted = await extractor.acall(probe_nodes)

    total_e = total_r = 0
    for i, n in enumerate(extracted, 1):
        ents = n.metadata.get(KG_NODES_KEY) or []
        rels = n.metadata.get(KG_RELATIONS_KEY) or []
        total_e += len(ents)
        total_r += len(rels)
        preview = n.get_content()[:120].replace("\n", " ")
        print(f"=== chunk {i}  entities={len(ents)}  relations={len(rels)} ===")
        print(f"  text: {preview!r}")
        for e in ents[:5]:
            d = (e.properties.get("description") or "")[:80]
            print(f"    {e.label:14s} {e.name!r:35s}  desc={d!r}")
        for r in rels[:3]:
            src = next((e.name for e in ents if e.id == r.source_id), "?")
            tgt = next((e.name for e in ents if e.id == r.target_id), "?")
            print(f"    {r.label:14s} {src!r} -> {tgt!r}")

    print(f"\nper-chunk totals: entities={total_e}  relations={total_r}\n")

    print("--- cross-chunk merge ---")
    merged_e, merged_r = await merge_kg_extraction(extracted, llm)
    print(f"unique entities after merge: {len(merged_e)}  "
          f"(reduction: {total_e}→{len(merged_e)})")
    print(f"unique relations after merge: {len(merged_r)}  "
          f"(reduction: {total_r}→{len(merged_r)})")
    summary_triggers = [e for e in merged_e if e.properties.get("mention_count", 0) > 1]
    print(f"entities seen in >1 chunk: {len(summary_triggers)}")
    for e in summary_triggers[:10]:
        d = (e.properties.get("description") or "")[:80]
        print(f"    {e.label:14s} {e.name!r:35s}  "
              f"x{e.properties['mention_count']}  desc={d!r}")


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(args.chunks, args.gleaning, args.workers))

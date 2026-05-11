"""Probe KG extraction on real medical chunks.

Splits the medical corpus into chunks (same params the worker uses)
and runs the project's KG extractor on the first few — answers
"is the extractor failing on medical English text, or did the
worker silently lose its output during the full ingest?"
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY  # noqa: E402
from llama_index.core.node_parser import SentenceSplitter  # noqa: E402
from llama_index.core.schema import Document  # noqa: E402

from src.graph.index import build_kg_extractor  # noqa: E402
from src.retrieval.llm import build_llm  # noqa: E402
from tests.eval.medical_fixture import load_medical_source  # noqa: E402


async def main(n_chunks: int = 3) -> None:
    text = load_medical_source()
    splitter = SentenceSplitter(chunk_size=512, chunk_overlap=50)
    nodes = splitter.get_nodes_from_documents(
        [Document(text=text, metadata={"doc_id": "medical-diag"})]
    )
    print(f"total chunks: {len(nodes)}; probing first {n_chunks}\n")

    llm = build_llm()
    extractor = build_kg_extractor(llm, num_workers=1)

    out = await extractor.acall(nodes[:n_chunks], show_progress=False)
    for i, n in enumerate(out, 1):
        ents = n.metadata.get(KG_NODES_KEY, [])
        rels = n.metadata.get(KG_RELATIONS_KEY, [])
        text_preview = n.get_content()[:140].replace("\n", " ")
        print(f"=== chunk {i}  entities={len(ents)} relations={len(rels)} ===")
        print(f"  text: {text_preview!r}")
        for r in rels[:6]:
            src = next((e.name for e in ents if e.id == r.source_id), "?")
            tgt = next((e.name for e in ents if e.id == r.target_id), "?")
            print(f"    {r.label:20s} {src!r:35s} -> {tgt!r}")


if __name__ == "__main__":
    asyncio.run(main())

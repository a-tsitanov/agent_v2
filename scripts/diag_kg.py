"""Deep diagnostic: try every layer of SchemaLLMPathExtractor.

Steps:
  1. Plain `llm.acomplete()` — sanity check Ollama responds.
  2. `llm.achat()` — chat-mode sanity.
  3. `llm.astructured_predict(KGSchema, prompt)` — the actual call
     extractor uses. Captures the exception explicitly (extractor
     swallows them).
  4. If structured_predict works → run extractor and dump triplets.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from llama_index.core.indices.property_graph.transformations.schema_llm import (  # noqa: E402
    DEFAULT_SCHEMA_PATH_EXTRACT_PROMPT, KG_NODES_KEY, KG_RELATIONS_KEY,
)
from llama_index.core.schema import TextNode  # noqa: E402

from src.graph.index import build_kg_extractor  # noqa: E402
from src.retrieval.llm import build_llm  # noqa: E402

TEXT = """\
Договор поставки № ДП-2024/178-К от 15.03.2024 заключён между
ООО «Северные технологии» (ИНН 7707083893) и АО «Промсервис».
Контактное лицо: Иванов Иван Петрович, телефон +7 (495) 234-56-78.
Иванов Иван Петрович работает в ООО «Северные технологии» в должности
руководителя отдела продаж и отвечает за договоры с АО «Промсервис».
Сумма договора: 4 250 000,00 руб.
"""


async def main() -> None:
    llm = build_llm()

    print("=== 1. plain acomplete ===")
    try:
        r = await llm.acomplete("Reply 'OK' in one word.")
        print("  ok:", r.text[:200])
    except Exception:
        traceback.print_exc()

    print("\n=== 2. extractor run (Simple mode, regex-parsed) ===")
    extractor = build_kg_extractor(llm, mode="simple", num_workers=1)
    node = TextNode(text=TEXT, metadata={"doc_id": "diag-1"})
    out = await extractor.acall([node], show_progress=False)
    kg_nodes = out[0].metadata.get(KG_NODES_KEY, [])
    kg_rels = out[0].metadata.get(KG_RELATIONS_KEY, [])
    print(f"  entities: {len(kg_nodes)}")
    for e in kg_nodes:
        print(f"    {(e.label or '(none)'):20s}  {e.name!r}")
    print(f"  relations: {len(kg_rels)}")
    for r in kg_rels:
        src = next((e.name for e in kg_nodes if e.id == r.source_id), "?")
        tgt = next((e.name for e in kg_nodes if e.id == r.target_id), "?")
        print(f"    {r.label:20s}  {src!r} -> {tgt!r}")


if __name__ == "__main__":
    asyncio.run(main())

"""Static map activity_name → LLM role.

Used by ``ingest_metrics_extractor.parse_activity_timings`` to write
the correct per-row ``model`` column into ``ingest_metrics``.  Keeping
this as a pure data-only module (no side effects, no settings
references) makes it import-cheap for both the workflow process and
test fixtures.

A value of ``None`` means the activity doesn't call any LLM (regex,
embedding, plain DB writes) — finalize then writes ``model=NULL``
into the row, which is honest: nothing model-specific happened.
"""

from __future__ import annotations

from typing import Final, Literal

LLMRole = Literal["extraction", "judge", "search"]


ACTIVITY_TO_ROLE: Final[dict[str, LLMRole | None]] = {
    "fetch_source":         None,           # MinIO fetch — no LLM
    "parse_and_chunk":      "extraction",   # translation path uses extraction LLM
    "index_vector":         None,           # BGE-M3 embeddings, not LLM chat
    "inject_canonical":     None,           # regex identifier injection
    "extract_kg":           "extraction",   # LightRAG KG extraction
    "merge_and_resolve":    "judge",        # ER LLM-judge + cross-chunk merge
    "build_property_graph": None,           # Neo4j writes only
    "push_wikibase":        None,           # MediaWiki REST, no LLM
    "finalize":             None,           # Postgres + cleanup only
}

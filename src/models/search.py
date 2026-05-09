"""Pydantic shapes for the search API.

Mirrors enterprise-kb's ``src/models/search.py`` so the wire format
between LightRAG and LlamaIndex builds stays comparable — the eval
script in Stage 9 reads both and compares outputs.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str

    # ── retrieval mode ────────────────────────────────────────────────
    mode: str = Field(
        "hybrid",
        description="vector | hybrid | bypass — meaning per stage.",
    )
    department: str | None = None
    top_k: int = 10
    user_id: str | None = None

    # ── metadata filters ─────────────────────────────────────────────
    doc_type_filter: str | None = None
    created_after: int | None = None
    created_before: int | None = None

    # ── response tuning ──────────────────────────────────────────────
    response_type: str = "Multiple Paragraphs"
    include_references: bool = False

    # ── agentic ──────────────────────────────────────────────────────
    agentic: bool = False
    agentic_max_rounds: int = Field(3, ge=1, le=5)


class SourceCitation(BaseModel):
    doc_id: str
    chunk_id: str
    position: int = 0
    content: str
    score: float = 0.0
    department: str = ""
    doc_type: str = ""


class AgenticRoundStat(BaseModel):
    """Per-round telemetry from ``agentic_search``.

    ``sufficient=None`` when judge was skipped (early-exit on
    no-new-info, Stage G in enterprise-kb plan vocabulary).
    """

    round: int
    query: str
    new_sources: int = 0
    new_entities: int = 0
    new_relations: int = 0
    sufficient: bool | None = None
    judge_reason: str = ""


class SearchResponse(BaseModel):
    query: str
    answer: str
    mode: str
    sources: list[SourceCitation] = Field(default_factory=list)
    latency_ms: float = 0.0
    agentic_rounds: int | None = None
    follow_up_queries: list[str] | None = None
    agentic_round_stats: list[AgenticRoundStat] | None = None


__all__ = [
    "AgenticRoundStat",
    "SearchRequest",
    "SearchResponse",
    "SourceCitation",
]

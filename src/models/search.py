"""Pydantic shapes for the search API.

The sole search surface after the R7b cutover is
``/api/v1/search/{local,global,drift,auto}`` (``src/api/routes/search_v2.py``),
which reuses ``SearchRequest`` / ``SearchResponse`` for every mode. The
legacy ReAct/Self-RAG request shapes and their telemetry models were
removed with those routes.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ConversationTurn(BaseModel):
    role: str = Field("user", description="user | assistant")
    content: str


class SearchRequest(BaseModel):
    """Search request shared by all ``/api/v1/search/*`` endpoints.

    The mode is selected by the endpoint (local / global / drift / auto),
    not by a field here — only ``query`` and ``top_k`` are consumed by the
    plan-execute / GraphRAG flows; the remaining fields are retained for
    backward-compatible clients.
    """

    query: str
    history: list[ConversationTurn] = Field(
        default_factory=list,
        description="Prior turns (client-managed). Empty = single-shot, no contextualisation.",
    )
    mode: str = Field(
        "hybrid",
        description="vector | hybrid | bypass",
    )
    department: str | None = None
    top_k: int = 10
    user_id: str | None = None
    doc_type_filter: str | None = None
    created_after: int | None = None
    created_before: int | None = None
    response_type: str = "Multiple Paragraphs"
    include_references: bool = False
    # Optional answer-shape template — a named template
    # (prompts/answer_templates/<name>.md) or an inline template string.
    # Empty/None → default Russian-output synthesis (unchanged).
    answer_template: str | None = None


class SourceCitation(BaseModel):
    doc_id: str
    chunk_id: str
    position: int = 0
    content: str
    score: float = 0.0
    department: str = ""
    doc_type: str = ""


class DocumentRef(BaseModel):
    doc_id: str
    url: str


class SearchResponse(BaseModel):
    query: str
    answer: str
    mode: str
    sources: list[SourceCitation] = Field(default_factory=list)
    documents: list[DocumentRef] = Field(default_factory=list)
    latency_ms: float = 0.0


__all__ = [
    "DocumentRef",
    "SearchRequest",
    "SearchResponse",
    "SourceCitation",
]

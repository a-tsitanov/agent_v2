"""Pydantic shapes for the search API.

The sole search surface after the R7b cutover is
``/api/v1/search/{local,global,drift,auto}`` (``src/api/routes/search_v2.py``),
which reuses ``SearchRequest`` / ``SearchResponse`` for every mode. The
legacy ReAct/Self-RAG request shapes and their telemetry models were
removed with those routes.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator


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

    query: str = Field(min_length=1, description="User query (non-empty).")
    history: list[ConversationTurn] = Field(
        default_factory=list,
        max_length=50,
        description="Prior turns (client-managed). Empty = single-shot, no contextualisation.",
    )
    mode: str = Field(
        "hybrid",
        description="RESERVED — not applied by the current retrieval flow.",
    )
    # Reserved filter fields: accepted for backward-compatible clients but
    # NOT applied by the plan-execute / GraphRAG flows.  They are NOT an
    # access-control boundary — do not rely on them for scoping.  See #11.
    department: str | None = Field(
        default=None, description="RESERVED — not applied (not an access boundary).",
    )
    top_k: int = Field(default=10, ge=1, le=100, description="Results to retrieve (1–100).")
    user_id: str | None = Field(
        default=None, description="RESERVED — not applied (not an access boundary).",
    )
    doc_type_filter: str | None = Field(
        default=None, description="RESERVED — not applied.",
    )
    # Date filters (applied; see date-filters spec). ISO YYYY-MM-DD, each
    # bound independent + optional, combined with AND. `created_*` filter the
    # INSERTION date; `doc_date_*` the client-provided document date. (They
    # were previously RESERVED int fields, never applied.)
    created_after: str | None = Field(
        default=None, description="Insertion-date lower bound, ISO YYYY-MM-DD.",
    )
    created_before: str | None = Field(
        default=None, description="Insertion-date upper bound, ISO YYYY-MM-DD.",
    )
    doc_date_after: str | None = Field(
        default=None, description="Document-date lower bound, ISO YYYY-MM-DD.",
    )
    doc_date_before: str | None = Field(
        default=None, description="Document-date upper bound, ISO YYYY-MM-DD.",
    )
    response_type: str = "Multiple Paragraphs"
    include_references: bool = False
    # Optional answer-shape template — a named template
    # (prompts/answer_templates/<name>.md) or an inline template string.
    # Empty/None → default Russian-output synthesis (unchanged).
    answer_template: str | None = None
    groups: list[str] | None = Field(
        default=None,
        description="Include-list of channel groups; None/[] = all groups.",
    )
    exclude_groups: list[str] | None = Field(
        default=None,
        description="Exclude-list of channel groups (mutually exclusive with groups).",
    )
    # When false the final large-model synthesis is skipped and `answer`
    # comes back empty; everything else in the response is unchanged.
    # For callers that compose their own answer from `sources` and only
    # pay for retrieval.
    synthesize: bool = Field(
        default=True,
        description="Run the final answer synthesis (default). False returns retrieval only.",
    )

    @field_validator(
        "created_after", "created_before", "doc_date_after", "doc_date_before",
    )
    @classmethod
    def _validate_iso_date(cls, v: str | None) -> str | None:
        """Reject a malformed date bound → 422 (FastAPI maps ValueError)."""
        if v is None:
            return v
        try:
            date.fromisoformat(v)
        except ValueError as exc:
            raise ValueError(f"must be ISO YYYY-MM-DD: {exc}") from exc
        return v

    @field_validator("groups", "exclude_groups")
    @classmethod
    def _validate_groups(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        from src.retrieval.groups import GROUP_SET
        bad = [g for g in v if g not in GROUP_SET]
        if bad:
            raise ValueError(f"unknown group(s) {bad}; allowed: {sorted(GROUP_SET)}")
        return v

    @model_validator(mode="after")
    def _groups_xor_exclude(self):
        if self.groups and self.exclude_groups:
            raise ValueError("groups and exclude_groups are mutually exclusive")
        return self


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

"""Pydantic shapes for the search API.

Three search endpoints share most of the response shape:
  * ``SearchRequest`` — plain hybrid retrieve + synthesize.
  * ``AgentSearchRequest`` — ReAct agent (R7).
  * ``SelfRAGSearchRequest`` — ReAct + reflective synthesis (R8).

Telemetry models (`AgenticRoundStat`, `AgenticStepStat`,
`ReflectiveCitation`, `ReflectiveUncertainty`) attach to
``SearchResponse`` when the endpoint produced them.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    """`/api/v1/search` — hybrid retrieve + single synthesize.

    The ``agentic`` / ``agentic_max_rounds`` fields are kept as the
    transition contract; R4 splits agentic dispatch into the
    `/agent` and `/selfrag` endpoints and removes these fields.
    """

    query: str
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

    # legacy — removed in R4 when route split lands
    agentic: bool = False
    agentic_max_rounds: int = Field(3, ge=1, le=5)


class AgentSearchRequest(BaseModel):
    """`/api/v1/agent` — ReAct agent with tool calls (R7)."""

    query: str
    department: str | None = None
    top_k: int = 10
    user_id: str | None = None
    doc_type_filter: str | None = None
    created_after: int | None = None
    created_before: int | None = None
    response_type: str = "Multiple Paragraphs"
    include_references: bool = False
    max_iterations: int = Field(8, ge=1, le=20)


class SelfRAGSearchRequest(AgentSearchRequest):
    """`/api/v1/selfrag` — ReAct + reflective synthesis (R8)."""

    max_refinements: int = Field(3, ge=0, le=10)


class SourceCitation(BaseModel):
    doc_id: str
    chunk_id: str
    position: int = 0
    content: str
    score: float = 0.0
    department: str = ""
    doc_type: str = ""


class AgenticRoundStat(BaseModel):
    """Per-round telemetry from the legacy judge-based loop.

    ``sufficient=None`` when judge was skipped (early-exit on
    no-new-info).  Kept for legacy `agentic_search` until R10.
    """

    round: int
    query: str
    new_sources: int = 0
    new_entities: int = 0
    new_relations: int = 0
    sufficient: bool | None = None
    judge_reason: str = ""


class AgenticStepStat(BaseModel):
    """Per-step telemetry for ReAct agent (R7).

    Replaces ``AgenticRoundStat`` for tool-call based loops —
    one entry per LLM-decision step rather than per retrieval round.
    """

    step: int
    tool_name: str
    tool_args: dict
    observation_summary: str = ""
    reasoning_excerpt: str = ""


class ReflectiveCitation(BaseModel):
    """A claim in the answer with its supporting chunk_id."""

    claim: str
    chunk_id: str


class ReflectiveUncertainty(BaseModel):
    """A part of the answer the model could not support from context."""

    topic: str
    reason: str


class ReflectiveAnswerDetail(BaseModel):
    """Structured view of a reflective synthesis (R8)."""

    citations: list[ReflectiveCitation] = Field(default_factory=list)
    uncertainties: list[ReflectiveUncertainty] = Field(default_factory=list)
    refinement_rounds: int = 0


class JudgeOutput(BaseModel):
    """Structured output of the LLM judge (R2).

    Returned by `LLMJudge.via_structured` via
    `llm.astructured_predict(JudgeOutput, prompt)`.
    """

    sufficient: bool = Field(
        ..., description="True if the retrieved context is enough to answer."
    )
    follow_up_query: str = Field(
        default="",
        description=(
            "Concise follow-up search query if context is insufficient; "
            "empty string when sufficient."
        ),
    )
    reason: str = Field(
        default="",
        description="One-sentence rationale.",
    )


class SearchResponse(BaseModel):
    query: str
    answer: str
    mode: str
    sources: list[SourceCitation] = Field(default_factory=list)
    latency_ms: float = 0.0

    # legacy judge-based loop (R10 will retire or flag-gate)
    agentic_rounds: int | None = None
    follow_up_queries: list[str] | None = None
    agentic_round_stats: list[AgenticRoundStat] | None = None

    # ReAct (R7) and Self-RAG (R8) telemetry
    agentic_step_stats: list[AgenticStepStat] | None = None
    answer_detail: ReflectiveAnswerDetail | None = None


__all__ = [
    "AgenticRoundStat",
    "AgenticStepStat",
    "AgentSearchRequest",
    "JudgeOutput",
    "ReflectiveAnswerDetail",
    "ReflectiveCitation",
    "ReflectiveUncertainty",
    "SearchRequest",
    "SearchResponse",
    "SelfRAGSearchRequest",
    "SourceCitation",
]

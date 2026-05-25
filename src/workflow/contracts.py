"""Payloads exchanged between the workflow and its activities.

Heavy state (list[BaseNode], EntityNode lists) NEVER travels in
payloads — it is pickled to MinIO and referenced by URI.  These
contracts carry only IDs, URIs, and small counters so the Temporal
DataConverter can JSON-serialise everything safely.

Search-side (Stage 1 of the search-mcp plan): smaller payloads.
``SerializedNode`` and ``SerializedMessage`` are deliberately tiny
projections of LlamaIndex ``NodeWithScore`` / ``ChatMessage``
respectively — only what the synthesizer needs to reconstitute
context.  Per ReAct session typically <30 nodes, <40 messages → fits
comfortably in Temporal's 2 MB payload limit even for long loops.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

GraphStatus = Literal["completed", "vector_only"]
WikibaseStatus = Literal["ok", "skipped", "failed"]
SearchMode = Literal["simple", "agent", "selfrag"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class WikibasePushed(_Frozen):
    status: WikibaseStatus
    created_items: int = 0
    updated_items: int = 0
    external_id_statements: int = 0
    relation_statements: int = 0
    new_properties_created: int = 0


class IngestParams(_Frozen):
    doc_id: str
    path: str
    # Analytics tagging — propagated end-to-end so the finalize hook
    # writes ingest_metrics rows tagged with the same labels that the
    # /ingest endpoint set via Temporal Search Attributes.  Defaults
    # match `AnalyticsSettings` so older callers without the header
    # continue to work.
    version_tag: str = "unspecified"
    model: str = ""           # global default snapshot (LITELLM_LLM_MODEL)
    # Per-role model snapshots taken at submit time so finalize can
    # write the right model into ingest_metrics per activity.  Empty
    # ⇒ that role falls back to ``model``.
    extraction_model: str = ""
    judge_model: str = ""
    search_model: str = ""
    env: str = ""


class Ctx(_Frozen):
    doc_id: str
    local_path: str
    cleanup_dir: str | None
    workflow_run_id: str


class Parsed(_Frozen):
    ctx: Ctx
    nodes_uri: str
    chunk_count: int


class Indexed(_Frozen):
    node_ids: list[str]
    count: int


class Injected(_Frozen):
    count: int


class EntitySample(_Frozen):
    """Compact entity representation suitable for inclusion in
    activity results (kept small so Temporal UI doesn't truncate)."""

    name: str
    label: str


class RelationSample(_Frozen):
    source: str
    target: str
    label: str


class DuplicateGroup(_Frozen):
    """Pre-merge entity name that appeared `count` times across
    chunks — exactly the candidates the merger collapses."""

    name: str
    count: int
    labels: list[str] = []


class KGExtracted(_Frozen):
    parsed: Parsed
    nodes_with_kg_uri: str
    entity_count: int = 0
    relation_count: int = 0
    entity_labels_top: dict[str, int] = {}
    relation_labels_top: dict[str, int] = {}
    sample_entities: list[EntitySample] = []
    sample_relations: list[RelationSample] = []


class Merged(_Frozen):
    kg: KGExtracted
    merged_entities_uri: str
    raw_entity_count: int = 0
    merged_entity_count: int = 0
    relation_count: int = 0
    duplicate_groups: list[DuplicateGroup] = []
    phones_collapsed: int = 0
    phone_alias_map: dict[str, str] = {}
    er_merged: int = 0
    er_alias_map: dict[str, str] = {}


class GraphBuilt(_Frozen):
    entities: int
    relations: int


class GraphBuildResult(_Frozen):
    """Output of the ``GraphBuildWorkflow`` child — bundles the
    ``Merged`` staging-blob descriptor (needed by the post-graph
    ``push_wikibase`` activity in the parent) with the ``GraphBuilt``
    counters for ``finalize``.  Composition keeps both upstream fields
    intact instead of grafting fields onto an existing contract."""

    merged: Merged
    built: GraphBuilt


class FinalizeIn(_Frozen):
    ctx: Ctx
    indexed: Indexed
    graph_status: GraphStatus
    entities: int = 0
    relations: int = 0
    wikibase: WikibasePushed | None = None
    # Analytics tags propagated from IngestParams so the finalize-side
    # metrics-extractor hook labels every ingest_metrics row.
    version_tag: str = "unspecified"
    model: str = ""            # default fallback (LITELLM_LLM_MODEL)
    extraction_model: str = ""
    judge_model: str = ""
    search_model: str = ""
    env: str = ""


class MarkFailedIn(_Frozen):
    ctx: Ctx | None
    params: IngestParams
    error: str


class IngestResult(_Frozen):
    doc_id: str
    chunk_count: int
    graph_status: GraphStatus
    entities: int = 0
    relations: int = 0
    wikibase_status: WikibaseStatus = "skipped"


# ══════════════════════════════════════════════════════════════════
#  Search workflow payloads (Stage 1 of the search-mcp plan)
# ══════════════════════════════════════════════════════════════════


class SerializedNode(_Frozen):
    """Wire-friendly projection of LlamaIndex ``NodeWithScore``.

    Only the bits the synthesizer needs to reconstruct context.
    Reconstructed inside activities via ``TextNode(id_=..., text=...,
    metadata=...)`` wrapped in ``NodeWithScore(node=..., score=...)``.
    """

    chunk_id: str
    text: str
    score: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)


class SerializedToolCall(_Frozen):
    """One tool call an assistant turn requested.

    Mirrors the OpenAI ``tool_calls`` entry shape so serde can rebuild
    a valid function-calling history: every TOOL message must be
    preceded by the ASSISTANT message whose ``tool_calls`` it answers.
    """

    id: str
    name: str
    arguments: str  # JSON-encoded kwargs


class SerializedMessage(_Frozen):
    """Wire-friendly projection of LlamaIndex ``ChatMessage``."""

    role: Literal["system", "user", "assistant", "tool"]
    content: str = ""
    tool_call_id: str = ""  # for TOOL messages — which call this answers
    name: str = ""  # for TOOL messages — the function name
    # for ASSISTANT messages — the tool call(s) this turn requested, so
    # the next reasoning step sees a valid assistant→tool pairing
    # instead of an orphan tool observation.
    tool_calls: list[SerializedToolCall] = Field(default_factory=list)


class ToolSpec(_Frozen):
    """LLM-visible tool description: just the name + prose for the
    function-calling prompt.  The activity uses these as stub
    ``FunctionTool`` entries so ``llm.achat_with_tools`` sees the
    tools menu — but the tool bodies are never executed in-process
    (Workflow dispatches via ``tool_execution`` activity instead).
    """

    name: str
    description: str


# ── inputs / outputs for activities ─────────────────────────────


class SearchParams(_Frozen):
    """Workflow input — what the API route / MCP server submits."""

    query: str
    mode: SearchMode = "agent"
    max_iterations: int = 8
    max_refinements: int = 3
    request_id: str = ""
    # Observation distillation knobs, resolved from AgentSettings at
    # submit time and propagated here so the workflow never reads env
    # at runtime (replay-safe).  Defaults mirror AgentSettings.
    distill_enabled: bool = True
    distill_min_chars: int = 1500
    observation_max_chars: int = 6000
    # Analytics: same shape as IngestParams so the same Search
    # Attributes (VersionTag, Model, Env, …) propagate to Temporal.
    version_tag: str = "unspecified"
    env: str = ""


class ReasoningParams(_Frozen):
    """Input to the ``agent_reasoning_step`` activity."""

    messages: list[SerializedMessage]
    tools: list[ToolSpec]


class AgentDecision(_Frozen):
    """Output of ``agent_reasoning_step``.

    ``tool_name == "submit_answer"`` is the sentinel the workflow
    treats as terminal — no tool_execution is invoked, control
    drops straight to synthesize_answer.
    """

    tool_name: str
    tool_kwargs: dict[str, Any] = Field(default_factory=dict)
    tool_call_id: str = ""        # so we can build the matching TOOL message
    raw_text: str = ""             # for Temporal-history debug
    finished_no_call: bool = False  # LLM gave up on tool calling


class ToolCallParams(_Frozen):
    """Input to the ``tool_execution`` activity."""

    tool_name: str
    tool_kwargs: dict[str, Any] = Field(default_factory=dict)
    # Only populated for ``filter_by_metadata`` (which filters the
    # accumulator) — empty for retrieval tools to keep payloads small.
    accumulated_sources: list[SerializedNode] = Field(default_factory=list)


class ToolCallResult(_Frozen):
    """Output of ``tool_execution``."""

    tool_name: str
    observation: str  # JSON-string (or raw text for read_full_document)
    sources_added: list[SerializedNode] = Field(default_factory=list)
    duration_ms: int = 0
    error: str = ""  # non-empty if the tool failed, empty on success


Relevance = Literal["relevant", "partial", "irrelevant"]


class DistillParams(_Frozen):
    """Input to the ``distill_observation`` activity.

    Carries the raw (large) tool observation plus the user query so the
    distiller can extract only query-relevant facts and grade relevance.
    """

    query: str
    tool_name: str
    observation: str


class DistillResult(_Frozen):
    """Output of ``distill_observation``.

    ``distilled`` is the compact, query-focused text that goes into the
    agent's reasoning history (bounding context growth).  ``relevance``
    gates whether this step's sources are kept in the accumulator.
    """

    distilled: str
    relevance: Relevance = "partial"


class SynthesizeParams(_Frozen):
    """Input to the ``synthesize_answer`` activity."""

    query: str
    mode: SearchMode
    accumulated: list[SerializedNode] = Field(default_factory=list)
    max_refinements: int = 3


class ReflectiveCitationDict(_Frozen):
    claim: str
    chunk_id: str


class ReflectiveUncertaintyDict(_Frozen):
    topic: str
    reason: str


class SynthesizeResult(_Frozen):
    """Output of ``synthesize_answer``."""

    text: str
    citations: list[ReflectiveCitationDict] = Field(default_factory=list)
    uncertainties: list[ReflectiveUncertaintyDict] = Field(default_factory=list)
    refinement_rounds: int = 0


class AgenticStepStatDict(_Frozen):
    step: int
    tool_name: str
    tool_args: dict[str, Any] = Field(default_factory=dict)
    observation_summary: str = ""
    relevance: str = ""  # distiller verdict: relevant/partial/irrelevant


class SearchOutcome(_Frozen):
    """Final workflow output — mapped onto SearchResponse by route handler."""

    query: str
    mode: SearchMode
    answer: str
    sources: list[SerializedNode] = Field(default_factory=list)
    step_stats: list[AgenticStepStatDict] = Field(default_factory=list)
    citations: list[ReflectiveCitationDict] = Field(default_factory=list)
    uncertainties: list[ReflectiveUncertaintyDict] = Field(default_factory=list)
    refinement_rounds: int = 0
    latency_ms: int = 0

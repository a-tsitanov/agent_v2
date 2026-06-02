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
# "local" is the R2 plan-execute path (SearchOrchestratorWorkflow);
# "global"/"drift" are the R7a GraphRAG routing modes (GlobalSearchWorkflow).
# "simple" is not a user-facing mode — it's the SynthesizeParams branch
# selector the orchestrator passes for plain synthesis.  (The legacy
# "agent"/"selfrag" modes were removed with the R7b cutover.)
SearchMode = Literal["simple", "local", "global", "drift"]


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


# ── inputs / outputs for activities ─────────────────────────────
#
# R7b cutover: the legacy ReAct ``SearchWorkflow`` and its exclusive
# contracts were removed — ``SearchParams``, ``ReasoningParams``,
# ``AgentDecision``, ``ToolCallParams``/``ToolCallResult``,
# ``DistillParams``/``DistillResult``, ``ToolSpec``,
# ``SerializedMessage``/``SerializedToolCall`` and the ``Relevance``
# alias.  Everything below is used by the plan-execute / GraphRAG path.


class CoverageParams(_Frozen):
    """Input to the ``coverage_check`` activity.

    Asks whether the evidence gathered so far is enough to FULLY answer
    the query (all its parts) before the agent is allowed to finish.
    """

    query: str
    evidence: str


class CoverageResult(_Frozen):
    """Output of ``coverage_check``.

    ``complete`` — is the gathered evidence sufficient to fully answer?
    ``missing`` — when not complete, a short description of what still
    needs to be retrieved (drives one more loop iteration).  Fail-open:
    on any doubt/parse failure the activity returns ``complete=True`` so
    a flaky check can never trap the agent.
    """

    complete: bool = True
    missing: str = ""


class SynthesizeParams(_Frozen):
    """Input to the ``synthesize_answer`` activity."""

    query: str
    mode: SearchMode
    accumulated: list[SerializedNode] = Field(default_factory=list)
    max_refinements: int = 3
    # R2 plan-execute flow synthesises on the large tier
    # (``build_synthesis_llm``); legacy ReAct paths leave this False and
    # keep the small search-tier synthesizer for backward compatibility.
    use_synthesis_llm: bool = False


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


# ══════════════════════════════════════════════════════════════════
#  Plan-execute search payloads (Search R2)
# ══════════════════════════════════════════════════════════════════


class PlanParams(_Frozen):
    """Input to the ``plan_subquestions`` activity.

    Decomposes a compound question into atomic sub-questions (small
    planner model).  ``max_subqueries`` bounds the parallel fan-out.
    """

    query: str
    max_subqueries: int = 5


class PlanResult(_Frozen):
    """Output of ``plan_subquestions`` — always ≥1 sub-question
    (``[query]`` for atomic questions / on planner failure)."""

    subquestions: list[str] = Field(default_factory=list)


class RetrieveParams(_Frozen):
    """Input to the ``retrieve_subquestion`` activity.

    One deterministic retrieval step (hybrid vector + graph) for a
    single sub-question.  No tool selection / no LLM reasoning here.
    """

    subquestion: str
    top_k: int = 10


class RetrieveResult(_Frozen):
    """Output of ``retrieve_subquestion`` — sources gathered for one
    sub-question, already deduped by chunk_id within the step."""

    subquestion: str
    sources: list[SerializedNode] = Field(default_factory=list)
    duration_ms: int = 0
    error: str = ""


class RerankParams(_Frozen):
    """Input to the ``rerank_sources`` activity (Search R5).

    The merged graph+vector pool (already deduped by chunk_id across
    sub-questions) plus the original user query.  The activity runs the
    bge cross-encoder over the UNIFIED pool and returns the top-N.
    """

    query: str
    sources: list[SerializedNode] = Field(default_factory=list)
    top_n: int = 5


class RerankResult(_Frozen):
    """Output of ``rerank_sources`` — the reranked top-N pool."""

    sources: list[SerializedNode] = Field(default_factory=list)


class SubQueryParams(_Frozen):
    """Input to ``SubQueryRetrievalWorkflow`` — one sub-question."""

    subquestion: str
    top_k: int = 10


class SubQueryResult(_Frozen):
    """Output of ``SubQueryRetrievalWorkflow`` — deduped sources."""

    subquestion: str
    sources: list[SerializedNode] = Field(default_factory=list)


class OrchestratorParams(_Frozen):
    """Workflow input for ``SearchOrchestratorWorkflow`` — what the
    ``/search/local`` route submits."""

    query: str
    max_subqueries: int = 5
    top_k: int = 10
    max_refinements: int = 3
    # Pre-synthesis coverage gate (R4) — resolved from AgentSettings at
    # submit time and propagated here so the workflow never reads env at
    # runtime (replay-safe).  Defaults mirror AgentSettings.
    coverage_check_enabled: bool = True
    max_coverage_rounds: int = 1


class SearchOutcome(_Frozen):
    """Final workflow output — mapped onto SearchResponse by route handler."""

    query: str
    mode: SearchMode
    answer: str
    sources: list[SerializedNode] = Field(default_factory=list)
    documents: list[str] = Field(default_factory=list)
    step_stats: list[AgenticStepStatDict] = Field(default_factory=list)
    citations: list[ReflectiveCitationDict] = Field(default_factory=list)
    uncertainties: list[ReflectiveUncertaintyDict] = Field(default_factory=list)
    refinement_rounds: int = 0
    latency_ms: int = 0


# ── offline community build (Search R6) ─────────────────────────────


class CommunityRef(_Frozen):
    """One detected community — the cross-activity handle the workflow
    fans out over for summarisation.  ``members`` are entity NAMES (the
    ``__Entity__.name`` primary key used everywhere else in the graph).
    """

    community_id: int
    level: int = 0
    members: list[str] = Field(default_factory=list)

    @property
    def member_count(self) -> int:
        return len(self.members)


class DetectCommunitiesParams(_Frozen):
    """Input to ``detect_communities_activity`` — GDS Leiden detection.

    ``min_size`` drops communities below the threshold (noise); ``level``
    tags the written ``:Community`` nodes (single-level for R6, kept for a
    future hierarchical pass).
    """

    min_size: int = 3
    level: int = 0


class DetectCommunitiesResult(_Frozen):
    """Output of ``detect_communities_activity`` — the communities to
    summarise.  Empty on any GDS / store error (fail-safe)."""

    communities: list[CommunityRef] = Field(default_factory=list)


class SummarizeCommunityParams(_Frozen):
    """Input to ``summarize_community_activity`` — summarise ONE
    community's members (+ their inter-member relations) via the small
    tier and persist on ``:Community.summary``."""

    community_id: int
    level: int = 0
    members: list[str] = Field(default_factory=list)


class SummarizeCommunityResult(_Frozen):
    """Output of ``summarize_community_activity`` — the summary text and
    whether it was persisted.  ``summary`` is empty on any error."""

    community_id: int
    summary: str = ""
    persisted: bool = False


class CommunityBuildResult(_Frozen):
    """Final ``CommunityBuildWorkflow`` output — counts only (the data
    lives on the ``:Community`` nodes in Neo4j)."""

    detected: int = 0
    summarized: int = 0


# ══════════════════════════════════════════════════════════════════
#  Query routing + GraphRAG global search (Search R7a)
# ══════════════════════════════════════════════════════════════════


# "local"  — a specific / factual question best answered from concrete
#            chunks (the R2–R5 plan-execute flow).
# "global" — a corpus-level / thematic / aggregate question best answered
#            by map-reducing over community summaries (GraphRAG global).
# "drift"  — a complex / mixed question: run local first, then append the
#            top community summaries as extra synthesis context.
RouteLabel = Literal["local", "global", "drift"]


class RouteParams(_Frozen):
    """Input to the ``route_query`` activity — the raw user question."""

    query: str


class RouteResult(_Frozen):
    """Output of ``route_query`` — the chosen search mode.

    Fail-safe: any classifier/LLM error or unparseable reply yields
    ``route="local"`` (the safe default) so a flaky router can never
    break search.  ``reason`` is advisory (telemetry only)."""

    route: RouteLabel = "local"
    reason: str = ""


class CommunitySummaryRef(_Frozen):
    """One community's stored summary — the unit the global MAP step
    produces a partial answer over.  Read from ``:Community.summary``."""

    community_id: int
    level: int = 0
    summary: str = ""


class MapCommunitiesParams(_Frozen):
    """Input to the ``map_communities`` activity — fetch community
    summaries to map over for a global question.

    ``level`` selects the community level to read; ``limit`` bounds how
    many summaries enter the (parallel) MAP step so a huge corpus doesn't
    fan out unbounded."""

    query: str
    level: int = 0
    limit: int = 20


class MapCommunitiesResult(_Frozen):
    """Output of ``map_communities`` — the community summaries to map
    over.  Empty on any store error (fail-safe → global yields a
    no-evidence answer rather than raising)."""

    communities: list[CommunitySummaryRef] = Field(default_factory=list)


class MapPartialParams(_Frozen):
    """Input to ``map_community_partial`` — produce a partial answer for
    ONE community summary against the user query (small tier)."""

    query: str
    community_id: int
    summary: str = ""


class MapPartialResult(_Frozen):
    """Output of ``map_community_partial`` — the per-community partial
    answer + a self-rated relevance score (0..1) used to drop irrelevant
    communities before REDUCE.  Fail-safe: empty partial on any error."""

    community_id: int
    partial: str = ""
    score: float = 0.0


class DocumentsForCommunitiesParams(_Frozen):
    """Input to ``documents_for_communities`` — community ids to resolve
    back to their source documents."""

    community_ids: list[int] = Field(default_factory=list)


class DocumentsForCommunitiesResult(_Frozen):
    """Output — distinct source doc_ids behind the given communities."""

    doc_ids: list[str] = Field(default_factory=list)


class GlobalSearchParams(_Frozen):
    """Workflow input for ``GlobalSearchWorkflow`` — what the
    ``/search/global`` route (and the drift path) submit."""

    query: str
    level: int = 0
    max_communities: int = 20
    map_parallelism: int = 4
    max_refinements: int = 3
    # Drift mode (R7a): when True, the workflow's REDUCE label is "drift"
    # and the partials are MERGED with caller-supplied local sources
    # rather than standing alone.  Plain global leaves this False.
    drift_mode: bool = False

"""Centralized configuration for kb-llamaindex.

All settings are loaded via ``pydantic-settings`` v2 with per-subsystem
nested classes, each with its own env-var prefix.  The composed
:class:`Settings` lives at module level — import via
``from src.config import settings``.

Pattern mirrors ``enterprise-kb/src/config.py`` so anyone familiar with
the existing project can navigate this one without surprises.
"""

from __future__ import annotations

from functools import cached_property
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Two physical model tiers operators actually manage:
#   * ``small`` — local, high-volume (extraction, judge, search, plan, …)
#   * ``large`` — final user-facing synthesis only.
LLMTier = Literal["small", "large"]
# Logical workloads.  Each role maps to a tier via ``_DEFAULT_ROLE_TIERS``
# (overridable per role through ``LITELLM_ROLE_TIERS``).
LLMRole = Literal[
    "extraction",
    "judge",
    "search",
    "route",
    "plan",
    "retrieve",
    "distill",
    "coverage",
    "synthesis",
]

# Declarative role→tier map.  Everything runs on the small/local model
# EXCEPT the final answer synthesis which gets the large model.  Operators
# escalate any single role with ``LITELLM_ROLE_TIERS='{"plan":"large"}'``.
_DEFAULT_ROLE_TIERS: dict[str, LLMTier] = {
    "extraction": "small",
    "judge": "small",
    "search": "small",
    "route": "small",
    "plan": "small",
    "retrieve": "small",
    "distill": "small",
    "coverage": "small",
    "synthesis": "large",
}


# ── per-subsystem settings ───────────────────────────────────────────


class ApiSettings(BaseSettings):
    """FastAPI surface — host, port, auth keys, CORS, log level."""

    model_config = SettingsConfigDict(env_prefix="API_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000
    env: str = "development"
    log_level: str = "info"
    log_json: bool = False
    keys: str = "dev-local-key"
    cors_origins: str = "*"
    upload_dir: str = "/app/data/uploads"

    @cached_property
    def keys_list(self) -> list[str]:
        return [k.strip() for k in self.keys.split(",") if k.strip()]

    @cached_property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


class MilvusSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MILVUS_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 19530
    collection: str = "kb_llamaindex"
    timeout_s: float = 10.0
    dim: int = 768

    @cached_property
    def uri(self) -> str:
        return f"http://{self.host}:{self.port}"


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEO4J_", env_file=".env", extra="ignore")

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: SecretStr = SecretStr("changeme")
    database: str = "neo4j"
    timeout_s: float = 30.0


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    db: str = "kb_llamaindex"
    user: str = "postgres"
    password: SecretStr = SecretStr("postgres")
    connect_timeout_s: int = 10

    @cached_property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class LiteLLMSettings(BaseSettings):
    """Connection to a LiteLLM proxy (or any OpenAI-compatible endpoint).

    ``llama-index-llms-openai-like`` and
    ``llama-index-embeddings-openai-like`` both speak this protocol —
    same wire format as enterprise-kb uses, just wired through LlamaIndex
    abstractions instead of direct LangChain.
    """

    model_config = SettingsConfigDict(env_prefix="LITELLM_", env_file=".env", extra="ignore")

    base_url: str = "http://localhost:4000"
    api_key: SecretStr = SecretStr("sk-litellm-stub")
    # ── two physical model tiers ─────────────────────────────────────
    # Operators manage exactly two model names.  Every logical role maps
    # to one of these via ``role_tiers`` (see ``_DEFAULT_ROLE_TIERS``).
    #   * small — local, high-volume.  Default gemma4:e4b: cheap/fast and
    #     reliable enough for extraction/judge/search/plan/etc.
    #   * large — final user-facing synthesis only.  Default gpt-4o-mini.
    # Escalate a single role to large via
    # ``LITELLM_ROLE_TIERS='{"plan":"large"}'``.  See docs/MODELS.md.
    model_small: str = "gpt-4o-mini"
    model_large: str = "gpt-4o-mini"
    # Provided overrides are MERGED onto ``_DEFAULT_ROLE_TIERS`` so an
    # operator can escalate one role (e.g. ``{"plan": "large"}``) without
    # having to re-declare every other role's tier.
    role_tiers: dict[str, LLMTier] = Field(default_factory=lambda: dict(_DEFAULT_ROLE_TIERS))
    # DEPRECATED alias.  Kept (defaulting to "") only so the legacy
    # ``build_llm()`` no-role path and any unmigrated reader resolves to
    # a model.  Empty ⇒ callers fall back to ``model_small`` via
    # ``effective_base``.  Remove once all readers use the tier fields.
    llm_model: str = ""
    embedding_model: str = "nomic-embed-text"
    embedding_dim: int = 768
    timeout_s: float = 900.0
    max_retries: int = 2

    @field_validator("role_tiers", mode="before")
    @classmethod
    def _merge_role_tiers(cls, v: object) -> dict[str, str]:
        """Merge any provided overrides onto the full default map so a
        partial ``role_tiers`` (e.g. ``{"plan": "large"}``) escalates one
        role while every other role keeps its default tier.  Accepts a
        JSON string (pydantic-settings env) or a dict."""
        merged: dict[str, str] = dict(_DEFAULT_ROLE_TIERS)
        if v is None:
            return merged
        if isinstance(v, str):
            import json

            v = json.loads(v) if v.strip() else {}
        if isinstance(v, dict):
            merged.update(v)
        return merged

    @property
    def effective_base(self) -> str:
        """Base model for the no-role legacy path: the deprecated
        ``llm_model`` if explicitly set, else the small tier."""
        return self.llm_model or self.model_small

    def tier_for(self, role: LLMRole) -> LLMTier:
        """Resolve ``role`` to a physical tier.  Unknown roles → small."""
        return self.role_tiers.get(role, "small")

    def model_for(self, role: LLMRole) -> str:
        """Resolve ``role`` → tier → one of the two physical models."""
        return self.model_large if self.tier_for(role) == "large" else self.model_small


class TemporalSettings(BaseSettings):
    """Temporal worker / client connection settings."""

    model_config = SettingsConfigDict(env_prefix="TEMPORAL_", extra="ignore")

    host: str = "localhost"
    port: int = 7233
    namespace: str = "default"
    task_queue: str = "kb-ingest"
    activity_concurrency: int = 4
    staging_bucket: str = "kb-staging"

    # GPU-bound activities (LLM extract_kg + merge_and_resolve) run on
    # a separate task queue so we can cap their concurrency
    # independently of the IO-bound fast activities.  Default of 1
    # serialises LLM calls — sane for a single local GPU.  Raise on
    # multi-GPU hosts or when proxy-side batching makes parallel calls
    # safe.
    llm_task_queue: str = "kb-ingest-llm"
    llm_activity_concurrency: int = 1

    # Merge stage (GraphBuildWorkflow → merge_and_resolve +
    # build_property_graph) runs on its OWN queue so it interleaves with
    # extract_kg instead of queueing behind a burst of extracts on
    # kb-ingest-llm (head-of-line blocking starved merge under load).
    # Default 1 + llm_activity_concurrency=1 → up to ~2 concurrent LLM
    # tasks in flight (one extract lane + one merge lane); the GPU/proxy
    # is sized for that.  Raise on multi-GPU hosts.
    merge_task_queue: str = "kb-ingest-merge"
    merge_activity_concurrency: int = 1

    # Search-side activities (plan_subquestions, retrieve_subquestion,
    # coverage_check, rerank_sources, synthesize_answer) live on their own
    # task queue so operators can control GPU split between ingest and
    # search independently.  Cap ≥ 1; raise it when LLM proxy / OpenAI
    # quotas allow several parallel search sessions.
    # Renamed kb-search-llm → kb-search-small (Search R2): the queue hosts
    # the small-tier plan-execute flow (planner + parallel sub-query
    # retrieval), so the name reflects the dominant model tier rather than
    # "any LLM".  The R7b cutover removed the legacy ReAct SearchWorkflow
    # that previously shared this queue.
    search_task_queue: str = "kb-search-small"
    search_activity_concurrency: int = 4

    # Large-tier final synthesis (Search R5) runs on a dedicated queue
    # with a LOW concurrency cap so the heavyweight synthesis model is
    # never asked to serve many parallel sessions (it dominates GPU /
    # proxy budget).  The orchestrator pins ``synthesize_answer`` here
    # via ``execute_activity(task_queue=...)``; everything else (plan,
    # retrieve, coverage_check, rerank) stays on ``search_task_queue``.
    large_task_queue: str = "kb-search-large"
    large_activity_concurrency: int = 2

    # bge cross-encoder top-N for the unified graph+vector rerank pass
    # (Search R5).  Trims the merged pool to the most relevant chunks
    # before the (expensive) large-tier synthesis.
    rerank_top_n: int = 5

    # Offline graph-community build (Search R6) — GDS Leiden detection +
    # per-community batch summarisation.  Runs on its OWN dedicated queue
    # so the heavy GDS projection + batch summary work NEVER touches the
    # query hot path; an admin endpoint (and an optional Temporal Schedule)
    # is the only trigger.  Concurrency is intentionally LOW: summaries use
    # the small tier but there can be many communities, and we don't want
    # to flood the LLM proxy with a burst from a single rebuild.
    graph_build_task_queue: str = "kb-graph-build"
    graph_build_activity_concurrency: int = 2
    # Bounded parallelism for the per-community summarize fan-out inside
    # CommunityBuildWorkflow (independent of the worker-side activity cap).
    community_summary_parallelism: int = 4
    # Communities smaller than this are ignored (too small to summarise
    # meaningfully — likely noise / disconnected pairs).
    community_min_size: int = 3

    @property
    def target(self) -> str:
        return f"{self.host}:{self.port}"


class MinioSettings(BaseSettings):
    """S3-compatible upload storage.

    User uploads land in `bucket` synchronously from the ingest
    endpoint; the worker downloads them to `download_dir` before
    feeding them to the pipeline and cleans up the local copy after
    ingestion completes.  Same MinIO instance as the Milvus backend
    by default — only the bucket is separate.
    """

    model_config = SettingsConfigDict(env_prefix="MINIO_", env_file=".env", extra="ignore")

    endpoint: str = "localhost:9000"
    access_key: SecretStr = SecretStr("minioadmin")
    secret_key: SecretStr = SecretStr("minioadmin")
    bucket: str = "kb-uploads"
    secure: bool = False
    region: str = "us-east-1"
    # Where the worker stages downloaded files before processing.
    # Removed after each workflow run via the `cleanup_local` activity.
    download_dir: str = "/tmp/kb-cache"


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGESTION_", env_file=".env", extra="ignore")

    chunk_size: int = 512
    chunk_overlap: int = 50
    breakpoint_percentile: int = 95
    batch_size: int = 10
    cache_dir: str = "/app/data/ingestion_cache"
    # When True, ingest pipeline runs a per-chunk LLM translation
    # step that fills node.metadata["translated_text"] with a
    # Russian rendering of the original.  Original chunk text is
    # NOT mutated — Milvus / Neo4j :Chunk nodes keep the source
    # language for citation fidelity.  The KG extractor then reads
    # the translated text so entities + descriptions land in
    # Russian, enabling cross-lingual graph dedup.
    translate_to_russian: bool = True
    translation_concurrency: int = 4
    # How to split work between LLM translation calls.
    #   * `per_document` — translate the entire document in a single
    #     LLM call (windowed only if it exceeds the threshold).
    #     Best translation quality (full cross-sentence context),
    #     fewest calls, but requires the document text + prompt to
    #     fit in the model context.
    #   * `per_chunk` — translate each chunk independently after the
    #     splitter.  Fits any document size, parallelisable, but
    #     loses cross-chunk context.
    #   * `auto` (default) — per_document when the doc is under
    #     `translation_doc_threshold_chars`, per_chunk otherwise.
    translation_strategy: str = "auto"
    # Soft cap (in chars) for the per-document single-call
    # translation path.  Above this, the document is split into
    # paragraph-aligned windows before being fed to the LLM.
    #
    # The default is tuned for the production target — Ollama
    # qwen3 with the native 32k-token context (qwen3:8b / 14b /
    # 32b all default to 32k).  Budget per call:
    #   input  = prompt (~500 tok) + doc window (X tok)
    #   output = ~1.3 × X (EN→RU expansion)
    #   total  = 500 + 2.3 × X  must stay under 32k
    # → safe X ≈ 8-10k tokens ≈ 30-40k chars.  30k leaves headroom
    # for long sentences and is robust against tokenization quirks.
    #
    # For OpenAI gpt-4o-mini (128k ctx) you can bump this to
    # 200_000-400_000 via env to slash the number of windows and
    # squeeze more cross-sentence context per call:
    #   INGESTION_TRANSLATION_DOC_THRESHOLD_CHARS=200000
    # Larger Ollama contexts (YaRN extended qwen3 to 131k) similarly.
    translation_doc_threshold_chars: int = 30_000
    # When True, swap the default SentenceSplitter for a
    # SemanticSplitterNodeParser that embeds each sentence and cuts
    # the document at high-distance boundaries (topic shifts) instead
    # of at fixed token counts.  Trades extra embedding calls per
    # ingest (~1 per sentence) for chunks aligned with semantic
    # structure — typically better retrieval precision on documents
    # with heterogeneous sections.  See docs/DEPLOYMENT.md.
    semantic_chunking: bool = False
    # GLiNER span-NER model used by the OPT-IN ``gliner`` /
    # ``gliner+llm`` extractor modes (see
    # ``src/graph/index.py:build_kg_extractor``).  The ``gliner`` extra
    # must be installed for those modes; the default extraction path
    # (``lightrag``) never touches this.
    gliner_model: str = "urchade/gliner_multi-v2.1"


class HFSettings(BaseSettings):
    """Offline HuggingFace model loading for air-gapped deploys.

    Two project models are pulled from the HuggingFace Hub on first use:
    the GLiNER span-NER model (``settings.ingestion.gliner_model``) and
    the BGE cross-encoder reranker (``rerank_model`` below).  In an
    air-gapped deploy those weights must already live in a local HF
    cache; ``scripts/download_models.py`` pre-populates it online and
    ``src/retrieval/hf_offline.py:configure_hf`` flips the standard HF
    env vars so the loaders read from the cache only.

    EXPLICIT env names (no shared prefix): each field binds to exactly
    one env var via ``validation_alias`` so we NEVER accidentally bind
    HuggingFace's OWN ``HF_HOME`` / ``HF_HUB_OFFLINE`` — those belong to
    the HF libraries and ``configure_hf`` sets them itself.
    """

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        extra="ignore",
    )

    offline: bool = Field(default=False, validation_alias="HF_OFFLINE")
    cache_dir: str | None = Field(default=None, validation_alias="HF_CACHE_DIR")
    rerank_model: str = Field(
        default="BAAI/bge-reranker-v2-m3",
        validation_alias="HF_RERANK_MODEL",
    )


class WikibaseSettings(BaseSettings):
    """Self-hosted Wikibase populator settings.

    When ``enabled=True``, ``DocumentIngestWorkflow`` calls
    ``push_wikibase`` after a successful graph build and pushes
    canonical entities + typed relations + identifier statements
    into Wikibase.  Default disabled — operator opts in after
    running ``scripts/setup_wikibase.py`` to bootstrap the bot
    user and the base-class Items.
    """

    model_config = SettingsConfigDict(
        env_prefix="WIKIBASE_",
        env_file=".env",
        extra="ignore",
    )

    enabled: bool = False
    base_url: str = "http://localhost:8181"
    bot_user: str = "KbBot"
    bot_password: SecretStr = SecretStr("botpass")
    language: str = "ru"
    timeout_s: float = 30.0


class AgentSettings(BaseSettings):
    """Knobs for the agentic search endpoints (`/agent`, `/selfrag`)."""

    model_config = SettingsConfigDict(env_prefix="AGENT_", env_file=".env", extra="ignore")

    # Legacy judge-based loop (kept for R9 baseline eval).
    max_rounds: int = Field(default=3, ge=1, le=10)
    top_k: int = 10
    # ReAct loop (R7): how many tool-call iterations before forcing
    # `submit_answer`.
    max_iterations: int = Field(default=8, ge=1, le=20)
    # Plan-execute flow (R2): max sub-questions the planner may emit —
    # bounds the parallel SubQueryRetrievalWorkflow fan-out (and planner
    # LLM cost) regardless of what the small model returns.
    max_subqueries: int = Field(default=5, ge=1, le=20)
    # Entity Resolution (cross-language / multi-form dedup).  When
    # enabled, the worker runs an extra step between
    # `merge_kg_extraction` and `PropertyGraphIndex` that finds
    # semantic duplicates ("BCC" ≡ "Базальноклеточный рак",
    # "Иванов И.И." ≡ "Иван Иванов", ...) and consolidates them into
    # one canonical entity.  See `src/graph/entity_resolution.py`.
    er_enabled: bool = True
    # Pairs per LLM-judge call when ER routes borderline candidates.
    er_judge_batch_size: int = Field(default=10, ge=1, le=50)
    # Persistent ER verdict cache: when on, borderline LLM-judge
    # verdicts are stored in Neo4j (`:ERVerdict`, order-insensitive
    # name/label key) so recurring pairs across re-ingests / hub-heavy
    # docs skip the LLM.  OPTIONAL + FAIL-SAFE: any Neo4j error or a
    # missing store falls back to pure LLM judging.
    er_verdict_cache_enabled: bool = True
    # Process-wide concurrency cap for LLM calls (search-side).
    # Applied via BoundedLLM wrapper in DI — all callers (ReAct, Self-RAG,
    # graph_search's LLMSynonymRetriever, judge) share this gate.
    # Bump up when LLM proxy / OpenAI quotas allow; default 8 leaves
    # headroom for ingest's serial llm_activity_concurrency.
    llm_max_concurrent: int = Field(default=8, ge=1, le=64)
    # Reflective synthesis (R8): how many draft → critique → retrieve
    # → redraft rounds the synthesizer attempts.
    max_refinements: int = Field(default=3, ge=0, le=10)
    # Observation distillation (R11): between tool_execution and the
    # next reasoning step, large tool observations are passed through a
    # one-shot LLM that extracts only query-relevant facts and grades
    # relevance.  Bounds reasoning-context growth on big corpora.  The
    # relevance verdict is advisory (history note + stats); full nodes
    # ALWAYS reach the synthesizer, so distillation never loses facts.
    distill_enabled: bool = True
    # Only distil observations larger than this (chars) — small ones
    # aren't worth the extra LLM call.
    distill_min_chars: int = Field(default=1500, ge=0)
    # Hard cap (chars) on any single observation written into the
    # reasoning history — backstop even when distillation is off or the
    # distilled text is still long.
    observation_max_chars: int = Field(default=6000, ge=500)
    # Pre-submit coverage check: when the agent picks submit_answer, an
    # LLM first judges whether the gathered evidence fully covers the
    # question; if not, the named gap is fed back and one more retrieval
    # round runs.  Adds the gap-detection plain `agent` mode lacks.
    coverage_check_enabled: bool = True
    # How many times the coverage check may bounce the agent back before
    # submit_answer is accepted unconditionally (caps extra LLM calls +
    # guarantees termination alongside max_iterations).
    max_coverage_checks: int = Field(default=1, ge=0, le=5)
    # Plan-execute flow (R4): after the orchestrator merges all
    # sub-question sources, it runs ONE coverage_check (reusing
    # ``coverage_check_enabled`` above).  On a named gap it issues the
    # gap as ONE extra SubQueryRetrievalWorkflow, re-merges, then
    # synthesizes.  Bounds the number of such extra rounds (and the
    # extra LLM + retrieval cost) — distinct from the ReAct loop's
    # ``max_coverage_checks`` so the two paths tune independently.
    max_coverage_rounds: int = Field(default=1, ge=0, le=3)
    # GraphRAG global search (R7a, decision C): the routing modes
    # local|global|drift map-reduce over the community summaries built in
    # R6.  ``global_max_communities`` caps how many summaries enter the
    # (parallel) MAP step so a huge corpus doesn't fan out unbounded;
    # ``global_map_parallelism`` bounds the per-community MAP concurrency
    # inside GlobalSearchWorkflow so a single query doesn't flood the
    # small-tier LLM proxy.
    global_max_communities: int = Field(default=20, ge=1, le=200)
    global_map_parallelism: int = Field(default=4, ge=1, le=32)
    # Multi-hop graph_walk seeding (Search R3b): in the deterministic
    # SubQuery retrieve path, after ``graph_search`` returns entities the
    # retrieve activity auto-seeds the bounded ``graph_walk`` tool from
    # the TOP graph_search entity (no LLM tool-pick needed).  Fail-open:
    # any walk error is swallowed and the vector+graph_search results are
    # returned unchanged.  ``graph_walk_hops`` is the requested hop count
    # (the tool clamps it to GRAPH_WALK_MAX_HOPS).
    graph_walk_enabled: bool = True
    graph_walk_hops: int = Field(default=2, ge=1, le=3)
    # Canonical entity linking (Task 6): when enabled, ingest resolves
    # each mention to an existing Wikibase QID via exact-alias →
    # embedding-kNN → optional LLM verify before deciding to mint a new
    # item (see `src/graph/canonical_linker.py`).  Default OFF — the
    # linker + alias storage ship as building blocks and are NOT yet
    # wired into the ingest activity.
    canonical_linker_enabled: bool = False


class MetricsSettings(BaseSettings):
    """Worker-side Prometheus exporter (Stage 2 of analytics plan).

    When ``enabled=True``, the worker installs a process-wide
    ``temporalio.runtime.Runtime`` with a ``PrometheusConfig`` listener
    on ``bind_address``.  Prometheus (running in docker compose) scrapes
    this endpoint from ``host.docker.internal:<port>``.
    """

    model_config = SettingsConfigDict(
        env_prefix="METRICS_",
        env_file=".env",
        extra="ignore",
    )

    enabled: bool = True
    bind_address: str = "0.0.0.0:9090"


class AnalyticsSettings(BaseSettings):
    """Version-tag plumbing for the ingest-metrics layer.

    Submit-time tag (``X-Version-Tag`` header on /ingest) falls back to
    ``default_version_tag``; ``env_name`` labels the deployment for
    Temporal search attributes and the Postgres ``ingest_metrics`` rows.
    """

    model_config = SettingsConfigDict(
        env_prefix="ANALYTICS_",
        env_file=".env",
        extra="ignore",
    )

    default_version_tag: str = "unspecified"
    env_name: str = "dev-local"


# ── composed top-level settings ──────────────────────────────────────


class Settings(BaseSettings):
    """Single import surface for the rest of the codebase.

    All sub-settings are constructed lazily as cached_property so that
    importing this module doesn't trigger env parsing for every
    subsystem when only one is needed (e.g. unit tests).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @cached_property
    def api(self) -> ApiSettings:
        return ApiSettings()

    @cached_property
    def milvus(self) -> MilvusSettings:
        return MilvusSettings()

    @cached_property
    def neo4j(self) -> Neo4jSettings:
        return Neo4jSettings()

    @cached_property
    def postgres(self) -> PostgresSettings:
        return PostgresSettings()

    @cached_property
    def litellm(self) -> LiteLLMSettings:
        return LiteLLMSettings()

    @cached_property
    def temporal(self) -> TemporalSettings:
        return TemporalSettings()

    @cached_property
    def minio(self) -> MinioSettings:
        return MinioSettings()

    @cached_property
    def ingestion(self) -> IngestionSettings:
        return IngestionSettings()

    @cached_property
    def agent(self) -> AgentSettings:
        return AgentSettings()

    @cached_property
    def wikibase(self) -> WikibaseSettings:
        return WikibaseSettings()

    @cached_property
    def hf(self) -> HFSettings:
        return HFSettings()

    @cached_property
    def metrics(self) -> MetricsSettings:
        return MetricsSettings()

    @cached_property
    def analytics(self) -> AnalyticsSettings:
        return AnalyticsSettings()


settings = Settings()


__all__ = [
    "AgentSettings",
    "AnalyticsSettings",
    "ApiSettings",
    "HFSettings",
    "IngestionSettings",
    "LiteLLMSettings",
    "MetricsSettings",
    "MilvusSettings",
    "MinioSettings",
    "Neo4jSettings",
    "PostgresSettings",
    "Settings",
    "TemporalSettings",
    "WikibaseSettings",
    "settings",
]

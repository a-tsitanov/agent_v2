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

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


LLMRole = Literal["extraction", "judge", "search"]


# ── per-subsystem settings ───────────────────────────────────────────


class ApiSettings(BaseSettings):
    """FastAPI surface — host, port, auth keys, CORS, log level."""

    model_config = SettingsConfigDict(
        env_prefix="API_", env_file=".env", extra="ignore"
    )

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
    model_config = SettingsConfigDict(
        env_prefix="MILVUS_", env_file=".env", extra="ignore"
    )

    host: str = "localhost"
    port: int = 19530
    collection: str = "kb_llamaindex"
    timeout_s: float = 10.0
    dim: int = 768

    @cached_property
    def uri(self) -> str:
        return f"http://{self.host}:{self.port}"


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEO4J_", env_file=".env", extra="ignore"
    )

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: SecretStr = SecretStr("changeme")
    database: str = "neo4j"
    timeout_s: float = 30.0


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_", env_file=".env", extra="ignore"
    )

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

    model_config = SettingsConfigDict(
        env_prefix="LITELLM_", env_file=".env", extra="ignore"
    )

    base_url: str = "http://localhost:4000"
    api_key: SecretStr = SecretStr("sk-litellm-stub")
    # Default model: qwen3:8b.  Has reliable tool calling (Hermes-
    # style) and structured output — required by R7/R8 (ReAct agent
    # and reflective synthesis).  Smaller models (qwen2.5:3b,
    # llama3.1:8b) work for plain retrieve+synthesize but fail
    # function-calling reliability tests; see docs/MODELS.md for
    # escalation path.
    llm_model: str = "qwen3:8b"
    # Per-role overrides — empty ("") means "use ``llm_model``".  Keeps
    # single-model deployments simple; cap into a per-role model when
    # the operator wants the cheap/fast model for high-volume judge
    # calls (cross-chunk merge + ER pair-wise yes/no) while keeping a
    # stronger model for extraction or the user-facing answer agent.
    # See ``model_for`` for resolution semantics.
    extraction_model: str = ""
    judge_model: str = ""
    search_model: str = ""
    embedding_model: str = "nomic-embed-text"
    embedding_dim: int = 768
    timeout_s: float = 900.0
    max_retries: int = 2

    def model_for(self, role: LLMRole) -> str:
        """Return the configured model name for ``role`` with fallback
        to ``llm_model`` when the role-specific field is empty."""
        override = {
            "extraction": self.extraction_model,
            "judge":      self.judge_model,
            "search":     self.search_model,
        }[role]
        return override or self.llm_model


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

    # Search-side activities (agent_reasoning_step, tool_execution,
    # synthesize_answer) live on their own task queue so operators can
    # control GPU split between ingest and search independently.  Cap
    # ≥ 1; raise it when LLM proxy / OpenAI quotas allow several
    # parallel search sessions.
    search_task_queue: str = "kb-search-llm"
    search_activity_concurrency: int = 4

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

    model_config = SettingsConfigDict(
        env_prefix="MINIO_", env_file=".env", extra="ignore"
    )

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
    model_config = SettingsConfigDict(
        env_prefix="INGESTION_", env_file=".env", extra="ignore"
    )

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
        env_prefix="WIKIBASE_", env_file=".env", extra="ignore",
    )

    enabled: bool = False
    base_url: str = "http://localhost:8181"
    bot_user: str = "KbBot"
    bot_password: SecretStr = SecretStr("botpass")
    language: str = "ru"
    timeout_s: float = 30.0


class AgentSettings(BaseSettings):
    """Knobs for the agentic search endpoints (`/agent`, `/selfrag`)."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_", env_file=".env", extra="ignore"
    )

    # Legacy judge-based loop (kept for R9 baseline eval).
    max_rounds: int = Field(default=3, ge=1, le=10)
    top_k: int = 10
    # ReAct loop (R7): how many tool-call iterations before forcing
    # `submit_answer`.
    max_iterations: int = Field(default=8, ge=1, le=20)
    # Entity Resolution (cross-language / multi-form dedup).  When
    # enabled, the worker runs an extra step between
    # `merge_kg_extraction` and `PropertyGraphIndex` that finds
    # semantic duplicates ("BCC" ≡ "Базальноклеточный рак",
    # "Иванов И.И." ≡ "Иван Иванов", ...) and consolidates them into
    # one canonical entity.  See `src/graph/entity_resolution.py`.
    er_enabled: bool = True
    # Pairs per LLM-judge call when ER routes borderline candidates.
    er_judge_batch_size: int = Field(default=10, ge=1, le=50)
    # Process-wide concurrency cap for LLM calls (search-side).
    # Applied via BoundedLLM wrapper in DI — all callers (ReAct, Self-RAG,
    # graph_search's LLMSynonymRetriever, judge) share this gate.
    # Bump up when LLM proxy / OpenAI quotas allow; default 8 leaves
    # headroom for ingest's serial llm_activity_concurrency.
    llm_max_concurrent: int = Field(default=8, ge=1, le=64)
    # Reflective synthesis (R8): how many draft → critique → retrieve
    # → redraft rounds the synthesizer attempts.
    max_refinements: int = Field(default=3, ge=0, le=10)
    # R10: legacy judge-based agentic_search remains in the codebase
    # as a comparative baseline for R9 eval.  Routed under
    # `/api/v1/legacy/agent` only when this flag is true.  Default
    # off — production traffic should go through /agent or /selfrag.
    enable_legacy_agent: bool = False


class MetricsSettings(BaseSettings):
    """Worker-side Prometheus exporter (Stage 2 of analytics plan).

    When ``enabled=True``, the worker installs a process-wide
    ``temporalio.runtime.Runtime`` with a ``PrometheusConfig`` listener
    on ``bind_address``.  Prometheus (running in docker compose) scrapes
    this endpoint from ``host.docker.internal:<port>``.
    """

    model_config = SettingsConfigDict(
        env_prefix="METRICS_", env_file=".env", extra="ignore",
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
        env_prefix="ANALYTICS_", env_file=".env", extra="ignore",
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
    def metrics(self) -> MetricsSettings:
        return MetricsSettings()

    @cached_property
    def analytics(self) -> AnalyticsSettings:
        return AnalyticsSettings()


settings = Settings()


__all__ = [
    "AnalyticsSettings",
    "ApiSettings",
    "AgentSettings",
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

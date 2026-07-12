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
from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

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
    "synthesis": "large",
}


# ── per-subsystem settings ───────────────────────────────────────────


class ApiSettings(BaseSettings):
    """FastAPI surface — host, port, auth keys, CORS, log level."""

    model_config = SettingsConfigDict(env_prefix="API_", env_file=".env", extra="ignore")

    # NB: API host/port are set on the uvicorn command line, not here.
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
    dim: int = 1536

    # --- ANN index (scale) -------------------------------------------
    # The chunk collection's vector index.  llama-index's MilvusVectorStore
    # defaults to ``index_type="FLAT"`` (brute-force, exhaustive scan) —
    # fine up to a few hundred k vectors, a latency cliff beyond ~1M.  We
    # default to HNSW so production collections get approximate-NN search.
    # NOTE: ``index_type`` only takes effect when the collection is
    # (re)created (fresh deploy or ``overwrite=True`` re-ingest); an
    # existing FLAT collection keeps FLAT until rebuilt — so this is an
    # opt-in-by-rebuild swap, never a silent in-place mutation.  Set
    # ``MILVUS_INDEX_TYPE=FLAT`` to keep exact search.
    index_type: str = "HNSW"
    hnsw_m: int = 16
    """HNSW graph degree (M).  Higher → better recall, more memory."""
    hnsw_ef_construction: int = 200
    """HNSW build-time search width.  Higher → better recall, slower build."""
    hnsw_ef_search: int = 64
    """HNSW query-time search width (ef).  Higher → better recall, slower
    query.  Must be ≥ the search top_k."""

    @cached_property
    def uri(self) -> str:
        return f"http://{self.host}:{self.port}"


class Neo4jSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NEO4J_", env_file=".env", extra="ignore")

    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: SecretStr = SecretStr("changeme")
    database: str = "neo4j"

    # Bolt-driver connection pool (src/graph/store.py, Track A2).  The
    # store is now cached once per process, so this is the pool shared by
    # every activity in that process — size it to the worker's activity
    # concurrency, not 1-per-call.  acquisition timeout bounds the wait
    # for a free pooled connection; connection timeout bounds the TCP
    # connect.  Both seconds.
    max_connection_pool_size: int = Field(default=16, ge=1)
    connection_acquisition_timeout_s: float = 60.0
    connection_timeout_s: float = 30.0

    # Bounded retry for transient write errors (src/graph/write_retry.py,
    # Track A3): concurrent MERGE into shared hub nodes throws a retryable
    # Neo.TransientError (deadlock / lock-acquisition timeout); re-run the
    # write a few times with backoff instead of failing the document.
    write_retry_max_attempts: int = Field(default=5, ge=1)
    write_retry_base_delay_s: float = 0.05

    # Debug toggle: log EVERY Cypher this process sends to Neo4j — one INFO
    # line per query (collapsed/truncated Cypher, param KEYS only, row
    # count, elapsed ms).  Off by default; flip on (NEO4J_QUERY_LOG=true) to
    # confirm/inspect the graph queries the search path issues without
    # touching Neo4j's own query.log.  Wrapper in src/graph/store.py.
    query_log: bool = False


class GraphSettings(BaseSettings):
    """Which graph backend the store factory builds.

    Strangler seam for the Neo4j -> NebulaGraph migration: every graph
    caller goes through ``src.graph.store.build_graph_store()`` which
    dispatches on this.  Default stays "neo4j" until per-workload parity
    benchmarks pass (project policy: benchmark before adopting — mirrors
    ``community_backend``)."""

    model_config = SettingsConfigDict(env_prefix="GRAPH_", env_file=".env", extra="ignore")

    backend: Literal["neo4j", "nebula"] = "neo4j"


class NebulaSettings(BaseSettings):
    """Connection to the NebulaGraph cluster (Phase 1+ write path).

    Mirrors ``Neo4jSettings``'s shape — same fields, NebulaGraph's own
    defaults (graphd port 9669, root/nebula, single space "kb")."""

    model_config = SettingsConfigDict(env_prefix="NEBULA_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 9669
    user: str = "root"
    password: SecretStr = SecretStr("nebula")
    space: str = "kb"
    # Rows per INSERT VERTEX/EDGE statement (multi-VALUES batching). Higher
    # amortises round-trips at ingest scale; see docs/superpowers/specs/
    # 2026-07-11-nebula-ingest-batch-design.md.
    write_batch_size: int = Field(default=256, ge=1)


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    db: str = "kb_llamaindex"
    user: str = "postgres"
    password: SecretStr = SecretStr("postgres")
    connect_timeout_s: int = 10

    # Per-process connection pool (src/storage/pg_pool.py).  Sized
    # CONSERVATIVELY: the deployment runs ~10 processes (8 worker pools
    # + API) against a single shared Postgres, so total app demand is
    # pool_max_size * n_processes and must leave headroom under
    # ``max_connections`` for Temporal (512 shards) on the same DB.
    # min_size=0 → no idle connections reserved per process.
    pool_min_size: int = 0
    pool_max_size: int = 4
    pool_timeout_s: float = 30.0

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
    embedding_model: str = "text-embedding-3-small"
    timeout_s: float = 900.0
    max_retries: int = 2
    # ── extra request-body params ────────────────────────────────────
    # Backend-specific fields injected verbatim into every chat request
    # body (via OpenAILike ``additional_kwargs={"extra_body": ...}``).
    # The OpenAI SDK rejects unknown top-level kwargs, so these MUST ride
    # in ``extra_body`` — that's where the SDK forwards arbitrary JSON.
    # Example: disable Qwen3 chain-of-thought for every call with
    #   LITELLM_EXTRA_BODY='{"think": false}'
    # ``extra_body_roles`` shallow-merges per-role overrides on top, e.g.
    #   LITELLM_EXTRA_BODY_ROLES='{"synthesis": {"think": true}}'
    # to keep thinking on for the final-answer role only.  Resolve the
    # effective dict for a role via ``extra_body_for(role)``.
    # ``NoDecode`` hands the raw env string to ``_parse_extra_body`` below
    # instead of pydantic-settings JSON-decoding it in the source — that
    # lets an empty ``LITELLM_EXTRA_BODY=`` mean "no params" instead of a
    # JSON parse error.
    extra_body: Annotated[dict[str, Any], NoDecode] = Field(default_factory=dict)
    extra_body_roles: Annotated[dict[str, dict[str, Any]], NoDecode] = Field(default_factory=dict)

    @field_validator("extra_body", "extra_body_roles", mode="before")
    @classmethod
    def _parse_extra_body(cls, v: object) -> object:
        """Accept a JSON string (pydantic-settings env), a dict, or an
        empty/None value (⇒ no params).  Mirrors ``role_tiers`` so an
        empty ``LITELLM_EXTRA_BODY=`` doesn't blow up JSON parsing."""
        if v is None:
            return {}
        if isinstance(v, str):
            import json

            return json.loads(v) if v.strip() else {}
        return v

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

    def extra_body_for(self, role: LLMRole | None) -> dict[str, Any]:
        """Effective extra request-body params for ``role``: the global
        ``extra_body`` default, shallow-merged with any per-role override
        from ``extra_body_roles`` (override keys win).  ``role=None``
        (legacy no-role path) gets the global default only.  Returns a
        fresh dict so callers can't mutate the stored settings."""
        merged = dict(self.extra_body)
        if role is not None:
            merged.update(self.extra_body_roles.get(role, {}))
        return merged


class TemporalSettings(BaseSettings):
    """Temporal worker / client connection settings."""

    model_config = SettingsConfigDict(env_prefix="TEMPORAL_", extra="ignore")

    host: str = "localhost"
    port: int = 7233
    namespace: str = "default"
    task_queue: str = "kb-ingest"
    activity_concurrency: int = 4
    staging_bucket: str = "kb-staging"

    # Closed-workflow history retention.  Without an explicit value the
    # namespace keeps the stock default, letting per-doc DocumentIngest /
    # GraphBuild histories accumulate in the shared Postgres.  setup_db
    # enforces this on init (idempotent).  0 → leave the namespace as-is.
    namespace_retention_days: int = 3

    # The always-on IngestSchedulerWorkflow singleton runs on its OWN queue
    # so its churn (a submit signal per doc + frequent continue_as_new at
    # high K) can't contend with DocumentIngestWorkflow task processing on
    # the `main` pool. The scheduler still starts DocumentIngestWorkflow
    # CHILDREN on `task_queue` (main) — the scheduler pool doesn't register
    # that workflow. Run a small pool here (1-2 replicas — it's a singleton).
    scheduler_task_queue: str = "kb-ingest-scheduler"

    # LLM-bound extract_kg activities run on a separate task queue.
    # LLMPool owns LLM concurrency via a single global semaphore (LLM_POOL_N).
    # This Temporal cap must be >= LLM_POOL_N so the in-process pool (not
    # Temporal) is the binding LLM throttle.
    # Lower only if Temporal slot overhead is a concern on a constrained host.
    llm_task_queue: str = "kb-ingest-llm"
    llm_activity_concurrency: int = 18

    # Merge stage (GraphBuildWorkflow → merge_and_resolve +
    # build_property_graph) runs on its OWN queue so it interleaves with
    # extract_kg instead of queueing behind a burst of extracts on
    # kb-ingest-llm (head-of-line blocking starved merge under load).
    # LLMPool owns LLM concurrency via a single global semaphore (LLM_POOL_N).
    # This Temporal cap must be >= LLM_POOL_N so the in-process pool (not
    # Temporal) is the binding LLM throttle.
    merge_task_queue: str = "kb-ingest-merge"
    merge_activity_concurrency: int = 14

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
    # Offline analytics materialisation (Wave 1): centrality + link-prediction
    # runs on the same kb-graph-build queue so it shares its low concurrency cap
    # and never touches the query hot path.  Raise to process more nodes in
    # parallel; keep modest so a rebuild doesn't starve Neo4j / LLM proxy.
    analytics_materialize_concurrency: int = Field(
        default=2,
        ge=1,
        description="GDS-воркеры для офлайн-материализации аналитики (centrality/link-prediction)",
    )
    # Bounded parallelism for the per-community summarize fan-out inside
    # CommunityBuildWorkflow (independent of the worker-side activity cap).
    community_summary_parallelism: int = 4
    # Communities smaller than this are ignored (too small to summarise
    # meaningfully — likely noise / disconnected pairs).
    community_min_size: int = 3
    # GDS Leiden tuning knobs (detection only — never the query path).
    # ``gamma`` = resolution: >1 yields MORE, smaller communities; <1
    # yields fewer, larger ones.  ``concurrency`` = GDS worker threads for
    # the Leiden run; keep modest so a rebuild doesn't starve Neo4j.
    community_leiden_gamma: float = Field(default=1.0, gt=0)
    community_leiden_concurrency: int = Field(default=4, ge=1)

    # Community-detection backend.  "gds" = in-Neo4j GDS Leiden (legacy);
    # "leidenalg" = in-worker leidenalg/igraph (memory off Neo4j); "graphscope"
    # = distributed single-level Leiden off GraphScope (scales past a single
    # worker).  Default stays "gds" until the strict-parity benchmark passes
    # (project policy: benchmark before adopting).
    community_backend: Literal["gds", "leidenalg", "graphscope"] = "gds"

    # Centrality COMPUTE backend. "gds" = in-Neo4j GDS pageRank/betweenness/
    # eigenvector (legacy). "igraph" = in-worker igraph over the edge-export seam
    # (memory off Neo4j; the ONLY option under nebula, which has no GDS — nebula
    # forces igraph regardless of this flag; see analytics/materialize.py).
    centrality_backend: Literal["gds", "igraph"] = "gds"

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
    # MUST be >= 8 chars: scripts/setup_wikibase.py refuses to provision the
    # bot (createAndPromote) below MediaWiki's minimum, so a too-short default
    # silently breaks the whole Wikibase push path.  Override in prod.
    bot_password: SecretStr = SecretStr("changemebot")
    language: str = "ru"
    timeout_s: float = 30.0


class WikiSettings(BaseSettings):
    """Continuous wiki-article editor (Project A). Generates per-entity
    MediaWiki pages from the Neo4j graph. Opt-in via WIKI_ENABLED."""

    model_config = SettingsConfigDict(
        env_prefix="WIKI_",
        env_file=".env",
        extra="ignore",
    )

    enabled: bool = False
    task_queue: str = "kb-wiki"
    activity_concurrency: int = Field(default=4, ge=1)
    sweep_batch: int = Field(default=50, ge=1)
    sweep_interval_minutes: int = Field(default=15, ge=1)
    citations_top_k: int = Field(default=8, ge=1)
    # Cap on 1-hop relations fed to the article prompt (ranked by neighbour
    # mention_count desc). Bounds prompt size for hub entities.
    max_relations: int = Field(default=30, ge=1)
    # Base URL for source-document download links in the "Источники" section.
    # Points at the documents API (GET {docs_base_url}/documents/{doc_id}).
    docs_base_url: str = "http://localhost:8000/api/v1"
    # MediaWiki Action API URL. Empty -> derived from wikibase.base_url
    # + "/w/api.php" by mediawiki_api_url() below.
    mediawiki_api_url: str = ""
    # MediaWiki site global id for sitelinks (wgLocalDatabases / site id).
    # Must match the wiki's actual site id; default fits the dev compose.
    site_global_id: str = "kbwiki"


class AgentSettings(BaseSettings):
    """Knobs for the search endpoints (`/api/v1/search/*`)."""

    model_config = SettingsConfigDict(env_prefix="AGENT_", env_file=".env", extra="ignore")

    # Conversation history (client-managed multi-turn): when enabled, prior
    # turns supplied on the request are used to contextualise the query into
    # a standalone form before retrieval.  Empty history = single-shot.
    conversation_history_enabled: bool = True
    history_max_turns: int = Field(default=6, ge=0, le=40)
    history_max_chars: int = Field(default=4000, ge=0)
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
    # Native Neo4j vector-index kNN for ER instead of the bounded
    # 5000-entity window.  Removes the window ceiling (at 200k canonicals
    # the window reaches only ~2% of true nearest matches; native kNN
    # ~96%) — essential under high-volume ingest where the graph outgrows
    # the window fast and the windowed path silently stops finding
    # cross-doc duplicates.  ON by default.  FAIL-SAFE: the native loader
    # idempotently builds `er_embedding_vec` and returns [] (→ within-batch
    # ER only) when `er_vec` isn't populated yet, so this can't crash a
    # fresh deploy.  For EXISTING graphs run `scripts/backfill_er_vector.py`
    # to populate `er_vec` on prior entities; without it, pre-backfill
    # entities won't be found until re-ingested.  Set false to force the
    # legacy window.
    er_use_native_vector_knn: bool = True
    # Neighbours fetched per new entity from the ER vector index when
    # native kNN is on.
    er_vector_knn_k: int = Field(default=20, ge=1, le=100)
    # Where the ER candidate-kNN vectors live. "native" = Neo4j in-graph
    # vector index (db.index.vector, unchanged prod path). "milvus" =
    # entity_er_vec Milvus collection (opt-in on neo4j for the parity
    # benchmark; FORCED under GRAPH_BACKEND=nebula, which has no in-graph
    # index). Dispatched in src/graph/entity_vector_store.py.
    er_vector_backend: Literal["native", "milvus"] = "native"
    # Where community-report vectors live for semantic community-select.
    # "native" = Neo4j in-graph community_report_vec index (prod path,
    # unchanged); "milvus" = community_report_vec Milvus collection (opt-in;
    # FORCED under GRAPH_BACKEND=nebula). Dispatched in
    # src/graph/community_vector_store.py.
    community_vector_backend: Literal["native", "milvus"] = "native"
    # Pre-submit coverage check: when the agent picks submit_answer, an
    # LLM first judges whether the gathered evidence fully covers the
    # question; if not, the named gap is fed back and one more retrieval
    # round runs.  Adds the gap-detection plain `agent` mode lacks.
    coverage_check_enabled: bool = True
    # Plan-execute flow (R4): after the orchestrator merges all
    # sub-question sources, it runs ONE coverage_check (reusing
    # ``coverage_check_enabled`` above).  On a named gap it issues the
    # gap as ONE extra SubQueryRetrievalWorkflow, re-merges, then
    # synthesizes.  Bounds the number of such extra rounds (and the
    # extra LLM + retrieval cost).
    max_coverage_rounds: int = Field(default=1, ge=0, le=3)
    # GraphRAG global search (R7a, decision C): the routing modes
    # local|global|drift map-reduce over the community summaries built in
    # R6.  ``global_max_communities`` caps how many summaries enter the
    # (parallel) MAP step so a huge corpus doesn't fan out unbounded;
    # LLM_POOL_N is the single throttle for MAP concurrency.
    global_max_communities: int = Field(default=20, ge=1, le=200)
    # Multi-hop graph_walk seeding (Search R3b): in the deterministic
    # SubQuery retrieve path, after ``graph_search`` returns entities the
    # retrieve activity auto-seeds the bounded ``graph_walk`` tool from
    # the TOP graph_search entity (no LLM tool-pick needed).  Fail-open:
    # any walk error is swallowed and the vector+graph_search results are
    # returned unchanged.  ``graph_walk_hops`` is the requested hop count
    # (the tool clamps it to GRAPH_WALK_MAX_HOPS).
    graph_walk_enabled: bool = True
    graph_walk_hops: int = Field(default=2, ge=1, le=3)
    # When on, graph_walk is seeded from BOTH the top graph_search entity
    # AND the top find_entity_by_name (fulltext) entity when they differ —
    # so a fulltext-matched entity (partial name / typo) still contributes
    # its neighbourhood even if graph_search already returned something.
    graph_walk_dual_seed: bool = True
    # Relation polarity + temporal-validity filtering at RETRIEVAL (#8):
    # when on (default), the bounded ``graph_walk`` drops relationships the
    # source text NEGATES (``polarity == 'negated'``) and edges whose
    # ``valid_to`` is strictly in the past (expired).  NULL/missing polarity
    # reads as affirmed; NULL ``valid_to`` reads as never-expiring; legacy
    # edges without these props are unaffected.  Opt-out (set False) if it
    # ever misbehaves — then negated/expired edges surface as before.
    graph_walk_filter_polarity_temporal: bool = True
    # ``path_depth`` for the similarity graph_search inside the local
    # pipeline: how many triplet-hops of neighbours aretrieve pulls around
    # each matched entity. Default 1 (current behaviour); raise (≤3) to
    # widen graph context. Tunable via TEMPORAL/AGENT env without code.
    graph_search_path_depth: int = Field(default=1, ge=1, le=3)
    # Graph retriever candidate count (VectorContextRetriever top_k). Raised
    # from the LlamaIndex default so a named entity isn't ranked out of the
    # result set on a large graph.
    graph_similarity_top_k: int = Field(default=20, ge=1, le=100)
    # Community build: how many Leiden dendrogram levels to materialise.
    # 1 = single-level (today's cost/behaviour); raise to build the
    # hierarchy (offline, additive). Safety-capped.
    community_max_levels: int = Field(default=1, ge=1, le=10)
    # Global/drift community selection strategy. "lexical" = today's
    # word-overlap; "semantic" = kNN over report_vec; "descent" = GraphRAG
    # hierarchy descent. Default lexical (query behaviour unchanged) — flip
    # only after a hierarchy build has populated reports + report_vec.
    community_dynamic_selection: Literal["lexical", "semantic", "descent"] = "lexical"


class LLMPoolSettings(BaseSettings):
    """Per-process LLM concurrency pool (K+N model).

    ONE semaphore of size ``n`` (``LLM_POOL_N``) gates EVERY LLM call across
    all roles.  Paired with admission K (``INGEST_ADMISSION_MAX_INFLIGHT``)
    this is the entire concurrency contract: "at most K documents in flight,
    at most N concurrent LLM calls".

    Role still selects the physical *model* (``build_llm(role)`` ->
    ``LiteLLMSettings.model_for``); it no longer affects gating.

    Per-process, NOT distributed - the true cross-process GPU ceiling belongs
    at the LiteLLM proxy (out of scope).
    """

    model_config = SettingsConfigDict(
        env_prefix="LLM_POOL_",
        env_file=".env",
        extra="ignore",
    )

    n: int = Field(default=8, ge=1)


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
    # --- analytical-query layer (Wave 0 v1a) ---
    default_top_n: int = Field(
        default=20,
        description="Максимальное число строк, возвращаемых аналитическим запросом по умолчанию (top-N).",
    )
    max_steps: int = Field(
        default=3, description="Максимальное число примитивных вызовов в одном аналитическом плане."
    )
    cypher_fallback_enabled: bool = Field(
        default=False,
        description="Разрешить фолбэк на text-to-Cypher при отсутствии подходящего примитива (v1c; по умолчанию выключено).",
    )


class EventsSettings(BaseSettings):
    """first_seen / event-detection config (Wave 0 E1)."""

    model_config = SettingsConfigDict(
        env_prefix="EVENTS_",
        env_file=".env",
        extra="ignore",
    )
    first_seen_enabled: bool = Field(
        default=False,
        description="Включить простановку метки first_seen при создании узла (переключать ТОЛЬКО после бэкфила).",
    )
    new_window_days: int = Field(
        default=14, description="Окно в днях для выборки новых событий (new_events) по умолчанию."
    )
    backfill_sentinel: int = Field(
        default=0,
        description="Метка эпохи-дня для узлов, созданных до включения first_seen (маркер бэкфила).",
    )
    extraction_enabled: bool = Field(
        default=True,
        description="Извлечение структурных LLM-событий в extract_kg (E2; по умолчанию вкл — удлиняет промпт/вывод на каждый чанк, выключать при нехватке LLM-бюджета)",
    )
    taxonomy: list[str] = Field(
        default_factory=lambda: [
            "deal",
            "appointment",
            "lawsuit",
            "incident",
            "payment",
            "meeting",
            "sanction",
        ],
        description="Закрытый список типов событий (event_type), с открытым fallback для длинного хвоста",
    )


class SignalsSettings(BaseSettings):
    """Knowledge-quality / actionable-signal config (Wave 0 P1)."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALS_",
        env_file=".env",
        extra="ignore",
    )
    orphan_min_degree: int = Field(
        default=1,
        description="Минимальная степень узла графа, ниже которой он считается изолированным (орфаном).",
    )
    # per-type expected identifier attributes for completeness scoring
    expected_attrs: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "Organization": ["INN", "OGRN", "PostalAddress", "PhoneNumber"],
            "Person": ["PhoneNumber", "Email"],
        },
        description="Ожидаемые идентификаторы для оценки полноты данных по типу сущности (используется в completeness-сигнале).",
    )
    # Composite risk score weights (Wave 1).  Five components; must sum to 1.0.
    # Operators can rebalance via SIGNALS_RISK_WEIGHTS='{"affiliation":0.4,...}'.
    risk_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "affiliation": 0.30,
            "brokerage": 0.20,
            "controversy": 0.20,
            "volatility": 0.15,
            "opacity": 0.15,
        },
        description="Веса компонентов composite risk_score (нормализованы к сумме 1.0)",
    )
    # Risk-band thresholds: score >= high → "high"; >= medium → "medium"; else "low".
    risk_bands: dict[str, float] = Field(
        default_factory=lambda: {"high": 0.66, "medium": 0.33},
        description="Пороги полос risk_score: >=high → high, >=medium → medium, иначе low",
    )
    # GDS node-similarity (link prediction) knobs.
    link_prediction_top_k: int = Field(
        default=10,
        ge=1,
        description="top-K соседей на узел для GDS node-similarity (link prediction)",
    )
    link_prediction_min_score: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Минимальный similarity для записи ребра :LIKELY_LINK",
    )


class MonitorSettings(BaseSettings):
    """Arc 2 continuous monitoring + alerts (scheduled sweep)."""

    model_config = SettingsConfigDict(env_prefix="MONITOR_", env_file=".env", extra="ignore")

    enabled: bool = Field(
        default=False, description="Включить непрерывный мониторинг/алерты (Arc 2)"
    )
    task_queue: str = Field(default="kb-monitor", description="Очередь воркера монитор-свипа")
    activity_concurrency: int = Field(
        default=2, ge=1, description="Параллелизм активностей монитора"
    )
    sweep_interval_minutes: int = Field(
        default=30, ge=1, description="Период Temporal-Schedule монитор-свипа, мин"
    )
    new_window_days: int = Field(
        default=7, ge=1, description="Окно (дни) для детекта новых first_seen-связей"
    )
    risk_rise_delta: float = Field(
        default=0.1, gt=0.0, le=1.0, description="Порог роста risk_score для алерта"
    )
    burst_enabled: bool = Field(
        default=False, description="Включить burst-детектор событий в монитор-свипе (E3)"
    )
    burst_window_days: int = Field(
        default=7, ge=1, description="Окно (дни) для подсчёта недавних событий в burst-детекторе"
    )
    burst_baseline_windows: int = Field(
        default=4, ge=1, description="Сколько предыдущих окон усреднять как базовую ставку burst"
    )
    burst_min_count: int = Field(
        default=2,
        ge=1,
        description="Мин. число недавних событий, чтобы пара (сущность,тип) считалась всплеском",
    )
    burst_ratio: float = Field(
        default=3.0, gt=1.0, description="Порог burst_score (recent/base) для алерта о всплеске"
    )
    webhook_url: str = Field(
        default="",
        description="URL генеричного webhook для доставки алертов (пусто — доставка выключена)",
    )
    webhook_timeout_s: float = Field(
        default=5.0, gt=0.0, description="Таймаут POST на webhook доставки алертов, сек"
    )
    deliver_batch: int = Field(
        default=100, ge=1, description="Сколько непушенных алертов доставлять за один свип"
    )


class ClassifierSettings(BaseSettings):
    """Input document classifier — skips junk before it enters the
    pipeline.  Opt-in (``enabled=False`` by default).

    Two layers: cheap deterministic rules (extension / size) then an
    optional LLM gate over a bounded preview.  A ``force=true`` flag on
    /ingest bypasses the deterministic rules so an operator can push a
    document the rules would skip.  Fail-soft everywhere: any classifier
    error defaults to INGEST (false-skip is the costly error)."""

    model_config = SettingsConfigDict(
        env_prefix="CLASSIFIER_",
        env_file=".env",
        extra="ignore",
    )

    enabled: bool = False
    max_size_mb: float = 25.0
    min_size_bytes: int = 1
    skip_extensions: list[str] = Field(
        default_factory=lambda: [
            "exe",
            "dll",
            "bin",
            "zip",
            "tar",
            "gz",
            "7z",
            "png",
            "jpg",
            "jpeg",
            "gif",
            "bmp",
            "svg",
            "mp3",
            "mp4",
            "mov",
            "avi",
            "wav",
        ]
    )
    preview_chars: int = 4000
    llm_enabled: bool = True


class IngestAdmissionSettings(BaseSettings):
    """Document-level admission control (always on).  /ingest hands every
    document to a singleton ``IngestSchedulerWorkflow`` that runs at most
    ``max_inflight`` (K) documents at once, each to completion, FIFO - so a
    document's tail (merge) isn't starved behind newer documents' extract
    bursts."""

    model_config = SettingsConfigDict(
        env_prefix="INGEST_ADMISSION_",
        env_file=".env",
        extra="ignore",
    )

    max_inflight: int = Field(default=1, ge=1)

    # Where the ingest BACKLOG lives (Track B).  ``temporal`` = today's
    # singleton IngestSchedulerWorkflow (backlog in workflow state, every
    # /ingest a signal — chokes on bulk inserts as history balloons).
    # ``rabbitmq`` = the backlog moves to a durable RabbitMQ queue and a
    # consumer admits at most ``max_inflight`` (prefetch=K) at a time.
    # Default ``temporal`` → no behaviour change until explicitly flipped.
    # Env var is the bare ``INGEST_QUEUE_BACKEND`` (no admission prefix).
    backend: Literal["temporal", "rabbitmq"] = Field(
        default="temporal",
        validation_alias="INGEST_QUEUE_BACKEND",
    )


class RabbitMQSettings(BaseSettings):
    """RabbitMQ ingest-queue connection (Track B).

    Only consumed when ``INGEST_QUEUE_BACKEND=rabbitmq``.  The producer
    (/ingest) publishes ``IngestParams`` as a persistent message to
    ``queue``; the consumer pulls with ``prefetch`` =
    ``IngestAdmissionSettings.max_inflight`` (admission K) and starts a
    ``DocumentIngestWorkflow`` per message, ack on success /
    dead-letter (``dlx`` → ``dlq``) on failure."""

    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_",
        env_file=".env",
        extra="ignore",
    )

    url: str = "amqp://guest:guest@localhost:5672/"
    # Configured ingest queues. /ingest picks one by explicit name
    # (validated against this list); the consumer declares + consumes ALL
    # of them on one channel under a GLOBAL prefetch=K (so total in-flight
    # ≤ K across every queue). Env: comma-separated, e.g.
    # RABBITMQ_QUEUES=ingest.pending,ingest.bulk. First entry is the default.
    queues: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["ingest.pending"],
    )
    # Dead-letter exchange + queue for messages that fail processing.
    dlx: str = "ingest.dlx"
    dlq: str = "ingest.dlq"
    # Requeue a nacked message instead of dead-lettering (debug only).
    requeue_on_failure: bool = False
    # Per-queue consumer ack timeout (ms), set as the ``x-consumer-timeout``
    # arg on ``queue``.  The consumer awaits the WHOLE DocumentIngestWorkflow
    # before acking, so a delivery can stay unacked for the full per-document
    # wall-clock (slow Neo4j writes; activity schedule_to_close up to ~12h).
    # RabbitMQ's default (30 min) would force-close the channel mid-document
    # and requeue every in-flight message → a storm of duplicate workflow
    # starts.  Set WELL above the longest document run (default 24h).
    consumer_timeout_ms: int = Field(default=86_400_000, ge=60_000)

    @field_validator("queues", mode="before")
    @classmethod
    def _split_queues(cls, v):
        """Allow a comma-separated env string (RABBITMQ_QUEUES=a,b) as well
        as a JSON/python list. Empty entries dropped; never empty."""
        if isinstance(v, str):
            v = [q.strip() for q in v.split(",") if q.strip()]
        if not v:
            return ["ingest.pending"]
        return v

    @property
    def default_queue(self) -> str:
        """Queue used when /ingest doesn't name one (the first configured)."""
        return self.queues[0]


# ── composed top-level settings ──────────────────────────────────────

# Known placeholder / default secrets that must NOT appear in production.
_PREFLIGHT_PLACEHOLDER_SECRETS: frozenset[str] = frozenset(
    {
        "dev-local-key",
        "changeme",
        "change-me",
        "postgres",
        "minioadmin",
        "changemebot",
        "botpass",
        "sk-litellm-stub",
    }
)


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
    def graph(self) -> GraphSettings:
        return GraphSettings()

    @cached_property
    def nebula(self) -> NebulaSettings:
        return NebulaSettings()

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
    def classifier(self) -> ClassifierSettings:
        return ClassifierSettings()

    @cached_property
    def ingest_admission(self) -> IngestAdmissionSettings:
        return IngestAdmissionSettings()

    @cached_property
    def rabbitmq(self) -> RabbitMQSettings:
        return RabbitMQSettings()

    @cached_property
    def agent(self) -> AgentSettings:
        return AgentSettings()

    @cached_property
    def llm_pool(self) -> LLMPoolSettings:
        return LLMPoolSettings()

    @cached_property
    def wikibase(self) -> WikibaseSettings:
        return WikibaseSettings()

    @cached_property
    def wiki(self) -> WikiSettings:
        return WikiSettings()

    @cached_property
    def hf(self) -> HFSettings:
        return HFSettings()

    @cached_property
    def metrics(self) -> MetricsSettings:
        return MetricsSettings()

    @cached_property
    def analytics(self) -> AnalyticsSettings:
        return AnalyticsSettings()

    @cached_property
    def events(self) -> EventsSettings:
        return EventsSettings()

    @cached_property
    def signals(self) -> SignalsSettings:
        return SignalsSettings()

    @cached_property
    def monitor(self) -> MonitorSettings:
        return MonitorSettings()

    @staticmethod
    def preflight(s: Settings) -> list[str]:
        """Return a list of actionable config problems (empty == OK).

        Hard problems matter in production (``API_ENV=production``); in dev
        they're advisory.  Callers decide whether to exit.
        """
        problems: list[str] = []
        prod = s.api.env == "production"

        if prod:
            api_keys = [k.strip() for k in s.api.keys.split(",") if k.strip()]
            if any(k in _PREFLIGHT_PLACEHOLDER_SECRETS for k in api_keys) or not api_keys:
                problems.append(
                    "API_KEYS contains a placeholder/default key; set real key(s) in production."
                )
            checks = {
                "NEO4J_PASSWORD": s.neo4j.password.get_secret_value(),
                "POSTGRES_PASSWORD": s.postgres.password.get_secret_value(),
                "MINIO_ACCESS_KEY": s.minio.access_key.get_secret_value(),
                "MINIO_SECRET_KEY": s.minio.secret_key.get_secret_value(),
            }
            for name, val in checks.items():
                if val in _PREFLIGHT_PLACEHOLDER_SECRETS:
                    problems.append(
                        f"{name} is a placeholder default ({val!r}); set a real "
                        f"secret in production."
                    )

        n = s.llm_pool.n
        if s.temporal.llm_activity_concurrency < n:
            problems.append(
                f"TEMPORAL_LLM_ACTIVITY_CONCURRENCY "
                f"({s.temporal.llm_activity_concurrency}) < LLM_POOL_N ({n}); "
                f"the Temporal cap must be >= N so the pool is the throttle."
            )
        if s.temporal.merge_activity_concurrency < n:
            problems.append(
                f"TEMPORAL_MERGE_ACTIVITY_CONCURRENCY "
                f"({s.temporal.merge_activity_concurrency}) < LLM_POOL_N ({n})."
            )

        if s.wiki.enabled or s.wikibase.enabled:
            bot_pw = s.wikibase.bot_password.get_secret_value()
            if len(bot_pw) < 8:
                problems.append(
                    "WIKIBASE_BOT_PASSWORD must be >= 8 chars when wiki/wikibase "
                    "is enabled (setup_wikibase refuses to provision the bot)."
                )
        return problems


settings = Settings()


__all__ = [
    "AgentSettings",
    "AnalyticsSettings",
    "ApiSettings",
    "EventsSettings",
    "HFSettings",
    "IngestionSettings",
    "LLMPoolSettings",
    "LiteLLMSettings",
    "MetricsSettings",
    "MilvusSettings",
    "MinioSettings",
    "MonitorSettings",
    "Neo4jSettings",
    "PostgresSettings",
    "Settings",
    "SignalsSettings",
    "TemporalSettings",
    "WikiSettings",
    "WikibaseSettings",
    "settings",
]

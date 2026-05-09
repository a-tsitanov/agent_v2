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

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    llm_model: str = "qwen2.5:3b"
    embedding_model: str = "nomic-embed-text"
    embedding_dim: int = 768
    timeout_s: float = 600.0
    max_retries: int = 2


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_", env_file=".env", extra="ignore"
    )

    url: str = "amqp://guest:guest@localhost:5672/"
    timeout_s: float = 10.0


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="INGESTION_", env_file=".env", extra="ignore"
    )

    chunk_size: int = 512
    chunk_overlap: int = 50
    breakpoint_percentile: int = 95
    batch_size: int = 10
    cache_dir: str = "/app/data/ingestion_cache"


class AgentSettings(BaseSettings):
    """Agentic-search loop knobs (Stage 4)."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_", env_file=".env", extra="ignore"
    )

    max_rounds: int = Field(default=3, ge=1, le=10)
    top_k: int = 10


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
    def rabbitmq(self) -> RabbitMQSettings:
        return RabbitMQSettings()

    @cached_property
    def ingestion(self) -> IngestionSettings:
        return IngestionSettings()

    @cached_property
    def agent(self) -> AgentSettings:
        return AgentSettings()


settings = Settings()


__all__ = [
    "ApiSettings",
    "AgentSettings",
    "IngestionSettings",
    "LiteLLMSettings",
    "MilvusSettings",
    "Neo4jSettings",
    "PostgresSettings",
    "RabbitMQSettings",
    "Settings",
    "settings",
]

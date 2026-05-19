"""Stage-0 smoke tests.

Goals:
  * importing the package doesn't raise on an empty / dev environment.
  * every nested ``Settings`` class instantiates with its declared
    defaults — proves that the env-prefix mapping isn't broken
    (typo'd field names would surface as ValidationError).
"""

from __future__ import annotations

import importlib


def test_top_level_package_imports() -> None:
    importlib.import_module("src")
    importlib.import_module("src.config")
    importlib.import_module("src.utils.logging")


def test_settings_defaults_load() -> None:
    from src.config import settings

    assert settings.api.port == 8000
    assert settings.milvus.collection == "kb_llamaindex"
    assert settings.neo4j.uri.startswith("bolt://")
    assert settings.postgres.db == "kb_llamaindex"
    assert settings.litellm.embedding_dim > 0
    assert settings.temporal.target.endswith(":7233")
    assert settings.ingestion.chunk_size > 0
    assert settings.agent.max_rounds >= 1


def test_api_keys_parses_csv() -> None:
    from src.config import ApiSettings

    s = ApiSettings(keys="key-a, key-b , key-c")
    assert s.keys_list == ["key-a", "key-b", "key-c"]


def test_postgres_dsn_assembled() -> None:
    from src.config import PostgresSettings

    s = PostgresSettings()
    assert s.dsn.startswith("postgresql://")
    assert "@" in s.dsn
    assert s.dsn.endswith(f"/{s.db}")


def test_logging_configures_without_error() -> None:
    from src.utils.logging import configure_logging

    configure_logging(level="debug", json_output=False)
    configure_logging(level="info", json_output=True)


def test_wikibase_settings_defaults():
    from src.config import settings
    w = settings.wikibase
    assert w.enabled is False           # opt-in
    assert w.base_url == "http://localhost:8181"
    assert w.bot_user == "KbBot"
    assert w.language == "ru"
    assert w.timeout_s == 30.0


def test_wikibase_enabled_via_env(monkeypatch):
    """Build a fresh `WikibaseSettings` under a monkey-patched env.
    Avoids `reload(cfg)` which would leave the module-level
    `settings` singleton in a permanently-broken state for other
    tests in this file (cf. the same pattern in
    test_llm_cache_disabled_via_env)."""
    from src.config import WikibaseSettings
    monkeypatch.setenv("WIKIBASE_ENABLED", "true")
    monkeypatch.setenv("WIKIBASE_BASE_URL", "http://wb.internal:8181")
    fresh = WikibaseSettings()
    assert fresh.enabled is True
    assert fresh.base_url == "http://wb.internal:8181"


def test_per_role_model_falls_back_to_llm_model(monkeypatch):
    """When the role-specific env var is empty, model_for() returns
    LITELLM_LLM_MODEL so existing single-model deployments don't break."""
    from src.config import LiteLLMSettings

    monkeypatch.setenv("LITELLM_LLM_MODEL", "fallback-model")
    monkeypatch.delenv("LITELLM_EXTRACTION_MODEL", raising=False)
    monkeypatch.delenv("LITELLM_JUDGE_MODEL", raising=False)
    monkeypatch.delenv("LITELLM_SEARCH_MODEL", raising=False)

    fresh = LiteLLMSettings()
    assert fresh.model_for("extraction") == "fallback-model"
    assert fresh.model_for("judge") == "fallback-model"
    assert fresh.model_for("search") == "fallback-model"


def test_per_role_model_explicit_override(monkeypatch):
    """Each role-specific env-var wins over LITELLM_LLM_MODEL."""
    from src.config import LiteLLMSettings

    monkeypatch.setenv("LITELLM_LLM_MODEL", "default-model")
    monkeypatch.setenv("LITELLM_EXTRACTION_MODEL", "ext-14b")
    monkeypatch.setenv("LITELLM_JUDGE_MODEL", "judge-3b")
    monkeypatch.setenv("LITELLM_SEARCH_MODEL", "search-7b")

    fresh = LiteLLMSettings()
    assert fresh.model_for("extraction") == "ext-14b"
    assert fresh.model_for("judge") == "judge-3b"
    assert fresh.model_for("search") == "search-7b"
    # Legacy llm_model still readable for callers that don't use role.
    assert fresh.llm_model == "default-model"

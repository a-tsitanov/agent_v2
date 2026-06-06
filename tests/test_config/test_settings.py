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
    assert settings.agent.max_subqueries >= 1


def test_merge_queue_defaults() -> None:
    """Dedicated merge queue (decouples GraphBuildWorkflow's merge stage
    from a burst of extract_kg on kb-ingest-llm)."""
    from src.config import TemporalSettings

    s = TemporalSettings()
    assert s.merge_task_queue == "kb-ingest-merge"
    assert s.merge_activity_concurrency == 14


def test_merge_queue_env_override(monkeypatch) -> None:
    from src.config import TemporalSettings

    monkeypatch.setenv("TEMPORAL_MERGE_TASK_QUEUE", "custom-merge")
    monkeypatch.setenv("TEMPORAL_MERGE_ACTIVITY_CONCURRENCY", "3")
    fresh = TemporalSettings()
    assert fresh.merge_task_queue == "custom-merge"
    assert fresh.merge_activity_concurrency == 3


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


def test_roles_resolve_to_small_tier_by_default(monkeypatch):
    """Every role except synthesis runs on the small (local) model;
    synthesis runs on the large model.  Two physical model names."""
    from src.config import LiteLLMSettings

    monkeypatch.setenv("LITELLM_MODEL_SMALL", "small-model")
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "large-model")
    monkeypatch.delenv("LITELLM_ROLE_TIERS", raising=False)

    fresh = LiteLLMSettings()
    for role in ("extraction", "judge", "search", "plan", "coverage"):
        assert fresh.model_for(role) == "small-model"
    assert fresh.model_for("synthesis") == "large-model"


def test_role_tier_override_via_env(monkeypatch):
    """A partial LITELLM_ROLE_TIERS escalates one role to large while
    every other role keeps its default tier."""
    from src.config import LiteLLMSettings

    monkeypatch.setenv("LITELLM_MODEL_SMALL", "small-model")
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "large-model")
    monkeypatch.setenv("LITELLM_ROLE_TIERS", '{"plan": "large"}')

    fresh = LiteLLMSettings()
    assert fresh.model_for("plan") == "large-model"
    # synthesis default-large preserved through the merge.
    assert fresh.model_for("synthesis") == "large-model"
    # untouched roles stay small.
    assert fresh.model_for("judge") == "small-model"


def test_effective_base_prefers_deprecated_alias(monkeypatch):
    """Legacy no-role path: explicit llm_model wins, else small tier."""
    from src.config import LiteLLMSettings

    monkeypatch.setenv("LITELLM_MODEL_SMALL", "small-model")
    # Force-empty (not delenv): the tracked .env file may still carry a
    # deprecated LITELLM_LLM_MODEL value that env_file would otherwise load.
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    assert LiteLLMSettings().effective_base == "small-model"

    monkeypatch.setenv("LITELLM_LLM_MODEL", "legacy-model")
    assert LiteLLMSettings().effective_base == "legacy-model"


def test_agent_graph_similarity_top_k_default():
    from src.config import settings
    assert settings.agent.graph_similarity_top_k == 20


def test_llm_pool_settings_defaults():
    from src.config import LLMPoolSettings
    s = LLMPoolSettings()
    assert s.tier_small_total == 25
    assert s.tier_large_total == 8
    # f49a83c anti-regression sizing rule must hold by default:
    assert s.lane_caps["extraction"] <= s.tier_small_total - s.judge_floor
    assert s.lane_caps["judge"] >= 1

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
    assert settings.rabbitmq.url.startswith("amqp://")
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

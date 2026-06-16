"""Boot-time preflight: actionable problems instead of mid-request stack traces."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.config import Settings


def _settings(**over):
    s = MagicMock(spec=Settings)
    s.api = MagicMock(env=over.get("env", "production"), keys=over.get("keys", "real-key-123"))
    s.neo4j = MagicMock(password=MagicMock(get_secret_value=lambda: over.get("neo4j", "realpass")))
    s.postgres = MagicMock(password=MagicMock(get_secret_value=lambda: over.get("pg", "realpass")))
    s.minio = MagicMock(
        access_key=MagicMock(get_secret_value=lambda: over.get("minio", "realkey")),
        secret_key=MagicMock(get_secret_value=lambda: over.get("minio_secret", "realsecret")),
    )
    s.llm_pool = MagicMock(n=over.get("n", 8))
    s.temporal = MagicMock(
        llm_activity_concurrency=over.get("llm_cap", 18),
        merge_activity_concurrency=over.get("merge_cap", 14),
    )
    s.wikibase = MagicMock(
        enabled=over.get("wb", False),
        bot_password=MagicMock(get_secret_value=lambda: over.get("bot_pw", "longenoughpw")),
    )
    s.wiki = MagicMock(enabled=over.get("wiki", False))
    return s


def test_preflight_clean_prod_has_no_problems():
    assert Settings.preflight(_settings()) == []


def test_preflight_flags_placeholder_secret_in_prod():
    problems = Settings.preflight(_settings(keys="dev-local-key"))
    assert any("API_KEYS" in p for p in problems)


def test_preflight_flags_temporal_cap_below_pool_n():
    problems = Settings.preflight(_settings(n=20, llm_cap=18))
    assert any("LLM_POOL_N" in p for p in problems)


def test_preflight_dev_allows_placeholders():
    assert Settings.preflight(_settings(env="development", keys="dev-local-key")) == []


def test_preflight_flags_placeholder_minio_secret_in_prod():
    problems = Settings.preflight(_settings(minio_secret="minioadmin"))
    assert any("MINIO_SECRET_KEY" in p for p in problems)


def test_preflight_flags_placeholder_key_in_multikey_api_keys():
    problems = Settings.preflight(_settings(keys="real-key,dev-local-key"))
    assert any("API_KEYS" in p for p in problems)


def test_preflight_flags_short_wiki_bot_password():
    problems = Settings.preflight(_settings(wiki=True, bot_pw="short"))
    assert any("WIKIBASE_BOT_PASSWORD" in p for p in problems)

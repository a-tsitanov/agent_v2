from src.config import WikiSettings


def test_wiki_settings_new_fields_defaults():
    s = WikiSettings()
    assert s.max_relations == 30
    assert s.docs_base_url == "http://localhost:8000/api/v1"
    assert s.citations_top_k == 8  # bumped from 5


def test_wiki_settings_env_override(monkeypatch):
    monkeypatch.setenv("WIKI_MAX_RELATIONS", "10")
    monkeypatch.setenv("WIKI_DOCS_BASE_URL", "https://kb.internal/api/v1")
    s = WikiSettings()
    assert s.max_relations == 10
    assert s.docs_base_url == "https://kb.internal/api/v1"

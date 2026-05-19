"""Role-keyed LLM factory tests.

Confirms each wrapper pulls its configured model name from
``LiteLLMSettings.model_for`` and that the legacy ``build_llm()``
(no role) keeps reading ``llm_model`` so unmigrated callers are
unaffected.

We patch ``OpenAILike`` so the tests don't try to open a network
connection to LiteLLM — only the wiring is exercised.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _capture(monkeypatch, *, role_kwarg, env_model_var, env_model_value):
    """Patch OpenAILike, call ``build_llm(...)`` once, return the model
    name that the factory passed in."""
    captured: dict = {}

    monkeypatch.setenv("LITELLM_LLM_MODEL", "default-model")
    if env_model_var:
        monkeypatch.setenv(env_model_var, env_model_value)

    def _spy(*_a, **kw):
        captured.update(kw)
        return MagicMock()

    # Import inside the function so monkey-patched env is seen by
    # `LiteLLMSettings()` constructed lazily inside build_llm.
    import src.retrieval.llm as llm_mod
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    reload(llm_mod)
    with patch("src.retrieval.llm.OpenAILike", side_effect=_spy):
        if role_kwarg is None:
            llm_mod.build_llm()
        else:
            getattr(llm_mod, f"build_{role_kwarg}_llm")()
    return captured.get("model")


def test_build_llm_no_role_uses_llm_model(monkeypatch):
    """Legacy path: no role kwarg ⇒ LITELLM_LLM_MODEL."""
    model = _capture(monkeypatch, role_kwarg=None,
                     env_model_var=None, env_model_value=None)
    assert model == "default-model"


def test_build_extraction_llm_uses_extraction_model(monkeypatch):
    model = _capture(monkeypatch, role_kwarg="extraction",
                     env_model_var="LITELLM_EXTRACTION_MODEL",
                     env_model_value="ext-14b")
    assert model == "ext-14b"


def test_build_judge_llm_uses_judge_model(monkeypatch):
    model = _capture(monkeypatch, role_kwarg="judge",
                     env_model_var="LITELLM_JUDGE_MODEL",
                     env_model_value="judge-3b")
    assert model == "judge-3b"


def test_build_search_llm_uses_search_model(monkeypatch):
    model = _capture(monkeypatch, role_kwarg="search",
                     env_model_var="LITELLM_SEARCH_MODEL",
                     env_model_value="search-7b")
    assert model == "search-7b"


def test_role_factory_falls_back_to_llm_model(monkeypatch):
    """Per-role wrapper falls back to LITELLM_LLM_MODEL when the
    role-specific env var is empty — keeps single-model deployments
    behaving identically after the refactor."""
    # No LITELLM_EXTRACTION_MODEL set ⇒ should hit fallback.
    model = _capture(monkeypatch, role_kwarg="extraction",
                     env_model_var=None, env_model_value=None)
    assert model == "default-model"

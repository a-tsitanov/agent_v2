"""Role-keyed LLM factory tests.

Confirms each wrapper resolves its model name through the two-tier
``LiteLLMSettings.model_for`` (role → tier → small/large physical
model) and that the legacy ``build_llm()`` (no role) reads the
``effective_base`` (small tier, or the deprecated ``llm_model`` alias
when explicitly set) so unmigrated callers are unaffected.

We patch ``OpenAILike`` so the tests don't try to open a network
connection to LiteLLM — only the wiring is exercised.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _capture(monkeypatch, *, role_kwarg, extra_env=None):
    """Patch OpenAILike, call ``build_llm(...)`` once, return the model
    name that the factory passed in."""
    captured: dict = {}

    monkeypatch.setenv("LITELLM_MODEL_SMALL", "small-model")
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "large-model")
    # Force-empty (not delenv): the tracked .env file may still carry a
    # deprecated LITELLM_LLM_MODEL value that env_file would otherwise load.
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    monkeypatch.delenv("LITELLM_ROLE_TIERS", raising=False)
    for k, v in (extra_env or {}).items():
        monkeypatch.setenv(k, v)

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


def test_build_llm_no_role_uses_small_tier(monkeypatch):
    """Legacy path: no role kwarg ⇒ small tier (effective_base)."""
    model = _capture(monkeypatch, role_kwarg=None)
    assert model == "small-model"


def test_build_llm_no_role_honours_deprecated_alias(monkeypatch):
    """Explicit LITELLM_LLM_MODEL still wins for the no-role path."""
    model = _capture(monkeypatch, role_kwarg=None,
                     extra_env={"LITELLM_LLM_MODEL": "legacy-model"})
    assert model == "legacy-model"


def test_build_extraction_llm_uses_small_tier(monkeypatch):
    assert _capture(monkeypatch, role_kwarg="extraction") == "small-model"


def test_build_judge_llm_uses_small_tier(monkeypatch):
    assert _capture(monkeypatch, role_kwarg="judge") == "small-model"


def test_build_search_llm_uses_small_tier(monkeypatch):
    assert _capture(monkeypatch, role_kwarg="search") == "small-model"


def test_build_synthesis_llm_uses_large_tier(monkeypatch):
    assert _capture(monkeypatch, role_kwarg="synthesis") == "large-model"


def _capture_kwargs(monkeypatch, *, role_kwarg, extra_env=None):
    """Like ``_capture`` but returns the full kwargs dict OpenAILike saw."""
    captured: dict = {}

    monkeypatch.setenv("LITELLM_MODEL_SMALL", "small-model")
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "large-model")
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    monkeypatch.delenv("LITELLM_ROLE_TIERS", raising=False)
    monkeypatch.delenv("LITELLM_EXTRA_BODY", raising=False)
    monkeypatch.delenv("LITELLM_EXTRA_BODY_ROLES", raising=False)
    for k, v in (extra_env or {}).items():
        monkeypatch.setenv(k, v)

    def _spy(*_a, **kw):
        captured.update(kw)
        return MagicMock()

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
    return captured


def test_no_extra_body_means_no_additional_kwargs(monkeypatch):
    """Default config ⇒ no extra_body wired into the request."""
    kw = _capture_kwargs(monkeypatch, role_kwarg="extraction")
    assert "extra_body" not in kw.get("additional_kwargs", {})


def test_global_extra_body_reaches_openai_like(monkeypatch):
    kw = _capture_kwargs(
        monkeypatch, role_kwarg="extraction",
        extra_env={"LITELLM_EXTRA_BODY": '{"think": false}'},
    )
    assert kw["additional_kwargs"] == {"extra_body": {"think": False}}


def test_per_role_override_reaches_openai_like(monkeypatch):
    extra_env = {
        "LITELLM_EXTRA_BODY": '{"think": false}',
        "LITELLM_EXTRA_BODY_ROLES": '{"synthesis": {"think": true}}',
    }
    extr = _capture_kwargs(monkeypatch, role_kwarg="extraction", extra_env=extra_env)
    synth = _capture_kwargs(monkeypatch, role_kwarg="synthesis", extra_env=extra_env)
    assert extr["additional_kwargs"] == {"extra_body": {"think": False}}
    assert synth["additional_kwargs"] == {"extra_body": {"think": True}}

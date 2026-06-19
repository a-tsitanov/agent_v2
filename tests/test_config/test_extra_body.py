"""Per-role extra request-body params on ``LiteLLMSettings``.

Operators inject backend-specific request fields (e.g. Qwen3's
``think=false`` to disable chain-of-thought) via ``LITELLM_EXTRA_BODY``
(global default) and ``LITELLM_EXTRA_BODY_ROLES`` (per-role overrides).
``extra_body_for(role)`` shallow-merges the role override onto the
global default — see ``src/retrieval/llm.py`` for how it reaches the
wire (OpenAILike ``additional_kwargs={"extra_body": ...}``).
"""

from __future__ import annotations

from src.config import LiteLLMSettings


def test_empty_by_default():
    cfg = LiteLLMSettings()
    assert cfg.extra_body == {}
    assert cfg.extra_body_for("extraction") == {}
    assert cfg.extra_body_for(None) == {}


def test_global_default_applies_to_every_role():
    cfg = LiteLLMSettings(extra_body={"think": False})
    assert cfg.extra_body_for("extraction") == {"think": False}
    assert cfg.extra_body_for("synthesis") == {"think": False}
    assert cfg.extra_body_for(None) == {"think": False}


def test_role_override_merges_onto_global():
    cfg = LiteLLMSettings(
        extra_body={"think": False},
        extra_body_roles={"synthesis": {"think": True}},
    )
    # role with no override → global default
    assert cfg.extra_body_for("extraction") == {"think": False}
    # overridden role → merged (override key wins)
    assert cfg.extra_body_for("synthesis") == {"think": True}


def test_role_override_adds_keys_keeps_global():
    cfg = LiteLLMSettings(
        extra_body={"think": False},
        extra_body_roles={"judge": {"temperature": 0.0}},
    )
    assert cfg.extra_body_for("judge") == {"think": False, "temperature": 0.0}


def test_returns_a_copy_not_the_stored_dict():
    """Callers must not mutate the settings through the returned dict."""
    cfg = LiteLLMSettings(extra_body={"think": False})
    out = cfg.extra_body_for("search")
    out["think"] = True
    assert cfg.extra_body == {"think": False}


def test_json_env_parsing(monkeypatch):
    monkeypatch.setenv("LITELLM_EXTRA_BODY", '{"think": false}')
    monkeypatch.setenv(
        "LITELLM_EXTRA_BODY_ROLES", '{"synthesis": {"think": true}}'
    )
    cfg = LiteLLMSettings()
    assert cfg.extra_body_for("extraction") == {"think": False}
    assert cfg.extra_body_for("synthesis") == {"think": True}


def test_empty_string_env_is_treated_as_no_params(monkeypatch):
    monkeypatch.setenv("LITELLM_EXTRA_BODY", "")
    monkeypatch.setenv("LITELLM_EXTRA_BODY_ROLES", "")
    cfg = LiteLLMSettings()
    assert cfg.extra_body == {}
    assert cfg.extra_body_roles == {}

"""Unit tests for the LiteLLM model-validator that runs at API +
worker startup.

The probe uses ``httpx.Client.get`` against
``{base_url}/v1/models``; we patch it so the tests don't need a
live proxy.  The validator is intentionally lenient (warns rather
than raises) — but the ``LITELLM_VALIDATE_MODELS_STRICT`` env flag
flips that, and we cover both branches.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.observability.litellm_models import validate_litellm_models


def _proxy_returns(models: list[str]):
    """Build a context manager that patches httpx.Client.get to return
    a fake LiteLLM /v1/models response listing the given model names."""
    fake = MagicMock()
    fake.raise_for_status = MagicMock()
    fake.json.return_value = {"data": [{"id": m} for m in models]}

    client_mock = MagicMock()
    client_mock.get.return_value = fake
    cm = MagicMock()
    cm.__enter__.return_value = client_mock
    cm.__exit__.return_value = False
    return patch("src.observability.litellm_models.httpx.Client",
                 return_value=cm)


def test_all_models_registered_logs_info(caplog, monkeypatch):
    # Two-tier model: validation cross-checks the two physical models.
    monkeypatch.setenv("LITELLM_MODEL_SMALL", "gpt-4o")
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "gpt-4o-mini")
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    monkeypatch.delenv("LITELLM_VALIDATE_MODELS_STRICT", raising=False)

    # Re-import settings under the new env.
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.observability.litellm_models as mod
    reload(mod)

    with _proxy_returns(["gpt-4o-mini", "gpt-4o"]):
        mod.validate_litellm_models(source="test-ok")
    # No raise; that's the assertion.


def test_missing_model_warns_in_non_strict_mode(monkeypatch):
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "gpt-4o-mini")
    monkeypatch.setenv("LITELLM_MODEL_SMALL", "qwen3:8b-not-real")
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    monkeypatch.delenv("LITELLM_VALIDATE_MODELS_STRICT", raising=False)

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.observability.litellm_models as mod
    reload(mod)

    # Should not raise — only log a warning.
    with _proxy_returns(["gpt-4o-mini", "gpt-4o"]):
        mod.validate_litellm_models(source="test-warn")


def test_missing_model_raises_in_strict_mode(monkeypatch):
    monkeypatch.setenv("LITELLM_MODEL_LARGE", "gpt-4o-mini")
    monkeypatch.setenv("LITELLM_MODEL_SMALL", "qwen3:8b-not-real")
    monkeypatch.setenv("LITELLM_LLM_MODEL", "")
    monkeypatch.setenv("LITELLM_VALIDATE_MODELS_STRICT", "true")

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.observability.litellm_models as mod
    reload(mod)

    with _proxy_returns(["gpt-4o-mini", "gpt-4o"]):
        with pytest.raises(RuntimeError, match="missing=small="):
            mod.validate_litellm_models(source="test-strict")


def test_proxy_unreachable_does_not_block_boot(monkeypatch):
    """Connectivity failure → empty available list → no validation;
    only an at-startup probe-failed warning.  The point is not to
    block worker boot on a momentarily-down proxy."""
    monkeypatch.setenv("LITELLM_LLM_MODEL", "gpt-4o-mini")
    monkeypatch.delenv("LITELLM_VALIDATE_MODELS_STRICT", raising=False)

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.observability.litellm_models as mod
    reload(mod)

    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__enter__.return_value.get.side_effect = ConnectionError("proxy down")
    cm.__exit__.return_value = False
    with patch("src.observability.litellm_models.httpx.Client", return_value=cm):
        # Should NOT raise (operator wants worker to come up while
        # the proxy is being booted).  Same in strict mode — strict
        # only matters when the proxy is reachable but lacks the model.
        mod.validate_litellm_models(source="test-unreachable")

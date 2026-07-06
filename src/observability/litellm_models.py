"""Startup validation: every model the operator put in
``LITELLM_*_MODEL`` env vars must be registered in the live LiteLLM
proxy.

We hit ``GET {base_url}/v1/models`` once at process boot and cross-
check the returned ``model_name`` list against ``settings.litellm.*``.
If something doesn't resolve, log a loud WARNING with the exact
fix-it-yourself recipe (works whether the proxy uses a YAML
``model_list`` or DB-stored models via ``store_model_in_db``).

Why a warning, not a hard raise:
  * Operators sometimes ingest with the legacy `build_llm()` path
    (no per-role override).  In that case only ``llm_model`` must
    exist; a stale ``LITELLM_JUDGE_MODEL`` env value should not
    block the worker.
  * Fail-fast on a missing model would prevent the worker from ever
    coming up while the operator is debugging an env mistake — too
    aggressive.

Operators who DO want hard-fail can set
``LITELLM_VALIDATE_MODELS_STRICT=true`` in env.

Symptoms this catches:
  * 500 from the proxy: ``TypeError: '<' not supported between
    instances of 'int' and 'NoneType'`` (the ``num_retry vs None``
    bug — see docker/litellm_config.yaml comment).
  * ``litellm.exceptions.BadRequestError: Model not found`` on the
    first activity that tries to chat the model.
"""

from __future__ import annotations

import os

import httpx
from loguru import logger

from src.config import settings


def _strict_mode() -> bool:
    return os.environ.get(
        "LITELLM_VALIDATE_MODELS_STRICT", "false",
    ).lower() in {"1", "true", "yes"}


def _list_available_models(base_url: str, api_key: str, timeout_s: float) -> list[str]:
    """Return the ``model_name`` list LiteLLM proxy currently
    serves.  Empty list on connectivity failure (we don't want
    network glitches to block worker boot)."""
    url = f"{base_url.rstrip('/')}/v1/models"
    try:
        with httpx.Client(timeout=timeout_s) as client:
            resp = client.get(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
            )
            resp.raise_for_status()
        data = resp.json().get("data", [])
        return [m.get("id") for m in data if m.get("id")]
    except Exception as exc:
        logger.warning(
            "LiteLLM /v1/models probe failed — skipping model validation "
            "(reason: {e}).  Misconfigured per-role models will still surface "
            "as 500s at ingest time.",
            e=exc,
        )
        return []


def _configured_models() -> dict[str, str]:
    """Return non-empty ``{label: model_name}`` requested by env.

    Two-tier model: every role resolves to one of two physical models
    (``model_small`` / ``model_large``), so validating those two covers
    all roles.  The deprecated ``llm_model`` alias is included only when
    an operator explicitly set it (legacy no-role ``build_llm()``)."""
    cfg = settings.litellm
    pairs = {
        "small":   cfg.model_small,
        "large":   cfg.model_large,
        "default": cfg.llm_model,
    }
    return {k: v for k, v in pairs.items() if v}


def validate_litellm_models(*, source: str) -> None:
    """Probe the LiteLLM proxy and warn (or raise) on missing models.

    ``source`` is a free-text tag like "api" / "worker" that lands in
    the log line so the operator can tell which process complained.
    """
    cfg = settings.litellm
    available = _list_available_models(
        base_url=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        timeout_s=min(cfg.timeout_s, 10.0),
    )
    if not available:
        return    # probe-failed branch already logged

    configured = _configured_models()
    missing: list[tuple[str, str]] = [
        (role, name) for role, name in configured.items()
        if name not in available
    ]

    if not missing:
        logger.info(
            "litellm model validation OK  source={s}  models={m}",
            s=source, m=sorted(set(configured.values())),
        )
        return

    summary = ", ".join(f"{role}={name!r}" for role, name in missing)
    advice = (
        "Fix-it: (a) edit docker/litellm_config.yaml `model_list` and "
        "`docker compose -p kb-llamaindex restart litellm`, or (b) if "
        "running with `store_model_in_db: true`, add the model via "
        "LiteLLM Admin UI / `POST /model/new`.  Available models right "
        f"now: {sorted(available)}"
    )

    if _strict_mode():
        raise RuntimeError(
            f"LiteLLM model validation FAILED (strict)  source={source}  "
            f"missing={summary}.  {advice}",
        )
    logger.warning(
        "LiteLLM model validation FAILED  source={s}  missing={m}.  {a}",
        s=source, m=summary, a=advice,
    )

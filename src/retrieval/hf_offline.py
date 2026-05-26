"""Offline HuggingFace cache wiring for air-gapped deploys.

``configure_hf()`` translates the project's ``settings.hf`` knobs into
the STANDARD HuggingFace env vars that every HF library honours
(``transformers``, ``huggingface_hub``, ``sentence-transformers``,
``gliner``).  It MUST be called BEFORE any of those libraries load a
model so the loaders read from a pre-populated local cache instead of
hitting the Hub.

The two project models that flow through HF (and therefore through this
helper) are the GLiNER span-NER model and the BGE cross-encoder
reranker — see ``scripts/download_models.py`` for the pre-download step.
Embeddings + LLMs go through the LiteLLM proxy and are NOT affected.

Idempotent + operator-friendly: an env var an operator has ALREADY set
(e.g. an explicit ``HF_HOME``) is left untouched, so manual overrides
always win over config-derived defaults.
"""

from __future__ import annotations

import os

from loguru import logger

from src.config import settings


def _set_if_absent(name: str, value: str, *, configured: list[str]) -> None:
    """Set ``os.environ[name]`` only when it is not already set, so an
    operator's explicit env value wins over the config-derived one."""
    if os.environ.get(name):
        return
    os.environ[name] = value
    configured.append(f"{name}={value}")


def configure_hf() -> list[str]:
    """Apply ``settings.hf`` to the HuggingFace env vars (idempotent).

    * ``cache_dir`` set  → point ``HF_HOME`` /
      ``SENTENCE_TRANSFORMERS_HOME`` / ``TRANSFORMERS_CACHE`` at it
      (only when unset — operator env wins).
    * ``offline`` true   → force ``HF_HUB_OFFLINE=1`` +
      ``TRANSFORMERS_OFFLINE=1`` so loaders never touch the network.

    Returns the list of ``NAME=value`` pairs it set (for logging /
    tests).  Safe to call repeatedly.
    """
    hf = settings.hf
    configured: list[str] = []

    if hf.cache_dir:
        for name in ("HF_HOME", "SENTENCE_TRANSFORMERS_HOME", "TRANSFORMERS_CACHE"):
            _set_if_absent(name, hf.cache_dir, configured=configured)

    if hf.offline:
        # Forced (not _set_if_absent): when the operator asks for offline
        # we guarantee it regardless of any stale env value.
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        configured.extend(["HF_HUB_OFFLINE=1", "TRANSFORMERS_OFFLINE=1"])

    if configured:
        logger.info("hf_offline: configured HuggingFace env: {c}", c=configured)
    return configured


__all__ = ["configure_hf"]

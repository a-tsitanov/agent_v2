"""LLM-client factory wrapping LiteLLM-proxied OpenAILike.

Per-role variants resolve their model through the two-tier
``LiteLLMSettings`` (role → tier → small/large physical model) — see
``docs/MODELS.md`` for the role→tier table and escalation path.
``build_llm()`` with no role kwarg stays as the legacy fallback (uses
``model_small``, or the deprecated ``llm_model`` if explicitly set) so
we don't break callers that haven't migrated yet.
"""

from __future__ import annotations

import os

from llama_index.core.llms import LLM
from llama_index.llms.openai_like import OpenAILike

from src.config import LLMRole, settings


def _fc_flag() -> bool:
    """Function-calling on by default; ``LITELLM_FUNCTION_CALLING=false``
    forces the prompt-based fallback for smaller models that don't do
    Hermes-style tool calls reliably (e.g. llama3.1:8b)."""
    return os.environ.get(
        "LITELLM_FUNCTION_CALLING", "true"
    ).lower() not in {"0", "false", "no"}


def _build(model: str) -> LLM:
    cfg = settings.litellm
    return OpenAILike(
        model=model,
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
        is_chat_model=True,
        is_function_calling_model=_fc_flag(),
    )


def build_llm(role: LLMRole | None = None) -> LLM:
    """Construct an LLM client.

    ``role=None`` (legacy) ⇒ uses ``settings.litellm.model_small``
    (or the deprecated ``llm_model`` if explicitly set).
    ``role=<name>`` ⇒ uses ``settings.litellm.model_for(role)`` which
    resolves role → tier → one of the two physical models.
    """
    cfg = settings.litellm
    model = cfg.model_for(role) if role else cfg.effective_base
    return _build(model)


def build_extraction_llm() -> LLM:
    """High-volume KG triple extraction + translation (small tier)."""
    return build_llm("extraction")


def build_judge_llm() -> LLM:
    """ER pair-wise yes/no judgements + cross-chunk merge summary
    (high call volume; small tier)."""
    return build_llm("judge")


def build_search_llm() -> LLM:
    """Latency-sensitive user-facing ReAct / Self-RAG / legacy agent
    reasoning (small tier)."""
    return build_llm("search")


def build_synthesis_llm() -> LLM:
    """Final user-facing answer synthesis (large tier)."""
    return build_llm("synthesis")

"""LLM-client factory wrapping LiteLLM-proxied OpenAILike."""

from __future__ import annotations

import os

from llama_index.core.llms import LLM
from llama_index.llms.openai_like import OpenAILike

from src.config import settings


def build_llm() -> LLM:
    """Construct the project LLM client from ``LiteLLMSettings``.

    Function calling is on by default (qwen3:8b supports
    Hermes-style tool calls reliably).  Set the env var
    ``LITELLM_FUNCTION_CALLING=false`` to force the prompt-based
    fallback — needed when running against a smaller model that
    skips tool calls (e.g. llama3.1:8b in baseline eval).
    """
    cfg = settings.litellm
    function_calling = os.environ.get(
        "LITELLM_FUNCTION_CALLING", "true"
    ).lower() not in {"0", "false", "no"}
    return OpenAILike(
        model=cfg.llm_model,
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
        is_chat_model=True,
        is_function_calling_model=function_calling,
    )

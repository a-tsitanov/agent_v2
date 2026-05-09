"""LLM-client factory wrapping LiteLLM-proxied OpenAILike."""

from __future__ import annotations

from llama_index.core.llms import LLM
from llama_index.llms.openai_like import OpenAILike

from src.config import settings


def build_llm() -> LLM:
    """Construct the project LLM client from ``LiteLLMSettings``."""
    cfg = settings.litellm
    return OpenAILike(
        model=cfg.llm_model,
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
        is_chat_model=True,
        is_function_calling_model=False,
    )

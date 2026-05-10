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
        # Default off — the project uses SimpleLLMPathExtractor for
        # KG extraction (regex-based parsing, no tool calls).  Flip
        # to True if you swap in SchemaLLMPathExtractor with a
        # function-calling capable model (GPT-4, Claude, large
        # Llama 3.1+ via Ollama).
        is_function_calling_model=False,
    )

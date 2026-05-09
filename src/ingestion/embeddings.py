"""Embedding-model factory.

Wraps LlamaIndex's ``OpenAILikeEmbedding`` to talk to the LiteLLM
proxy (or any OpenAI-compatible embeddings endpoint).  Same wire
protocol as enterprise-kb uses for the embedding side, just routed
through LlamaIndex abstractions.
"""

from __future__ import annotations

from llama_index.core.embeddings import BaseEmbedding
from llama_index.embeddings.openai_like import OpenAILikeEmbedding

from src.config import settings


def build_embedding_model() -> BaseEmbedding:
    """Construct the LiteLLM-proxied embedding model from settings.

    Caller decides whether to attach it to ``llama_index.core.Settings``
    globally or pass it explicitly into pipelines / indices (the
    latter is easier to swap in tests).
    """
    cfg = settings.litellm
    return OpenAILikeEmbedding(
        model_name=cfg.embedding_model,
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        embed_batch_size=10,
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
        # `api_key` env hint — LlamaIndex sometimes inspects this for
        # OpenAI-compatible auth even if api_key is explicit:
        is_local=False,
    )

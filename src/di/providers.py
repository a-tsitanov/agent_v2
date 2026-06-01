"""Dishka DI providers.

Two containers:
  * **API** — long-lived: PG client + shared LLM/embedding singletons.
    The only active consumer is the ingest route (injects ``AsyncPostgres``).
  * **Worker** — long-lived: PG client + shared LLM/embed singletons.
    Ingestion pipeline is built ad-hoc inside the worker task since
    its transformations vary per call.

Both share `CommonProvider` for cross-cutting LLM and embedding
singletons.

The R7b search cutover removed the legacy ReAct/Self-RAG routes and the
``ApiProvider`` retrieval/judge/synthesizer singletons they consumed —
the plan-execute search path runs in the Temporal worker and bootstraps
its own deps via ``src/workflow/_search_deps.py``, not this container.
"""

from __future__ import annotations

from dishka import Provider, Scope, make_async_container, provide
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.llms import LLM

from src.ingestion.embeddings import build_embedding_model
from src.retrieval.llm import build_search_llm
from src.retrieval.llm_semaphore import BoundedLLM
from src.storage.postgres import AsyncPostgres


class CommonProvider(Provider):
    """Shared singletons available to both API and worker."""

    scope = Scope.APP

    @provide
    def postgres(self) -> AsyncPostgres:
        return AsyncPostgres()

    @provide
    def llm(self) -> LLM:
        # Shared bounded LLM singleton.  Wrapped in BoundedLLM so every
        # async chat method passes through a process-wide semaphore,
        # protecting the GPU/proxy from unbounded concurrency.
        from src.config import settings
        return BoundedLLM(
            build_search_llm(),
            max_concurrent=settings.agent.llm_max_concurrent,
        )

    @provide
    def embed_model(self) -> BaseEmbedding:
        return build_embedding_model()


def build_api_container():
    return make_async_container(CommonProvider())


def build_worker_container():
    """Worker container — currently exposes only `CommonProvider`.

    The Temporal worker activities (`src.workflow.activities.*`)
    compose their pipeline and graph extractor inline since the
    set of transformations is activity-dependent.
    """
    return make_async_container(CommonProvider())

"""Dishka DI providers.

Two containers:
  * **API** — long-lived: PG client, retriever, judge, synthesizer,
    graph retriever (optional, falls back to None when Neo4j is
    unreachable).
  * **Worker** — long-lived: PG client + shared LLM/embed singletons.
    Ingestion pipeline is built ad-hoc inside the worker task since
    its transformations vary per call.

Both share `CommonProvider` for cross-cutting LLM and embedding
singletons.
"""

from __future__ import annotations

from dishka import AnyOf, Provider, Scope, make_async_container, provide
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.llms import LLM
from llama_index.core.response_synthesizers import (
    BaseSynthesizer,
    ResponseMode,
    get_response_synthesizer,
)
from loguru import logger

from src.ingestion.embeddings import build_embedding_model
from src.retrieval.agent import (
    GraphRetrieverProtocol,
    JudgeProtocol,
    RetrieverProtocol,
    SynthesizerProtocol,
)
from src.retrieval.judge import LLMJudge
from src.retrieval.llm import build_search_llm
from src.retrieval.llm_semaphore import BoundedLLM
from src.retrieval.vector_index import build_vector_index, build_vector_store
from src.storage.chunk_repository import ChunkRepository
from src.storage.postgres import AsyncPostgres


class CommonProvider(Provider):
    """Shared singletons available to both API and worker."""

    scope = Scope.APP

    @provide
    def postgres(self) -> AsyncPostgres:
        return AsyncPostgres()

    @provide
    def llm(self) -> LLM:
        # DI-injected LLM goes to /agent, /selfrag, /legacy/agent —
        # all user-facing answer paths.  Routes to the "search" role
        # (multimodel plan) AND wraps in BoundedLLM so every async
        # chat method passes through a process-wide semaphore.  This
        # protects the GPU/proxy from unbounded concurrency when
        # multiple ReAct sessions (or MCP-2 atomic tool calls hitting
        # LLMSynonymRetriever) fire at once.
        from src.config import settings
        return BoundedLLM(
            build_search_llm(),
            max_concurrent=settings.agent.llm_max_concurrent,
        )

    @provide
    def embed_model(self) -> BaseEmbedding:
        return build_embedding_model()


class ApiProvider(Provider):
    """Retrieval / synthesis singletons for the API process.

    The three search endpoints (`/search`, `/agent`, `/selfrag`) all
    consume from this provider; routing to plain vs agentic logic
    lives in the route handlers, not in the provider.
    """

    scope = Scope.APP

    @provide
    def retriever(self, embed_model: BaseEmbedding) -> RetrieverProtocol:
        """Dense-vector retriever over the project Milvus collection.

        Hybrid (BM25 + vector RRF) is implemented in
        `src.retrieval.hybrid.build_hybrid_retriever` but NOT wired
        here yet — production BM25 needs a separate docstore /
        sparse index decision (in-memory BM25 from Milvus walks
        doesn't scale).  Until that decision lands, dense-only is
        the API's retriever.
        """
        store = build_vector_store()
        index = build_vector_index(store, embed_model)
        return index.as_retriever(similarity_top_k=10)

    @provide
    def judge(self, llm: LLM) -> JudgeProtocol:
        return LLMJudge(llm)

    @provide
    def synthesizer(self, llm: LLM) -> AnyOf[BaseSynthesizer, SynthesizerProtocol]:
        return get_response_synthesizer(
            llm=llm, response_mode=ResponseMode.COMPACT,
        )

    @provide
    def chunk_repository(self, postgres: AsyncPostgres) -> ChunkRepository:
        """Doc-id keyed access to chunks (Milvus) + source files
        (Postgres → upload dir).  Used by the ReAct agent's
        `get_chunks_by_doc_id` and `read_full_document` tools so
        the agent can fetch full-document context that vector
        retrieval can't surface on its own."""
        return ChunkRepository(pg=postgres)

    @provide
    def graph_retriever(
        self, embed_model: BaseEmbedding, llm: LLM,
    ) -> GraphRetrieverProtocol | None:
        """Attach to the already-populated Neo4j graph store.

        Falls back to `None` when Neo4j is unreachable — every
        agentic path (legacy judge loop, ReAct R7, Self-RAG R8)
        handles `graph_retriever=None` natively.
        """
        try:
            from src.graph.index import (
                build_kg_extractor,
                build_property_graph_index,
            )
            from src.graph.retriever import GraphRetriever
            from src.graph.store import build_neo4j_graph_store

            graph_store = build_neo4j_graph_store()
            pg_index = build_property_graph_index(
                graph_store=graph_store,
                embed_model=embed_model,
                extractor=build_kg_extractor(llm),
                nodes=None,  # attach to existing populated store
            )
            return GraphRetriever(pg_index)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "graph_retriever disabled (Neo4j unreachable?): {err}",
                err=exc,
            )
            return None


def build_api_container():
    return make_async_container(CommonProvider(), ApiProvider())


def build_worker_container():
    """Worker container — currently exposes only `CommonProvider`.

    The Temporal worker activities (`src.workflow.activities.*`)
    compose their pipeline and graph extractor inline since the
    set of transformations is activity-dependent.
    """
    return make_async_container(CommonProvider())

"""dishka DI providers.

Two containers:
  * **API** — long-lived: PG client, retriever, judge, synthesizer,
    graph retriever (optional).
  * **Worker** — long-lived: PG client, ingestion pipeline, vector
    index, KG extractor + index.

Both share a ``CommonProvider`` for cross-cutting components (LLM,
embedding model).  Tests construct a custom container with stubs.
"""

from __future__ import annotations

from typing import AsyncIterator

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
from src.retrieval.llm import build_llm
from src.retrieval.vector_index import build_vector_index, build_vector_store
from src.storage.postgres import AsyncPostgres


class CommonProvider(Provider):
    """Shared singletons (LLM, embeddings, PG)."""

    scope = Scope.APP

    @provide
    def postgres(self) -> AsyncPostgres:
        return AsyncPostgres()

    @provide
    def llm(self) -> LLM:
        return build_llm()

    @provide
    def embed_model(self) -> BaseEmbedding:
        return build_embedding_model()


class ApiProvider(Provider):
    """Retrieval / synthesis singletons for the API process."""

    scope = Scope.APP

    @provide
    def retriever(self, embed_model: BaseEmbedding) -> RetrieverProtocol:
        # Vector retriever as the default "fast path".  Stage 5
        # hybrid retriever can be plugged here once the live BM25
        # docstore is sourced from Milvus.
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
    def graph_retriever(
        self, embed_model: BaseEmbedding, llm: LLM,
    ) -> GraphRetrieverProtocol | None:
        """Attach to the already-populated Neo4j graph store.

        Falls back to ``None`` when Neo4j is unreachable — search
        still works on vector chunks alone in that case.  The
        agent loop handles ``graph_retriever=None`` natively.
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
    """Worker DI is intentionally minimal at Stage 8 — the worker
    runs the ingestion pipeline directly via top-level functions.
    Reserved here so future stages can plug PropertyGraphIndex
    builders without changing the signature."""
    return make_async_container(CommonProvider())

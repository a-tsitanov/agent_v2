"""Stage-3 tests for vector index + basic query engine.

Avoids the live Milvus dependency by using LlamaIndex's built-in
``SimpleVectorStore`` (in-memory).  ``MockEmbedding`` deterministically
hashes text → vector, so retrieval ordering is reproducible.
``MockLLM`` echoes the prompt — the test doesn't assert response
quality, only that the engine wires together.
"""

from __future__ import annotations

from llama_index.core import Document, MockEmbedding
from llama_index.core.llms import MockLLM
from llama_index.core.vector_stores import SimpleVectorStore

from src.ingestion.pipeline import build_ingestion_pipeline
from src.retrieval.query_engine import build_basic_query_engine
from src.retrieval.vector_index import build_vector_index, index_nodes


def _three_docs() -> list[Document]:
    return [
        Document(
            text="Kubernetes is a container orchestration platform.",
            metadata={"file_path": "k8s.txt"},
        ),
        Document(
            text="Postgres is a relational database management system.",
            metadata={"file_path": "pg.txt"},
        ),
        Document(
            text="LlamaIndex provides RAG building blocks.",
            metadata={"file_path": "llama.txt"},
        ),
    ]


def test_index_nodes_returns_count_inserted() -> None:
    docs = _three_docs()
    pipeline = build_ingestion_pipeline()
    nodes = pipeline.run(documents=docs)

    embed_model = MockEmbedding(embed_dim=8)
    index = build_vector_index(SimpleVectorStore(), embed_model)
    inserted = index_nodes(index, nodes)

    assert inserted == len(nodes)
    assert inserted >= 3  # at least one node per doc


def test_index_nodes_handles_empty_input() -> None:
    embed_model = MockEmbedding(embed_dim=8)
    index = build_vector_index(SimpleVectorStore(), embed_model)
    assert index_nodes(index, []) == 0


def test_basic_query_engine_returns_source_nodes() -> None:
    docs = _three_docs()
    pipeline = build_ingestion_pipeline()
    nodes = pipeline.run(documents=docs)

    embed_model = MockEmbedding(embed_dim=8)
    index = build_vector_index(SimpleVectorStore(), embed_model)
    index_nodes(index, nodes)

    engine = build_basic_query_engine(
        index, llm=MockLLM(), similarity_top_k=2,
    )
    response = engine.query("what runs containers?")

    # Engine returned a Response object with source_nodes attached
    assert hasattr(response, "source_nodes")
    assert len(response.source_nodes) <= 2
    # Each source node carries metadata propagated from the original
    # document — Stage 7 will rely on this for source citation.
    for sn in response.source_nodes:
        assert sn.node.metadata.get("file_path", "").endswith(".txt")

"""Stage-3 tests for the vector index.

Avoids the live Milvus dependency by using LlamaIndex's built-in
``SimpleVectorStore`` (in-memory).  ``MockEmbedding`` deterministically
hashes text → vector, so retrieval ordering is reproducible.
"""

from __future__ import annotations

from llama_index.core import Document, MockEmbedding
from llama_index.core.vector_stores import SimpleVectorStore

from src.ingestion.pipeline import build_ingestion_pipeline
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
    pipeline = build_ingestion_pipeline(translate_to_russian=False)
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

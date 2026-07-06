"""Stage-3 tests for the vector index.

Avoids the live Milvus dependency by using LlamaIndex's built-in
``SimpleVectorStore`` (in-memory).  ``MockEmbedding`` deterministically
hashes text → vector, so retrieval ordering is reproducible.
"""

from __future__ import annotations

from unittest.mock import patch

from llama_index.core import Document, MockEmbedding
from llama_index.core.vector_stores import SimpleVectorStore

from src.config import MilvusSettings, settings
from src.ingestion.pipeline import build_ingestion_pipeline
from src.retrieval.vector_index import (
    _index_config,
    _search_config,
    build_vector_index,
    build_vector_store,
    index_nodes,
)


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


# --- ANN index config (scale) --------------------------------------------

def test_index_config_hnsw_carries_build_params() -> None:
    cfg = MilvusSettings(index_type="HNSW", hnsw_m=32, hnsw_ef_construction=128)
    assert _index_config(cfg) == {
        "index_type": "HNSW", "M": 32, "efConstruction": 128,
    }
    assert _search_config(cfg) == {"ef": cfg.hnsw_ef_search}


def test_index_config_flat_omits_hnsw_params() -> None:
    cfg = MilvusSettings(index_type="FLAT")
    # No M/efConstruction for FLAT — Milvus uses its own defaults.
    assert _index_config(cfg) == {"index_type": "FLAT"}
    assert _search_config(cfg) == {}


def test_build_vector_store_passes_ann_index_config() -> None:
    """build_vector_store must hand Milvus an explicit index_config so the
    collection is NOT created with the brute-force FLAT default."""
    with patch("src.retrieval.vector_index.MilvusVectorStore") as MVS:
        build_vector_store()
    _, kwargs = MVS.call_args
    assert kwargs["index_config"]["index_type"] == settings.milvus.index_type
    assert "search_config" in kwargs

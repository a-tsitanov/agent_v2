"""Vector store + index factories.

Two layers:
  * ``build_vector_store()`` constructs a Milvus-backed
    ``BasePydanticVectorStore`` from ``MilvusSettings`` — used in the
    real worker / API path.
  * ``build_vector_index()`` wraps any vector store into a
    ``VectorStoreIndex`` for retrieval.  Tests pass a
    ``SimpleVectorStore`` so they don't need a running Milvus.

Stage 7 (canonical identifier transform) does NOT touch this module —
identifiers go via the IngestionPipeline transform hook before nodes
arrive here.
"""

from __future__ import annotations

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.schema import BaseNode
from llama_index.core.vector_stores.types import BasePydanticVectorStore
from llama_index.vector_stores.milvus import MilvusVectorStore

from src.config import settings


def build_vector_store(
    *, overwrite: bool = False,
) -> BasePydanticVectorStore:
    """Open the Milvus collection configured in ``MilvusSettings``.

    ``overwrite=True`` drops and recreates — only for setup scripts /
    full re-ingest, never in the request path.
    """
    cfg = settings.milvus
    return MilvusVectorStore(
        uri=cfg.uri,
        collection_name=cfg.collection,
        dim=cfg.dim,
        overwrite=overwrite,
        # Milvus requires a non-empty similarity-metric — cosine is
        # the default LightRAG / enterprise-kb choice and matches what
        # most embedding models train against.
        similarity_metric="COSINE",
        # `upsert_mode=True` makes `add(nodes)` call `client.upsert`
        # instead of `client.insert`: re-inserting the same
        # `node_id` overwrites the row rather than creating a
        # duplicate.  Critical for workflow durability — Temporal
        # may retry the `index_vector` activity if the worker
        # crashes mid-batch.
        upsert_mode=True,
        # ANN index config — see MilvusSettings.  Without this the store
        # falls back to FLAT (exhaustive) search, which is a latency cliff
        # at production scale (≳1M chunk vectors).  Only applied when the
        # collection is (re)created.
        index_config=_index_config(cfg),
        search_config=_search_config(cfg),
    )


def _index_config(cfg) -> dict:
    """Build the Milvus dense-index config from settings.

    For non-HNSW index types the HNSW-specific build params are omitted
    so the server uses its own defaults.  ``metric_type`` is left out —
    MilvusVectorStore derives it from ``similarity_metric``.
    """
    conf: dict = {"index_type": cfg.index_type}
    if cfg.index_type.upper() == "HNSW":
        conf["M"] = cfg.hnsw_m
        conf["efConstruction"] = cfg.hnsw_ef_construction
    return conf


def _search_config(cfg) -> dict:
    """Build the Milvus query-time search params from settings."""
    if cfg.index_type.upper() == "HNSW":
        return {"ef": cfg.hnsw_ef_search}
    return {}


def build_vector_index(
    vector_store: BasePydanticVectorStore,
    embed_model: BaseEmbedding,
) -> VectorStoreIndex:
    """Wrap a vector store into a ``VectorStoreIndex`` ready for
    insertions and retrieval.  No documents are inserted here —
    callers stream nodes via ``index.insert_nodes(...)`` or via
    ``index_nodes()`` below.
    """
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    return VectorStoreIndex(
        nodes=[],
        storage_context=storage_context,
        embed_model=embed_model,
    )


def index_nodes(
    index: VectorStoreIndex,
    nodes: list[BaseNode],
) -> int:
    """Insert pre-chunked nodes (output of the ingestion pipeline).

    Returns the number of nodes inserted — handy for diagnostics in
    the worker.  Embedding is computed by ``index.insert_nodes`` if
    nodes don't carry pre-computed embeddings.
    """
    if not nodes:
        return 0
    index.insert_nodes(nodes)
    return len(nodes)

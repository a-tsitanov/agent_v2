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
    )


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

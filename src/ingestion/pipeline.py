"""LlamaIndex ``IngestionPipeline`` factory.

Stage 2 covers parsing + chunking + caching.  Embedding-attaching
transformation is added in Stage 3 once the vector store is wired —
keeping concerns split makes the pipeline easier to reason about
when iterating on either chunking quality or vector indexing.

The factory exposes two splitter modes:
  * ``SentenceSplitter`` (default) — fast, deterministic, no embed
    dependency.  Used by tests and for early bring-up.
  * ``SemanticSplitterNodeParser`` (opt-in via ``semantic=True``) —
    splits at embedding-similarity breakpoints.  Higher chunk
    quality but requires a working ``embed_model`` and adds an
    embedding round-trip per ingest.
"""

from __future__ import annotations

from pathlib import Path

from llama_index.core.embeddings import BaseEmbedding
from llama_index.core.ingestion import IngestionCache, IngestionPipeline
from llama_index.core.node_parser import (
    SemanticSplitterNodeParser,
    SentenceSplitter,
)
from llama_index.core.readers import SimpleDirectoryReader
from llama_index.core.schema import TransformComponent
from llama_index.core.storage.kvstore import SimpleKVStore

from src.config import settings


def _build_splitter(
    semantic: bool,
    embed_model: BaseEmbedding | None,
) -> TransformComponent:
    if semantic:
        if embed_model is None:
            raise ValueError(
                "semantic splitter requires an embed_model"
            )
        return SemanticSplitterNodeParser(
            buffer_size=1,
            breakpoint_percentile_threshold=settings.ingestion.breakpoint_percentile,
            embed_model=embed_model,
        )
    return SentenceSplitter(
        chunk_size=settings.ingestion.chunk_size,
        chunk_overlap=settings.ingestion.chunk_overlap,
    )


def _build_cache(cache_dir: str | Path | None) -> IngestionCache | None:
    """Persistent KV cache for transformation outputs.

    Pass ``None`` to disable (tests usually do).  Production path
    points it at ``settings.ingestion.cache_dir`` so re-ingestion of
    unchanged docs is a no-op.
    """
    if cache_dir is None:
        return None
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    persist_path = cache_path / "ingestion_cache.json"
    kvstore = (
        SimpleKVStore.from_persist_path(str(persist_path))
        if persist_path.exists()
        else SimpleKVStore()
    )
    return IngestionCache(
        cache=kvstore,
        collection="kb-llamaindex-ingestion",
    )


def build_ingestion_pipeline(
    *,
    embed_model: BaseEmbedding | None = None,
    semantic: bool = False,
    cache_dir: str | Path | None = None,
    extra_transformations: list[TransformComponent] | None = None,
) -> IngestionPipeline:
    """Compose an ``IngestionPipeline`` for the project.

    Args:
        embed_model: required if ``semantic=True``, ignored otherwise.
            Embeddings as a transformation step are attached in Stage 3
            (vector index) — at this stage we only chunk.
        semantic: switch to ``SemanticSplitterNodeParser`` instead of
            ``SentenceSplitter``.  Off by default — the cost (embed
            round-trip per chunk decision) outweighs the recall win on
            short documents.
        cache_dir: when set, persists transformation outputs so
            re-ingest of unchanged documents skips chunking.
        extra_transformations: hook for stages that bolt on more
            transforms (Stage 7 plugs in the canonical-identifier
            transform here).
    """
    transformations: list[TransformComponent] = [
        _build_splitter(semantic=semantic, embed_model=embed_model),
    ]
    if extra_transformations:
        transformations.extend(extra_transformations)
    return IngestionPipeline(
        transformations=transformations,
        cache=_build_cache(cache_dir),
    )


def read_documents(
    input_dir: str | Path,
    *,
    recursive: bool = True,
    required_exts: list[str] | None = None,
) -> list:
    """Thin wrapper around ``SimpleDirectoryReader`` used by tests and
    by the Stage 8 worker entry point.

    Default supported types: PDF, DOCX, PPTX, TXT, MD, EML — same set
    enterprise-kb covers via langchain readers.
    """
    reader = SimpleDirectoryReader(
        input_dir=str(input_dir),
        recursive=recursive,
        required_exts=required_exts,
    )
    return reader.load_data()

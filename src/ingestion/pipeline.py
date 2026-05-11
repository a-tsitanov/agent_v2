"""LlamaIndex ``IngestionPipeline`` factory.

The pipeline applies (in order):
  1. Splitter — `SentenceSplitter` (default) or
     `SemanticSplitterNodeParser` (opt-in via `semantic=True`,
     requires `embed_model`).
  2. `IdentifierCanonicalizationTransform` (default on) — populates
     `node.metadata["canonical_identifiers"]` and appends a
     canonical-identifier block to `node.text` so the LLM extractor
     downstream sees them.
  3. `extra_transformations` (callers can append more, e.g. KG
     extractor + description enricher in the worker).

Embedding generation is NOT part of the pipeline — it happens at
the vector-index insertion step (`src.retrieval.vector_index.
index_nodes`), since the same embedding model is also needed for
retrieval and tests sometimes mock it independently.
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
from src.ingestion.identifier_transform import (
    IdentifierCanonicalizationTransform,
)


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
    with_identifier_canon: bool = True,
    extra_transformations: list[TransformComponent] | None = None,
) -> IngestionPipeline:
    """Compose the project's ingestion pipeline.

    Args:
        embed_model: required when `semantic=True` (the semantic
            splitter calls embeddings to detect breakpoints).
            Ignored when `semantic=False`.
        semantic: switch from `SentenceSplitter` to
            `SemanticSplitterNodeParser`.  Off by default — the
            embed round-trip per chunk-decision rarely pays off on
            short documents.
        cache_dir: when set, persists transformation outputs so
            re-ingest of unchanged documents skips chunking +
            identifier extraction.
        with_identifier_canon: when True (default), the canonical
            identifier transform (phone → E.164, INN with checksum,
            etc.) runs between the splitter and any extra transforms.
            Turn off only for benchmarks that need to compare
            against a pipeline without identifier injection.
        extra_transformations: appended AFTER the identifier
            transform.  The worker uses this hook to add KG
            extractor + description enricher.
    """
    transformations: list[TransformComponent] = [
        _build_splitter(semantic=semantic, embed_model=embed_model),
    ]
    if with_identifier_canon:
        transformations.append(IdentifierCanonicalizationTransform())
    if extra_transformations:
        transformations.extend(extra_transformations)
    cache_obj = _build_cache(cache_dir)
    return IngestionPipeline(
        transformations=transformations,
        cache=cache_obj,
        # Without explicit `cache_dir` we don't want LlamaIndex's
        # default in-memory cache: it hashes by `transformation.to_dict()`
        # which collapses to identical strings for custom
        # `TransformComponent` subclasses (no class-name in the dict),
        # so the cache short-circuits later transforms with the
        # output of an earlier one.  Disabling avoids the collision.
        disable_cache=cache_obj is None,
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

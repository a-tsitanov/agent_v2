"""CLI: ingest a directory into Milvus.

Usage::

    python -m src.ingestion.run ./fixtures/
    python -m src.ingestion.run ./fixtures/ --recursive
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from loguru import logger

from src.config import settings
from src.ingestion.embeddings import build_embedding_model
from src.ingestion.pipeline import (
    build_ingestion_pipeline,
    read_documents,
)
from src.retrieval.vector_index import (
    build_vector_index,
    build_vector_store,
    index_nodes,
)
from src.utils.logging import configure_logging


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input_dir", type=Path, help="directory with documents")
    p.add_argument(
        "--recursive", action=argparse.BooleanOptionalAction, default=True,
    )
    p.add_argument(
        "--semantic",
        action="store_true",
        help="use SemanticSplitter (slower, requires real embed model)",
    )
    p.add_argument(
        "--overwrite-collection",
        action="store_true",
        help="DROP and recreate the Milvus collection — destructive",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    configure_logging(level=settings.api.log_level)

    from src.retrieval.llm_pool import get_llm_pool

    embed_model = build_embedding_model()
    pipeline = build_ingestion_pipeline(
        embed_model=embed_model if args.semantic else None,
        semantic=args.semantic,
        cache_dir=settings.ingestion.cache_dir,
        translator_llm=(
            get_llm_pool().get("extraction")
            if settings.ingestion.translate_to_russian else None
        ),
    )

    logger.info("reading documents from {p}", p=args.input_dir)
    docs = read_documents(args.input_dir, recursive=args.recursive)
    logger.info("read {n} documents", n=len(docs))

    nodes = pipeline.run(documents=docs)
    logger.info("pipeline produced {n} nodes", n=len(nodes))

    vector_store = build_vector_store(overwrite=args.overwrite_collection)
    index = build_vector_index(vector_store, embed_model)
    inserted = index_nodes(index, nodes)
    logger.info("inserted {n} nodes into Milvus", n=inserted)


if __name__ == "__main__":
    main()

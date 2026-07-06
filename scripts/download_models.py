"""Pre-download the project's HuggingFace models into a local cache.

Two models flow through HuggingFace (everything else goes through the
LiteLLM proxy):

  * GLiNER span-NER model — ``settings.ingestion.gliner_model``
    (default ``urchade/gliner_multi-v2.1``), used by the OPT-IN
    ``gliner`` / ``gliner+llm`` extractor modes.
  * BGE cross-encoder reranker — ``settings.hf.rerank_model``
    (default ``BAAI/bge-reranker-v2-m3``), used by the unified
    graph+vector rerank pass before synthesis.

Run this ONLINE on a box that can reach the Hub to populate the cache,
then copy the cache to the air-gapped host and run with
``HF_OFFLINE=true`` + ``HF_CACHE_DIR=<dir>`` (see docs/MODELS.md →
"Offline / air-gapped models").

Usage::

    python -m scripts.download_models                 # both models
    python -m scripts.download_models --models gliner
    python -m scripts.download_models --cache-dir /data/hf
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from huggingface_hub import snapshot_download
from loguru import logger

from src.config import settings
from src.utils.logging import configure_logging


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pre-download GLiNER + reranker into a local HF cache.",
    )
    parser.add_argument(
        "--models",
        choices=["all", "gliner", "reranker"],
        default="all",
        help="Which model(s) to download (default: all).",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="HF cache dir override (default: settings.hf.cache_dir or HF default).",
    )
    parser.add_argument(
        "--local-dir",
        default=None,
        help="Write FLAT real files here (no blobs/symlinks) instead of the HF "
             "cache — portable for air-gapped copy.  Load with local_files_only.",
    )
    return parser


def _resolve_cache_dir(cli_cache_dir: str | None) -> str | None:
    """CLI override > settings.hf.cache_dir > None (HF default)."""
    return cli_cache_dir or settings.hf.cache_dir


def _force_online(cache_dir: str | None) -> None:
    """Make THIS process online regardless of ambient offline env, and
    point the HF cache vars at ``cache_dir`` so the download lands where
    the loaders will read it.

    We download online by definition — explicitly clear any inherited
    ``HF_HUB_OFFLINE`` / ``TRANSFORMERS_OFFLINE`` so the script works even
    when the host env is configured for air-gapped runs.
    """
    os.environ["HF_HUB_OFFLINE"] = "0"
    os.environ["TRANSFORMERS_OFFLINE"] = "0"
    if cache_dir:
        for name in ("HF_HOME", "SENTENCE_TRANSFORMERS_HOME", "TRANSFORMERS_CACHE"):
            os.environ[name] = cache_dir


def _snapshot(repo_id: str, local_dir: str) -> None:  # pragma: no cover
    """Download a repo into a FLAT local dir — real files, no blobs/symlinks.

    Unlike the HF cache (``models--org--name/blobs/<sha>`` + ``snapshots/``
    symlinks), ``snapshot_download(local_dir=...)`` materialises real files
    in the repo's structure (hub >=0.23).  Portable: copy/tar/docker-COPY
    the folder to an air-gapped host without symlinks breaking."""
    dest = Path(local_dir) / repo_id.split("/")[-1]
    snapshot_download(repo_id=repo_id, local_dir=str(dest))
    logger.info("download_models: {r} -> {d}", r=repo_id, d=dest)


def _download_gliner(model_name: str) -> None:  # pragma: no cover
    """Download the GLiNER model into the cache."""
    from gliner import GLiNER

    logger.info("download_models: fetching GLiNER {m} ...", m=model_name)
    GLiNER.from_pretrained(model_name)


def _download_reranker(model_name: str) -> None:  # pragma: no cover
    """Download the cross-encoder reranker into the cache."""
    from sentence_transformers import SentenceTransformer

    logger.info("download_models: fetching reranker {m} ...", m=model_name)
    SentenceTransformer(model_name)


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    configure_logging()

    cache_dir = _resolve_cache_dir(args.cache_dir)
    _force_online(cache_dir)

    resolved = cache_dir or os.environ.get("HF_HOME") or "<HF default (~/.cache/huggingface)>"
    logger.info("download_models: target cache dir = {d}", d=resolved)

    gliner_model = settings.ingestion.gliner_model
    rerank_model = settings.hf.rerank_model

    want_gliner = args.models in ("all", "gliner")
    want_reranker = args.models in ("all", "reranker")

    try:
        if args.local_dir:
            # FLAT mode: real files, no blobs/symlinks — portable for
            # air-gapped copy.  Load by path with local_files_only=True.
            if want_gliner:
                _snapshot(gliner_model, args.local_dir)
            if want_reranker:
                _snapshot(rerank_model, args.local_dir)
            logger.info(
                "download_models: flat models in {d}. Point HF_RERANK_MODEL / "
                "gliner_model at <dir>/<model-leaf>, load local_files_only=True.",
                d=args.local_dir,
            )
            return 0

        if want_gliner:
            _download_gliner(gliner_model)
            logger.info("download_models: GLiNER ready ({m})", m=gliner_model)
        if want_reranker:
            _download_reranker(rerank_model)
            logger.info("download_models: reranker ready ({m})", m=rerank_model)
    except Exception as exc:
        logger.error("download_models: download FAILED: {e}", e=exc)
        return 1

    logger.info(
        "download_models: done. For air-gapped runs set "
        "HF_OFFLINE=true + HF_CACHE_DIR={d}",
        d=resolved,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

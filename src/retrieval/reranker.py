"""Cross-encoder reranker (BGE-reranker-v2-m3 by default).

Exposed as a separate factory because of its weight: pulls
``sentence-transformers``, downloads a model on first use, and runs
a transformer per chunk on inference.  Tests skip it; production
attaches it to the query engine as a ``NodePostprocessor``.

The model name + offline behaviour come from ``settings.hf`` so the
loader here and ``scripts/download_models.py`` share one source of
truth (see docs/MODELS.md → "Offline / air-gapped models").
"""

from __future__ import annotations

from llama_index.postprocessor.sbert_rerank import SentenceTransformerRerank

from src.config import settings
from src.retrieval.hf_offline import configure_hf


def build_reranker(
    *,
    model_name: str | None = None,
    top_n: int = 5,
) -> SentenceTransformerRerank:
    """Construct a cross-encoder reranker.

    ``model_name`` defaults to ``settings.hf.rerank_model`` (default
    ``BAAI/bge-reranker-v2-m3``).  First call downloads it (~1 GB for
    BGE-v2-m3) into the HuggingFace cache; air-gapped deploys pre-cache
    via ``scripts/download_models.py`` and set ``HF_OFFLINE=true`` +
    ``HF_CACHE_DIR=<dir>``.

    ``configure_hf()`` runs first so the cache dir / offline env vars
    are in place before ``sentence-transformers`` loads anything.
    """
    configure_hf()
    resolved_model = model_name or settings.hf.rerank_model

    # SentenceTransformerRerank forwards ``cache_folder`` to the
    # underlying CrossEncoder, and ``cross_encoder_kwargs`` straight into
    # its constructor (where ``local_files_only`` is a recognised
    # sentence-transformers kwarg).  Pass them as belt-and-suspenders on
    # top of the env vars configure_hf() already set.
    kwargs: dict = {"model": resolved_model, "top_n": top_n}
    if settings.hf.cache_dir:
        kwargs["cache_folder"] = settings.hf.cache_dir
    if settings.hf.offline:
        kwargs["cross_encoder_kwargs"] = {"local_files_only": True}

    return SentenceTransformerRerank(**kwargs)


__all__ = ["build_reranker"]

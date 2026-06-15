"""``classify_document`` — input gate run right after ``fetch_source``.

Decides whether a document is worth ingesting.  ``force`` bypasses the
deterministic rules.  Fail-soft: any error defaults to INGEST so a
classifier bug never silently drops good documents.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from temporalio import activity

from src.config import settings
from src.ingestion.classifier import apply_rules, classify_with_llm, read_preview
from src.workflow.contracts import ClassifyIn, ClassifyResult


@activity.defn
async def classify_document(inp: ClassifyIn) -> ClassifyResult:
    cfg = settings.classifier
    if inp.force:
        activity.logger.info("classify  doc=%s  forced (rules bypassed)", inp.ctx.doc_id)
        return ClassifyResult(ingest=True, reason="forced")

    try:
        path = Path(inp.ctx.local_path)
        size = path.stat().st_size if path.is_file() else 0

        verdict = apply_rules(
            path.name, size,
            max_size_mb=cfg.max_size_mb,
            min_size_bytes=cfg.min_size_bytes,
            skip_extensions=cfg.skip_extensions,
        )
        if verdict.skip:
            activity.logger.info(
                "classify  doc=%s  SKIP (rules): %s", inp.ctx.doc_id, verdict.reason,
            )
            return ClassifyResult(ingest=False, reason=verdict.reason)

        if not cfg.llm_enabled:
            return ClassifyResult(ingest=True, reason="rules-passed")

        preview = await asyncio.to_thread(read_preview, path, cfg.preview_chars)
        from src.retrieval.llm_pool import get_llm_pool

        llm = get_llm_pool().get("extraction")
        v = await classify_with_llm(preview, llm=llm)
        if not v.ingest:
            activity.logger.info(
                "classify  doc=%s  SKIP (llm): %s", inp.ctx.doc_id, v.reason,
            )
        return ClassifyResult(
            ingest=bool(v.ingest), reason=v.reason or "llm", doc_type=v.doc_type or "",
        )
    except Exception as exc:  # noqa: BLE001 — never lose a doc to a classifier bug
        activity.logger.warning(
            "classify_document failed, defaulting to ingest: %s", exc,
        )
        return ClassifyResult(ingest=True, reason="classifier-error-default-ingest")

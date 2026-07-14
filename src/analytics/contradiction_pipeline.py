"""End-to-end contradiction detection (hybrid method B, iteration 5).

Assembles the tested units into one offline pass:
  extract_claims (per doc) → cluster_claims (semantic) → structural detection
  per cluster → NLI-refine (drop phrasing-only).

All I/O is injected (extract/embed/NLI ``complete`` callables) so the pipeline
is unit-testable; the Temporal activity binds them to the real LLM/embedding
model. Pure orchestration — no store writes here (that's the activity's job).
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

from src.analytics.claim_cluster import detect_contradictions_clustered
from src.analytics.claim_extract import extract_claims
from src.analytics.claim_nli import refine_contradictions
from src.analytics.contradictions import Contradiction

Complete = Callable[[str], Awaitable[str]]
EmbedFn = Callable[[list[str]], Awaitable[list[list[float]]]]


async def detect_contradictions_e2e(
    docs: list[dict],
    *,
    extract_complete: Complete,
    embed: EmbedFn,
    nli_complete: Complete,
    threshold: float = 0.83,
) -> list[Contradiction]:
    """``docs`` = list of ``{"doc_id","source","text"}``. Returns NLI-confirmed
    contradictions across the batch."""
    all_claims = []
    for d in docs:
        claims = await extract_claims(
            d.get("text", ""), doc_id=d.get("doc_id", ""),
            source=d.get("source", ""), complete=extract_complete,
        )
        all_claims.extend(claims)
    if not all_claims:
        return []
    structural = await detect_contradictions_clustered(
        all_claims, embed=embed, threshold=threshold)
    return await refine_contradictions(structural, complete=nli_complete)

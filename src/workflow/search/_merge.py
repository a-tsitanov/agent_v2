"""Pure source-merge helpers for the plan-execute flow (R2).

Extracted as plain functions (no Temporal imports) so the orchestrator
and SubQuery workflows' core merge/dedup logic is unit-testable WITHOUT
a live Temporal test environment — the workflow bodies just call these.

Dedup is by ``chunk_id`` (a chunk may surface from both vector and
graph retrieval, or from overlapping sub-questions), keeping the FIRST
occurrence so the highest-priority source order is preserved.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.workflow.contracts import SerializedNode


def dedup_by_chunk_id(
    sources: Iterable[SerializedNode],
) -> list[SerializedNode]:
    """Drop duplicate ``SerializedNode``s by chunk_id, first wins."""
    seen: set[str] = set()
    out: list[SerializedNode] = []
    for n in sources:
        if n.chunk_id in seen:
            continue
        seen.add(n.chunk_id)
        out.append(n)
    return out


def merge_subquery_sources(
    results: Iterable[list[SerializedNode]],
) -> list[SerializedNode]:
    """Flatten per-sub-question source lists into one deduped list."""
    flat: list[SerializedNode] = []
    for group in results:
        flat.extend(group)
    return dedup_by_chunk_id(flat)

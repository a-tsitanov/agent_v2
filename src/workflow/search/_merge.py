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


def merge_subquery_sources_with_blocks(
    results: Iterable[list[SerializedNode]],
) -> tuple[list[SerializedNode], list[int]]:
    """Flatten per-sub-question source lists into one deduped list AND
    report how many chunks each input list contributed to it.

    The merged pool is a plain concatenation: block 1 is sub-question 1's
    hits, block 2 is sub-question 2's, and so on (plus one block per
    coverage round). Each block arrives sorted by its own retriever
    score, but those scores are NOT comparable across blocks — the whole
    point of the downstream cross-encoder is to put them on one scale.
    So the merged pool's *head* is not its best chunks, it is just the
    first sub-question, and anything that slices the pool by position
    needs these boundaries to avoid dropping whole facets of the query.

    The counts are post-dedup (a chunk already contributed by an earlier
    block is not counted again), so they always sum to ``len(pool)`` and
    can be used to cut the returned list back into blocks.
    """
    seen: set[str] = set()
    out: list[SerializedNode] = []
    sizes: list[int] = []
    for group in results:
        contributed = 0
        for n in group:
            if n.chunk_id in seen:
                continue
            seen.add(n.chunk_id)
            out.append(n)
            contributed += 1
        sizes.append(contributed)
    return out, sizes


def merge_subquery_sources(
    results: Iterable[list[SerializedNode]],
) -> list[SerializedNode]:
    """Flatten per-sub-question source lists into one deduped list."""
    return merge_subquery_sources_with_blocks(results)[0]

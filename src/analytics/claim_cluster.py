"""Semantic claim clustering (hybrid method B, iteration 3).

Claims from different sources describe the same fact slot with different words
('количество погибших' vs 'число жертв'), so exact ``(subject, attribute)``
grouping misses cross-source contradictions. Here we embed each claim's slot
(subject + attribute) and greedily cluster by cosine similarity, then run the
structural contradiction check (``contradiction_for_group``) per cluster.

``embed`` is injected (``list[str] -> list[vector]``) so this is unit-testable
without Milvus/nomic; the offline workflow binds it to the embedding model.
"""
from __future__ import annotations

import math
from collections.abc import Awaitable, Callable

from src.analytics.contradictions import Claim, Contradiction, contradiction_for_group

EmbedFn = Callable[[list[str]], Awaitable[list[list[float]]]]

# Slot text used for similarity: two claims about the same thing should embed
# close even when their exact wording differs. Value is EXCLUDED on purpose —
# it's what differs (the contradiction), not what identifies the slot.
def _slot_text(c: Claim) -> str:
    return f"{c.subject} {c.attribute}"


def cosine(a: list[float], b: list[float]) -> float:
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return sum(x * y for x, y in zip(a, b, strict=False)) / (na * nb)


async def cluster_claims(
    claims: list[Claim], *, embed: EmbedFn, threshold: float = 0.83,
) -> list[list[Claim]]:
    """Greedy single-pass clustering of claims by slot-embedding similarity.
    Each claim joins the first cluster whose seed is >= ``threshold`` similar,
    else it seeds a new cluster."""
    if not claims:
        return []
    vecs = await embed([_slot_text(c) for c in claims])
    clusters: list[list[Claim]] = []
    seeds: list[list[float]] = []
    for claim, vec in zip(claims, vecs, strict=False):
        for i, seed in enumerate(seeds):
            if cosine(vec, seed) >= threshold:
                clusters[i].append(claim)
                break
        else:
            clusters.append([claim])
            seeds.append(vec)
    return clusters


async def detect_contradictions_clustered(
    claims: list[Claim], *, embed: EmbedFn, threshold: float = 0.83,
) -> list[Contradiction]:
    """Cluster claims semantically, then flag clusters where sources disagree."""
    clusters = await cluster_claims(claims, embed=embed, threshold=threshold)
    return [c for cl in clusters if (c := contradiction_for_group(cl))]

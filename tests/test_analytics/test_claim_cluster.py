"""Semantic claim clustering (TDD): group claims about the same fact slot
across different phrasings. `embed` injected (fake vectors) — no Milvus/nomic."""
from __future__ import annotations

import pytest

from src.analytics.claim_cluster import cluster_claims, cosine, detect_contradictions_clustered
from src.analytics.contradictions import Claim

# key text = f"{subject} {attribute}"
_VECS = {
    "удар количество погибших": [1.0, 0.0, 0.0],
    "удар число жертв":         [0.98, 0.02, 0.0],  # semantically ~ the same slot
    "погода температура":       [0.0, 1.0, 0.0],    # unrelated
}


async def _embed(texts: list[str]) -> list[list[float]]:
    return [_VECS[t] for t in texts]


def test_cosine_basic():
    assert cosine([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_similar_keys_cluster_together():
    claims = [
        Claim(subject="удар", attribute="количество погибших", value="5", source="A"),
        Claim(subject="удар", attribute="число жертв", value="8", source="B"),
        Claim(subject="погода", attribute="температура", value="+30", source="C"),
    ]
    clusters = await cluster_claims(claims, embed=_embed, threshold=0.9)
    assert sorted(len(c) for c in clusters) == [1, 2]  # {погибло,жертвы} + {погода}


@pytest.mark.asyncio
async def test_clustered_detection_finds_cross_phrasing_contradiction():
    claims = [
        Claim(subject="удар", attribute="количество погибших", value="5", source="A"),
        Claim(subject="удар", attribute="число жертв", value="8", source="B"),
    ]
    out = await detect_contradictions_clustered(claims, embed=_embed, threshold=0.9)
    assert len(out) == 1
    assert {v.value for v in out[0].versions} == {"5", "8"}

"""Synthetic generators for the scale-bench harness.

Everything is derived from a seed — no production data is read or
exported.  The *shape* (entity count, label skew, duplicate rate,
embedding-cluster spread, degree distribution) is what drives the
scaling cliffs, so we generate it directly and sweep it.

Two consumers:

* ``gen_items`` → ``_Item`` list fed to the REAL
  ``src.graph.entity_resolution._candidate_pairs`` (ER O(N²) cost +
  duplicate-candidate recall).  Embedding *values* don't change the
  O(N²) cost — every within-label pair is compared regardless — so
  random unit vectors give faithful timing; injected near-duplicate
  clusters give a faithful recall signal.
* ``gen_vectors`` → a plain ``(N, dim)`` float32 matrix + query set for
  the Milvus FLAT-vs-HNSW benchmark.
* ``gen_edges`` → a degree-skewed edge list (with planted hubs) for the
  graph_walk hub-cliff benchmark against a local Neo4j.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.graph.entity_resolution import _Item, _normalize_entity_name

# Rough label skew of a Russian B2B / support corpus — Persons and Orgs
# dominate, which is exactly where the within-label O(N²) candidate-gen
# blows up.  Override per run to bracket the real distribution.
DEFAULT_LABEL_DIST: dict[str, float] = {
    "Person": 0.45,
    "Organization": 0.20,
    "Location": 0.10,
    "Concept": 0.10,
    "Product": 0.08,
    "Topic": 0.07,
}


def _unit_rows(rng: np.random.Generator, n: int, dim: int) -> np.ndarray:
    """n random unit (L2-normalised) row vectors."""
    v = rng.standard_normal((n, dim)).astype("float32")
    v /= np.linalg.norm(v, axis=1, keepdims=True) + 1e-12
    return v


def _assign_labels(
    rng: np.random.Generator, n: int, dist: dict[str, float],
) -> list[str]:
    labels = list(dist.keys())
    weights = np.array(list(dist.values()), dtype="float64")
    weights /= weights.sum()
    idx = rng.choice(len(labels), size=n, p=weights)
    return [labels[i] for i in idx]


@dataclass
class ItemSet:
    items: list[_Item]
    # True duplicate pairs as frozenset({norm_a, norm_b}); a perfect
    # candidate generator surfaces all of these.
    gold_pairs: set[frozenset[str]]


def gen_items(
    *,
    n: int,
    dim: int = 768,
    label_dist: dict[str, float] | None = None,
    dup_rate: float = 0.0,
    dup_jitter: float = 0.03,
    with_description: bool = True,
    seed: int = 7,
) -> ItemSet:
    """Generate ``n`` synthetic ``_Item`` entities with unit embeddings.

    ``dup_rate`` of them are planted near-duplicates of an earlier item
    (same vector + small jitter → high cosine, distinct name) — these
    SHOULD be surfaced as candidates / merged.  ``gold_pairs`` records
    the planted truth so recall can be measured.
    """
    rng = np.random.default_rng(seed)
    dist = label_dist or DEFAULT_LABEL_DIST
    labels = _assign_labels(rng, n, dist)
    base = _unit_rows(rng, n, dim)

    items: list[_Item] = []
    gold: set[frozenset[str]] = set()

    n_dups = int(n * dup_rate)
    dup_targets = set(rng.choice(n, size=n_dups, replace=False).tolist()) if n_dups else set()

    for i in range(n):
        name = f"Entity {i:07d}"
        norm = _normalize_entity_name(name)
        desc = f"Synthetic entity number {i} of type {labels[i]}." if with_description else ""
        items.append(_Item(
            name=name, norm=norm, label=labels[i],
            description=desc, mention_count=int(rng.integers(1, 25)),
            source="new", embedding=base[i].tolist(),
        ))

    # Plant near-duplicates: a variant sharing the original's label +
    # (jittered) vector, so it lands above the cosine floor.
    for di, target in enumerate(sorted(dup_targets)):
        v = base[target] + dup_jitter * rng.standard_normal(dim).astype("float32")
        v /= np.linalg.norm(v) + 1e-12
        name = f"Entity {target:07d} (var {di})"
        norm = _normalize_entity_name(name)
        items.append(_Item(
            name=name, norm=norm, label=items[target].label,
            description=items[target].description,
            mention_count=int(rng.integers(1, 25)),
            source="new", embedding=v.tolist(),
        ))
        gold.add(frozenset({items[target].norm, norm}))

    return ItemSet(items=items, gold_pairs=gold)


def gen_vectors(
    *,
    n: int,
    dim: int = 768,
    n_queries: int = 100,
    n_clusters: int | None = None,
    cluster_spread: float = 0.35,
    query_jitter: float = 0.10,
    seed: int = 7,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(corpus[n,dim], queries[n_queries,dim])`` unit vectors
    with realistic CLUSTER structure.

    Uniform random vectors in high dim are near-equidistant (curse of
    dimensionality): every point's "10 nearest" are near-ties, so
    ANN-vs-exhaustive recall@k looks artificially terrible even though
    latency is faithful.  Real embeddings cluster by topic, so we
    generate ``n_clusters`` centroids and scatter points around them.

    Noise is scaled by ``1/sqrt(dim)`` so ``cluster_spread`` /
    ``query_jitter`` are the *relative* L2 norm of the offset vs the
    unit centroid — otherwise ``spread · N(0,1)^dim`` has norm
    ``spread·sqrt(dim)`` which in 768-d dwarfs the centroid and destroys
    the cluster structure (the bug that made recall look like ~0.3).
    With the defaults a point sits at cosine ≈0.94 to its centroid, so
    each query has genuine near neighbours a correctly-tuned HNSW
    recovers — making recall@k meaningful.
    """
    rng = np.random.default_rng(seed)
    n_clusters = n_clusters or max(8, n // 500)
    centroids = _unit_rows(rng, n_clusters, dim)
    assign = rng.integers(0, n_clusters, size=n)
    sigma = cluster_spread / (dim ** 0.5)
    corpus = centroids[assign] + sigma * rng.standard_normal(
        (n, dim)
    ).astype("float32")
    corpus /= np.linalg.norm(corpus, axis=1, keepdims=True) + 1e-12

    qsigma = query_jitter / (dim ** 0.5)
    pick = rng.choice(n, size=n_queries, replace=False)
    q = corpus[pick] + qsigma * rng.standard_normal(
        (n_queries, dim)
    ).astype("float32")
    q /= np.linalg.norm(q, axis=1, keepdims=True) + 1e-12
    return corpus.astype("float32"), q.astype("float32")


def gen_edges(
    *,
    n_nodes: int,
    avg_degree: float = 4.0,
    hub_degrees: tuple[int, ...] = (1000,),
    seed: int = 7,
) -> tuple[list[tuple[int, int]], list[int]]:
    """Generate an undirected edge list with planted hubs.

    Returns ``(edges, hub_node_ids)``.  Most nodes get ~``avg_degree``
    random edges; the planted hub nodes get the requested high degrees
    — the shape that makes ``(e)-[*1..2]-`` enumeration explode.
    """
    rng = np.random.default_rng(seed)
    edges: list[tuple[int, int]] = []
    m = int(n_nodes * avg_degree / 2)
    src = rng.integers(0, n_nodes, size=m)
    dst = rng.integers(0, n_nodes, size=m)
    edges.extend((int(a), int(b)) for a, b in zip(src, dst) if a != b)

    hubs: list[int] = []
    for h, deg in enumerate(hub_degrees):
        hub = h  # low ids reserved as hubs
        hubs.append(hub)
        nbrs = rng.integers(0, n_nodes, size=deg)
        edges.extend((hub, int(x)) for x in nbrs if x != hub)
    return edges, hubs

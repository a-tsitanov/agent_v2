"""ER candidate-generation scaling + duplicate-recall benchmark.

Drives the REAL ``src.graph.entity_resolution._candidate_pairs`` with
synthetic ``_Item`` sets — no DB, no LLM, no network.  Measures:

* **Cost curve** — wall-time vs entity count.  ``_candidate_pairs``
  compares every within-label pair before keeping the top-k, so the
  cost is O((N/L)² · dim) per label L.  This is the P0.2 claim; the
  curve shows where it stops being affordable.

* **Candidate recall** — of the planted near-duplicate pairs, how many
  are surfaced as candidates (auto ∪ borderline).  A pair that never
  becomes a candidate can never be merged — so this is the ceiling on
  dedup quality, independent of the LLM judge.

Both run with zero production data; calibrate ``label_dist`` / ``dup_rate``
to bracket the real graph.
"""
from __future__ import annotations

import time

from src.graph.entity_resolution import ERConfig, _candidate_pairs

from tests.eval.scale.synth import gen_items


def _candidate_norm_pairs(items, cfg) -> set[frozenset[str]]:
    """Run _candidate_pairs and return the surfaced pairs as norm-sets."""
    auto, borderline = _candidate_pairs(items, cfg)
    norm_of = {it.name: it.norm for it in items}
    out: set[frozenset[str]] = set()
    for a_name, b_name, _cos in [*auto, *borderline]:
        na, nb = norm_of.get(a_name), norm_of.get(b_name)
        if na and nb and na != nb:
            out.add(frozenset({na, nb}))
    return out


def bench_cost_curve(
    sizes: list[int],
    *,
    dim: int = 768,
    label_dist: dict[str, float] | None = None,
    cfg: ERConfig | None = None,
    seed: int = 7,
) -> list[dict]:
    """Time ``_candidate_pairs`` at each entity count.  Returns one row
    per size; ``growth_vs_linear`` is (time-ratio ÷ N-ratio) between
    consecutive sizes — ≈2 ⇒ quadratic O(N²), ≈1 ⇒ linear."""
    cfg = cfg or ERConfig()
    rows: list[dict] = []
    prev_t: float | None = None
    prev_n: int | None = None
    for n in sizes:
        items = gen_items(
            n=n, dim=dim, label_dist=label_dist, dup_rate=0.0, seed=seed,
        ).items
        t0 = time.perf_counter()
        auto, borderline = _candidate_pairs(items, cfg)
        elapsed = time.perf_counter() - t0
        growth = ""
        if prev_t and prev_t > 0 and prev_n:
            ratio_n = n / prev_n
            growth = f"{(elapsed / prev_t) / (ratio_n):.2f}x/×N"  # cost-per-N multiple
        rows.append({
            "n": n,
            "seconds": round(elapsed, 4),
            "candidates": len(auto) + len(borderline),
            "growth_vs_linear": growth,
        })
        prev_t, prev_n = elapsed, n
    return rows


def bench_dedup_recall(
    *,
    n: int,
    dup_rate: float = 0.1,
    dim: int = 768,
    knn_ks: tuple[int, ...] = (10,),
    label_dist: dict[str, float] | None = None,
    seed: int = 7,
) -> list[dict]:
    """For each ``knn_k``, fraction of planted duplicate pairs surfaced
    as candidates.  Shows how recall trades off against the top-k cap."""
    iset = gen_items(
        n=n, dim=dim, label_dist=label_dist, dup_rate=dup_rate, seed=seed,
    )
    gold = iset.gold_pairs
    rows: list[dict] = []
    for k in knn_ks:
        cfg = ERConfig(knn_k=k)
        surfaced = _candidate_norm_pairs(iset.items, cfg)
        hit = len(gold & surfaced)
        rows.append({
            "knn_k": k,
            "gold_pairs": len(gold),
            "recalled": hit,
            "recall": round(hit / len(gold), 3) if gold else None,
        })
    return rows

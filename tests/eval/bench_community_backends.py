"""Strict-parity benchmark: GDS vs leidenalg vs graphscope community detection.

Run: uv run python -m tests.eval.bench_community_backends
Skips (exit 0) when Neo4j is unreachable.  Reports modularity, community
count, size distribution, wall time, and peak memory per backend so the
default flip (community_backend -> leidenalg) is an evidence-based decision.

The graphscope arm (single-level, same graph/seed as the leidenalg baseline)
is best-effort: `_run_graphscope_community` needs a GraphScope cluster and
its adapter body is a manual-gate finalize (see community_graphscope.py), so
this script skips that arm cleanly rather than printing a fake result.
"""

from __future__ import annotations

import time
import tracemalloc
from math import comb

from src.config import settings
from src.graph.community_leiden import build_graph, extract_entity_edges
from src.workflow.search.activities.community import _get_store


def _modularity(edges, nodes, name_to_cid) -> float:
    import leidenalg as la
    g, names = build_graph(edges, nodes)
    membership = [int(name_to_cid.get(n, -1)) for n in names]
    part = la.RBConfigurationVertexPartition(
        g, initial_membership=membership,
        weights=g.es["weight"] if "weight" in g.es.attributes() else None,
    )
    return part.quality()


def _ari(a: dict[str, str], b: dict[str, str]) -> float:
    """Adjusted Rand Index between two {name -> communityId} partitions.

    Computed straight from the pairwise contingency table (stdlib only —
    sklearn isn't a project dependency) so graphscope-vs-leidenalg community
    parity can be reported without adding one just for this benchmark.
    """
    from collections import Counter

    names = [n for n in a if n in b]
    n = len(names)
    if n < 2:
        return 1.0
    contingency: Counter[tuple[str, str]] = Counter((a[nm], b[nm]) for nm in names)
    row_totals: Counter[str] = Counter()
    col_totals: Counter[str] = Counter()
    for (ra, cb), cnt in contingency.items():
        row_totals[ra] += cnt
        col_totals[cb] += cnt
    sum_comb_c = sum(comb(cnt, 2) for cnt in contingency.values())
    sum_comb_a = sum(comb(cnt, 2) for cnt in row_totals.values())
    sum_comb_b = sum(comb(cnt, 2) for cnt in col_totals.values())
    total_comb = comb(n, 2)
    expected = (sum_comb_a * sum_comb_b) / total_comb if total_comb else 0.0
    max_index = 0.5 * (sum_comb_a + sum_comb_b)
    denom = max_index - expected
    if denom == 0:
        return 1.0
    return (sum_comb_c - expected) / denom


def _run_graphscope_arm(edges, nodes, leiden_single_cid, *, gamma, seed=19) -> None:
    """GraphScope single-level arm: build time + modularity + ARI parity vs
    the leidenalg single-level partition (same edges/nodes/gamma/seed).

    Skips cleanly when GraphScope is unavailable: `_run_graphscope_community`
    fails open to `{}`, so `single_level_rows_graphscope` returns every name
    mapped to communityId "0" — a degenerate single-community partition. We
    detect that (<=1 distinct community) and print a skip notice instead of
    reporting it as a real modularity/parity result.
    """
    from src.graph.community_graphscope import single_level_rows_graphscope

    t0 = time.perf_counter()
    rows = single_level_rows_graphscope(edges, nodes, gamma=gamma, seed=seed)
    dt = time.perf_counter() - t0
    gs_cid = {r["name"]: r["communityId"] for r in rows}
    ncomm = len(set(gs_cid.values()))
    if ncomm <= 1:
        print(
            "[graphscope] skipped — adapter unavailable (no cluster / "
            "manual gate not finalized): partition degenerated to a single "
            "community, not a real result",
        )
        return
    mod = _modularity(edges, nodes, gs_cid)
    ari = _ari(leiden_single_cid, gs_cid)
    print(f"[graphscope] time={dt:.1f}s communities={ncomm} "
          f"modularity={mod:.4f} ari_vs_leidenalg={ari:.4f}")


def main() -> int:
    store = _get_store()
    if store is None:
        print("Neo4j unreachable — benchmark skipped")
        return 0

    edges, nodes = extract_entity_edges(store)
    print(f"graph: {len(nodes)} entities / {len(edges)} edges")

    gamma = settings.temporal.community_leiden_gamma
    seed = 19

    from src.graph.community_leiden import hierarchy_rows
    for label, fn in [("leidenalg", lambda: hierarchy_rows(
            edges, nodes, gamma=gamma,
            max_levels=settings.agent.community_max_levels))]:
        tracemalloc.start()
        t0 = time.perf_counter()
        rows = fn()
        dt = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        coarsest = {r["name"]: r["ids"][-1] for r in rows}
        ncomm = len(set(coarsest.values()))
        mod = _modularity(edges, nodes, coarsest)
        print(f"[{label}] time={dt:.1f}s peak_rss~{peak/1e6:.0f}MB "
              f"communities={ncomm} modularity={mod:.4f}")
    # NB: GDS comparison is run separately by flipping community_backend=gds
    # and re-running the rebuild; this script measures the leidenalg side +
    # its modularity so it can be compared to the GDS modularity from logs.

    # --- graphscope arm: same graph + seed, single-level vs leidenalg ---
    from src.graph.community_leiden import single_level_rows
    leiden_single_cid = {
        r["name"]: r["communityId"]
        for r in single_level_rows(edges, nodes, gamma=gamma, seed=seed)
    }
    _run_graphscope_arm(edges, nodes, leiden_single_cid, gamma=gamma, seed=seed)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

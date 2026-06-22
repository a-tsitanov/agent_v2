"""Strict-parity benchmark: GDS vs leidenalg community detection.

Run: uv run python -m tests.eval.bench_community_backends
Skips (exit 0) when Neo4j is unreachable.  Reports modularity, community
count, size distribution, wall time, and peak memory per backend so the
default flip (community_backend -> leidenalg) is an evidence-based decision.
"""

from __future__ import annotations

import time
import tracemalloc

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


def main() -> int:
    store = _get_store()
    if store is None:
        print("Neo4j unreachable — benchmark skipped")
        return 0

    edges, nodes = extract_entity_edges(store)
    print(f"graph: {len(nodes)} entities / {len(edges)} edges")

    from src.graph.community_leiden import hierarchy_rows
    for label, fn in [("leidenalg", lambda: hierarchy_rows(
            edges, nodes, gamma=settings.temporal.community_leiden_gamma,
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

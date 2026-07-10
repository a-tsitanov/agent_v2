"""GraphScope community-detection backend (single-level Leiden, distributed).

Mirrors community_leiden.py's single-level entry, but runs on GraphScope so
detection scales off single-machine igraph / off Neo4j's GDS heap. The ONLY
GraphScope-touching code is `_run_graphscope_community`; everything else is
pure and unit-testable by mocking it. Selected via community_backend='graphscope'.
"""

from __future__ import annotations

from loguru import logger


def _all_names(edges: list[tuple[str, str, float]], node_names: list[str]) -> list[str]:
    """Dedup names across node_names + edge endpoints (mirrors
    community_leiden.build_graph's name set)."""
    return list(dict.fromkeys(
        list(node_names) + [e[0] for e in edges] + [e[1] for e in edges],
    ))


def _run_graphscope_community(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int,
) -> dict[str, str]:
    """Build a GraphScope graph from `edges` and run its modularity community
    algorithm; return {entity_name -> communityId}.

    NOTE (manual-gate): the exact GraphScope API — session bootstrap, graph
    load, and the algorithm call (native `leiden` if available, else
    `louvain`) — is finalized against the INSTALLED GraphScope on the cluster.
    Import is lazy so nothing here is needed for the DB-free unit tests (which
    mock this whole function). Fail-open: any error -> {} — the {} sentinel
    is what tells `single_level_rows_graphscope`'s guard to short-circuit to
    [] (a real, non-empty membership dict is never mistaken for a failure).
    """
    try:
        import graphscope  # noqa: F401  (lazy; heavy cluster dep)
        # --- finalize against installed GraphScope at the manual gate ---
        # sess = graphscope.session(cluster_type=...)
        # g = sess.load_from(edges=...)  # weighted undirected
        # ctx = graphscope.<leiden|louvain>(g, resolution=gamma, ...)
        # return {name: str(cid) for name, cid in ctx.to_dataframe(...)...}
        raise NotImplementedError(
            "GraphScope community call not finalized — complete against the "
            "installed GraphScope on the cluster (manual gate).",
        )
    except Exception as exc:
        logger.warning("graphscope community run failed: {e}", e=exc)
        return {}


def single_level_rows_graphscope(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int = 19,
) -> list[dict]:
    """Flat GraphScope partition -> rows [{name, communityId, ids:[cid]}]
    (same shape as community_leiden.single_level_rows)."""
    membership = _run_graphscope_community(edges, node_names, gamma=gamma, seed=seed)
    if not membership:
        # Adapter fail-open (import/API error) returns {} -> true no-op,
        # matching the leidenalg branch's raise->[] semantics. A non-empty
        # membership (even a single real community) is preserved as a result.
        return []
    rows: list[dict] = []
    for name in _all_names(edges, node_names):
        cid = str(membership.get(name, "0"))
        rows.append({"name": name, "communityId": cid, "ids": [cid]})
    return rows

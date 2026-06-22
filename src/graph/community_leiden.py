"""In-worker Leiden community detection (leidenalg/igraph).

Produces the SAME ``rows`` shape as the GDS path
(``[{name, communityId, ids}]``, ``ids`` finest->coarsest) so
``communities._coarsest_from_rows`` / ``_group_by_levels`` and all
:Community persistence are reused unchanged.  Memory lives in the worker
process, not Neo4j's JVM heap.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

# Keyset-paginated reads so we never materialise one giant result set.
_NODES_CYPHER = """
MATCH (e:__Entity__)
WHERE $after = '' OR e.name > $after
RETURN e.name AS name
ORDER BY e.name
LIMIT $limit
"""

_EDGES_CYPHER = """
MATCH (s:__Entity__)-[r]->(t:__Entity__)
WHERE $after = '' OR s.name > $after
RETURN s.name AS src, t.name AS tgt,
       coalesce(r.weight, 1.0) AS weight, s.name AS cursor
ORDER BY s.name
LIMIT $limit
"""


def extract_entity_edges(
    store: Any, *, batch_size: int = 50_000,
) -> tuple[list[tuple[str, str, float]], list[str]]:
    """Stream the ``__Entity__`` graph out of Neo4j as (edges, node_names)."""
    names: list[str] = []
    after = ""
    while True:
        page = store.structured_query(
            _NODES_CYPHER, param_map={"after": after, "limit": batch_size},
        )
        if not page:
            break
        for row in page:
            n = row.get("name")
            if n is not None:
                names.append(str(n))
        last_name = page[-1].get("name")
        if last_name is None or not (str(last_name) > after):
            break
        after = str(last_name)
        if len(page) < batch_size:
            break

    edges: list[tuple[str, str, float]] = []
    after = ""
    while True:
        page = store.structured_query(
            _EDGES_CYPHER, param_map={"after": after, "limit": batch_size},
        )
        if not page:
            break
        for row in page:
            s, t = row.get("src"), row.get("tgt")
            if s is not None and t is not None:
                edges.append((str(s), str(t), float(row.get("weight") or 1.0)))
        last_cursor = page[-1].get("cursor")
        if last_cursor is None or not (str(last_cursor) > after):
            break
        after = str(last_cursor)
        if len(page) < batch_size:
            break

    logger.info(
        "community_leiden: streamed {e} edges / {n} entities from Neo4j",
        e=len(edges), n=len(names),
    )
    return edges, names


def build_graph(
    edges: list[tuple[str, str, float]], node_names: list[str],
) -> tuple[Any, list[str]]:
    """Build an undirected weighted igraph; parallel edges summed."""
    import igraph as ig

    names: list[str] = list(dict.fromkeys(
        list(node_names) + [e[0] for e in edges] + [e[1] for e in edges],
    ))
    idx = {n: i for i, n in enumerate(names)}
    g = ig.Graph(n=len(names), directed=False)
    elist = [(idx[s], idx[t]) for s, t, _ in edges if s in idx and t in idx]
    weights = [w for s, t, w in edges if s in idx and t in idx]
    g.add_edges(elist)
    if weights:
        g.es["weight"] = weights
    # Collapse parallel/self edges (GDS undirected projection is simple).
    # NB: simplify() mutates in place — do NOT reassign (it can return None).
    g.simplify(multiple=True, loops=True, combine_edges={"weight": "sum"})
    return g, names


def single_level_rows(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int = 19,
) -> list[dict]:
    """Flat leidenalg partition → rows ``[{name, communityId, ids:[cid]}]``."""
    import leidenalg as la

    g, names = build_graph(edges, node_names)
    weights = g.es["weight"] if "weight" in g.es.attributes() else None
    part = la.find_partition(
        g, la.RBConfigurationVertexPartition,
        weights=weights, resolution_parameter=gamma, seed=seed,
    )
    membership = part.membership  # community index per vertex
    rows: list[dict] = []
    for i, name in enumerate(names):
        cid = str(membership[i])
        rows.append({"name": name, "communityId": cid, "ids": [cid]})
    return rows

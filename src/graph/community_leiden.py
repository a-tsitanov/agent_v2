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

"""Backend-dispatched graph edge EXPORT (Leiden read-phase).

``Neo4jGraphEdgeExport`` wraps the existing keyset-paginated Cypher
constants verbatim (default path, byte-for-byte unchanged; the constants
and pagination loops were MOVED here from
``community_leiden.extract_entity_edges``). ``NebulaGraphEdgeExport``
translates the same read to nGQL: a keyset ``LOOKUP`` for node names, then
a batched ``GO ... OVER RELATED`` edge scan keyed by VID.
"""
from __future__ import annotations

from typing import Any, Protocol

from src.config import settings

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
WHERE $after = '' OR elementId(r) > $after
RETURN s.name AS src, t.name AS tgt,
       coalesce(r.weight, 1.0) AS weight, elementId(r) AS cursor
ORDER BY elementId(r)
LIMIT $limit
"""


class GraphEdgeExport(Protocol):
    def stream_names(self, *, batch_size: int) -> list[str]: ...

    def stream_edges(
        self, *, batch_size: int, names: list[str] | None = None,
    ) -> list[tuple[str, str, float]]: ...


class Neo4jGraphEdgeExport:
    """Runs the historical keyset-paginated Cypher verbatim — zero behaviour
    change from the pre-seam ``extract_entity_edges`` implementation."""

    def __init__(self, store: Any):
        self._store = store

    def stream_names(self, *, batch_size: int) -> list[str]:
        names: list[str] = []
        after = ""
        while True:
            page = self._store.structured_query(
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
        return names

    def stream_edges(
        self, *, batch_size: int, names: list[str] | None = None,
    ) -> list[tuple[str, str, float]]:
        # `names` is accepted for Protocol parity with NebulaGraphEdgeExport
        # but IGNORED here — neo4j runs its own self-contained _EDGES_CYPHER
        # query and never needs the node-name set up front. Behaviour
        # unchanged from the pre-seam implementation.
        #
        # Edge pagination uses elementId(r) as cursor — elementId is unique
        # per relationship, so the cursor strictly advances every page
        # regardless of how many edges share the same source node.  A
        # source with more edges than batch_size is therefore never
        # silently truncated.
        edges: list[tuple[str, str, float]] = []
        after = ""
        while True:
            page = self._store.structured_query(
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
        return edges


class NebulaGraphEdgeExport:
    """nGQL graph edge EXPORT. Node names via keyset ``LOOKUP``; edges via a
    batched ``GO ... OVER RELATED`` scan keyed by VID (``entity_vid``).
    Values are inline-quoted (nebula binds no params)."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def stream_names(self, *, batch_size: int) -> list[str]:
        from src.graph.nebula_store import _q

        names: list[str] = []
        after = ""
        while True:
            rows = self._exec(
                f"LOOKUP ON `Entity` WHERE `Entity`.name > {_q(after)} "
                "YIELD `Entity`.name AS name | ORDER BY $-.name ASC "
                f"LIMIT {int(batch_size)};"
            )
            if not rows:
                break
            for row in rows:
                n = row.get("name")
                if n is not None:
                    names.append(str(n))
            last_name = rows[-1].get("name")
            if last_name is None or not (str(last_name) > after):
                break
            after = str(last_name)
            if len(rows) < batch_size:
                break
        return names

    def stream_edges(
        self, *, batch_size: int, names: list[str] | None = None,
    ) -> list[tuple[str, str, float]]:
        from src.graph.nebula_store import _chunks, _q, entity_vid

        # If the caller already streamed the node names (the common case —
        # extract_entity_edges always calls stream_names first), reuse them
        # and SKIP the internal re-scan. Only fall back to our own LOOKUP
        # scan when called standalone with names=None.
        if names is None:
            names = self.stream_names(batch_size=batch_size)
        vid2name = {entity_vid(n): n for n in names}

        edges: list[tuple[str, str, float]] = []
        vids = list(vid2name)
        for chunk in _chunks(vids, batch_size):
            listed = ", ".join(_q(v) for v in chunk)
            rows = self._exec(
                f"GO FROM {listed} OVER `RELATED` YIELD "
                "src(edge) AS s, dst(edge) AS d, `RELATED`.weight AS w;"
            )
            for row in rows:
                s, d = row.get("s"), row.get("d")
                if s in vid2name and d in vid2name:
                    w = row.get("w")
                    edges.append(
                        (vid2name[s], vid2name[d], float(w if w is not None else 1.0)),
                    )
        return edges


def build_graph_edge_export(store: Any) -> GraphEdgeExport:
    if settings.graph.backend == "nebula":
        return NebulaGraphEdgeExport(store)
    return Neo4jGraphEdgeExport(store)

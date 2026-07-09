"""The graph-store surface the app actually uses.

A narrow subset of LlamaIndex's ``PropertyGraphStore`` — the three methods
every graph caller funnels through.  Any backend (Neo4j today, NebulaGraph
after the migration) that structurally satisfies this Protocol can be
returned by ``src.graph.store.build_graph_store()``.  Keeping the seam this
small is what makes the strangler migration a per-method job, not a
per-call-site one.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class KbGraphStore(Protocol):
    def structured_query(
        self, query: str, param_map: dict[str, Any] | None = None
    ) -> list[dict]: ...

    def upsert_nodes(self, nodes: list[Any]) -> None: ...

    def upsert_relations(self, relations: list[Any]) -> None: ...

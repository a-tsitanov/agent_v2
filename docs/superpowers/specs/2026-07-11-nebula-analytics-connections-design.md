# Nebula analytics — connections primitive (nGQL) + AnalyticsGraphOps pattern

**Status:** proposed (autonomous, delegated — user chose "port analytics to nGQL") 2026-07-11. NebulaGraph migration. FIRST analytics-primitive port; establishes the `AnalyticsGraphOps` seam pattern the rest of the Tier-A primitives will follow. Branch `feat/nebula-analytics-connections` off `main` (base `ce09a35`).

## Goal

Under `GRAPH_BACKEND=nebula`, `analytics/primitives/connections.py`'s entity-neighborhood reads run via nGQL (they currently fail-open to `[]` — all analytics does under nebula). Establish a reusable `AnalyticsGraphOps` seam (Neo4j = current Cypher verbatim; Nebula = nGQL) that the remaining Tier-A primitives (aggregations, quality, signals, domain, events, dynamics, rollups, alerts) will extend. This is 1 of ~10 Tier-A slices. Neo4j path byte-for-byte unchanged.

## Background (grounded)

`connections.py` primitives call `run_rows(store, cypher, params)` (`analytics/store_query.py` — fail-soft `try/except → []`). Under nebula `run_rows`→`structured_query(param_map=...)` raises → caught → `[]`. The queries:
- `entity_dossier`: `_CORE` (entity props), `_NEIGHBORS` (`(e {name})-[r]-(n)` + rel/weight), `_IDENTIFIERS` (neighbours filtered by label ∈ id_types), `_COMMUNITIES` (`(e)-[:IN_COMMUNITY]->(c:Community)`).
- `neighbors_by_relation` (filtered neighbours), `common_connections` (`(x)-[r1]-(m)-[r2]-(y)` 2-hop bridge), `identifier_lookup` (`(id {name})-[r]-(e)`), `shared_identifier_entities` (`(id) WHERE label∈id_types` then `(id)-[]-(owner)` grouping).
- `connection_path`: `shortestPath((a)-[*..hops]-(b))` → nebula `FIND SHORTEST PATH FROM <va> TO <vb> OVER * UPTO <hops> STEPS`.
- `cooccurrence`: `(e)<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->(other)` — **Chunk-dependent → `[]` under nebula (deferred, like doc↔community/wiki-citations)**.

Nebula: Entity VID = `entity_vid(name)`; `GO ... OVER \`RELATED\` BIDIRECT` for neighbours; `GO ... OVER \`IN_COMMUNITY\``; `FETCH PROP` for props; `FIND SHORTEST PATH` for paths; labels via the Entity `label` column; rel type via `RELATED.rel_type`; weight via `RELATED.weight`.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** neo4j issues the SAME Cypher + params. Nebula only under `GRAPH_BACKEND=nebula`.
- Local commits only (**no push until FULL migration**). Never stage `docs/bruno/collection.bru`. Unit tests DB-free. Nebula inline nGQL (no param_map); `_q`/`entity_vid`.
- Chunk-dependent `cooccurrence` → `[]` under nebula (deferred). `run_rows` fail-soft is UNCHANGED (the seam replaces the raw-Cypher call inside each primitive, not the fail-soft wrapper).
- Row-shape parity: each nebula method returns rows with the SAME keys the neo4j Cypher `RETURN`s (the primitive's downstream mapping is unchanged).

## Design

### 1. Seam: `AnalyticsGraphOps` (`src/graph/analytics_graph_ops.py`, new — the pattern for all Tier-A)
A Protocol with one method PER connections read (this file GROWS as later primitives are ported, OR later primitives get sibling seams — decide during connections, but keep it a clean per-read method surface). For connections:
```python
class AnalyticsGraphOps(Protocol):
    def entity_core(self, name) -> list[dict]: ...
    def entity_neighbors(self, name, top_n) -> list[dict]: ...
    def entity_identifiers(self, name, id_types) -> list[dict]: ...
    def entity_communities(self, name) -> list[dict]: ...
    def neighbors_by_relation(self, name, rel, top_n) -> list[dict]: ...
    def common_connections(self, a, b, top_n) -> list[dict]: ...
    def identifier_lookup(self, value) -> list[dict]: ...
    def shared_identifier_entities(self, id_types, top_n) -> list[dict]: ...
    def connection_path(self, source, target, hops) -> list[dict]: ...
    def cooccurrence(self, name, top_n) -> list[dict]: ...  # nebula -> []
```
- **`Neo4jAnalyticsGraphOps(store)`** — each method runs the corresponding current Cypher verbatim (the constants/inline strings move here) with the same params + returns `run_rows`-style rows (use `store.structured_query`... but keep fail-soft where the primitive had it — actually the primitive's `run_rows` already wraps; the seam method just issues the query and returns rows, and the primitive keeps calling through `run_rows`? NO — cleaner: the primitive calls `build_analytics_graph_ops(store).<method>(...)`, and the seam method internally uses the same fail-soft pattern as `run_rows` OR the primitive wraps. KEEP the existing `run_rows` fail-soft: the seam's Neo4j method returns the raw rows via `run_rows`; the Nebula method returns nGQL rows (also fail-soft). Simplest: each seam method wraps its query in the same try/except→[] as `run_rows`.)
- **`NebulaAnalyticsGraphOps(store)`** — nGQL per method (GO/FETCH/FIND PATH); `cooccurrence` → `[]`.
- `build_analytics_graph_ops(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 2. Integration (`connections.py`)
Each primitive replaces its `run_rows(store, <cypher>, params)` with `build_analytics_graph_ops(store).<method>(...)`. Move the Cypher constants into the Neo4j impl. Downstream row-mapping in each primitive unchanged.

### 3. Nebula nGQL sketch (per read)
- `entity_core(name)`: `FETCH PROP ON \`Entity\` "<vid>" YIELD name, label, description, ...` → the `_CORE` RETURN keys.
- `entity_neighbors(name, top_n)`: `GO FROM "<vid>" OVER \`RELATED\` BIDIRECT YIELD dst(edge)/src(edge), rel_type, weight` → FETCH neighbour names/labels → assemble the `_NEIGHBORS` row keys; ORDER/top_n in Python.
- `entity_communities(name)`: `GO FROM "<vid>" OVER \`IN_COMMUNITY\` YIELD dst(edge)` → FETCH Community level/title.
- `common_connections(a,b,top_n)`: neighbours(a) ∩ neighbours(b) via two GOs + Python intersection.
- `connection_path(source,target,hops)`: `FIND SHORTEST PATH FROM "<va>" TO "<vb>" OVER * UPTO <hops> STEPS YIELD path AS p;` → map to the primitive's expected shape.
- `cooccurrence` → `[]`.
(The exact YIELD/row-key mapping is the implementer's job, matched to each Cypher's RETURN — verified by tests + a live gate.)

### 4. Tests (DB-free) + live gate
- Neo4j impl: fake store asserts each moved Cypher + params (byte-for-byte).
- Nebula impl: fake store asserts the nGQL shape per read (GO/FETCH/FIND PATH, `_q`/`entity_vid`); `cooccurrence` → `[]`; row keys match the neo4j RETURN.
- Dispatch + integration (connections primitives route through a fake ops; neo4j default unchanged; `run_rows` fail-soft preserved).
- Live gate: seed a small entity neighbourhood; `entity_dossier`/`neighbors_by_relation`/`common_connections`/`connection_path` return sane rows under nebula; `cooccurrence` → [].

## Out of scope (deferred)
- The other Tier-A primitives (aggregations/quality/signals/domain/events/dynamics/rollups/alerts) — sibling slices following this pattern.
- Tier B (GDS: centrality/communities/materialize) — GraphScope/distributed.
- `cooccurrence` under nebula (Chunk-dependent → []).

## Interfaces produced
- `src/graph/analytics_graph_ops.py`: `AnalyticsGraphOps` + Neo4j/Nebula impls + `build_analytics_graph_ops`.
- `src/analytics/primitives/connections.py`: routes through the seam.

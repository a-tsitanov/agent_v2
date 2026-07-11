# Nebula graph-compute read (extract_entity_edges + RELATED weight) — end-to-end community-detection under nebula

**Status:** approved (autonomous, delegated) 2026-07-11. NebulaGraph migration scale-blocker. Makes `detect_communities` (leidenalg) run FULLY under nebula: read the entity graph via nGQL + write `:Community` via the merged BUILD seam. Branch `feat/nebula-graph-compute-read` off `main` (base `69119be`).

## Goal

Under `GRAPH_BACKEND=nebula`, `extract_entity_edges(store)` reads the `Entity`/`RELATED` graph via nGQL (today it issues raw Cypher through `store.structured_query(param_map=...)`, which nebula RAISES on). Plus: give `RELATED` a `weight` column and write it (`mention_count`-derived), so nebula Leiden is **weighted** like neo4j (today nebula edges have no weight → unweighted, a quality divergence). Result: leidenalg community-detection runs end-to-end under nebula (read → Leiden in Python → write via the merged `CommunityWriteback`), live-verifiable.

## Background (grounded)

- `community_leiden.py::extract_entity_edges(store)` streams `(edges, names)`: keyset-paginated `_NODES_CYPHER` (`MATCH (e:__Entity__) WHERE $after='' OR e.name > $after RETURN e.name ORDER BY e.name LIMIT $limit`) + `_EDGES_CYPHER` (`MATCH (s)-[r]->(t) ... RETURN s.name AS src, t.name AS tgt, coalesce(r.weight, 1.0) AS weight, elementId(r) AS cursor ORDER BY elementId(r) LIMIT $limit`). Both via `store.structured_query(cypher, param_map={"after","limit"})`. Nebula's `structured_query` RAISES on non-empty `param_map` (Phase 2) → nebula extract fails today.
- Relations carry `properties["weight"] = float(mention_count)` (`merge.py:317`). Nebula `RELATED` schema (`nebula_schema.py:42`) has `(rel_type, polarity, valid_from, valid_to)` — NO weight; `upsert_relations` (`nebula_store.py`) doesn't write it. So nebula edges lose weight.
- `Entity` VID = `entity_vid(name)`; nebula `RELATED` default GO direction is outgoing (each edge has one src → counted once). `entity_name_idx` on `Entity(name)` backs LOOKUP.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** `extract_entity_edges` on neo4j issues the SAME two Cypher queries + params. Nebula reached only under `GRAPH_BACKEND=nebula`. `upsert_relations` on neo4j is untouched (weight already flows via the relation props); the nebula write gains a weight column.
- Local commits only (no push — migration policy: no push until full migration). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store recording statements).
- Nebula inline nGQL (no param_map); `_q` quoting; VID via `entity_vid`.
- Weight parity: nebula edge weight = `float(props.get("weight", 1.0) or 1.0)` (mirrors neo4j `coalesce(r.weight, 1.0)`).
- The nebula edge export is a WORKING full extract (node LOOKUP + batched GO), NOT the billion-scale direct-read (that stays deferred — the known full-scan concern). Correctness first.

## Design

### 1. `RELATED` weight (schema + write)
- `nebula_schema.py`: `CREATE EDGE IF NOT EXISTS \`RELATED\` (... , weight double DEFAULT 1.0)` (fresh spaces get it). Add a best-effort schema-evolution step in `ensure_schema` for EXISTING spaces: `ALTER EDGE \`RELATED\` ADD (weight double DEFAULT 1.0);` run fail-open (already-present → harmless error). Sequence it after the SCHEMA_DDL loop, before/with the probe.
- `nebula_store.py::upsert_relations`: add `weight` to the INSERT columns + per-row value `float(props.get("weight", 1.0) or 1.0)`: `INSERT EDGE \`RELATED\` (rel_type, polarity, valid_from, valid_to, weight) VALUES <src> -> <tgt>:(<rel_type>, <polarity>, <valid_from>, <valid_to>, <weight>)`. Keep the batching (multi-VALUES) from the merged slice.

### 2. Seam: `GraphEdgeExport` (`src/graph/graph_edge_export.py`, new)
```python
class GraphEdgeExport(Protocol):
    def stream_names(self, *, batch_size: int) -> list[str]: ...
    def stream_edges(self, *, batch_size: int) -> list[tuple[str, str, float]]: ...
```
- **`Neo4jGraphEdgeExport(store)`** — the CURRENT keyset logic (moved verbatim: `_NODES_CYPHER`/`_EDGES_CYPHER` + the two pagination loops from `extract_entity_edges`). Byte-for-byte behaviour.
- **`NebulaGraphEdgeExport(store)`**:
  - `stream_names`: keyset LOOKUP — `after=""`; loop `LOOKUP ON \`Entity\` WHERE \`Entity\`.name > "<after>" YIELD \`Entity\`.name AS name | ORDER BY $-.name ASC | LIMIT <batch>;`, collect names, advance `after = last name`, stop when a page < batch or empty. (`name > ""` matches all on the first page.)
  - `stream_edges`: build `vid2name = {entity_vid(n): n for n in stream_names(...)}` (names already streamed). Then iterate the VIDs in chunks of `batch_size`, `GO FROM <_q-quoted chunk> OVER \`RELATED\` YIELD src(edge) AS s, dst(edge) AS d, \`RELATED\`.weight AS w;`, and for each row append `(vid2name[s], vid2name[d], float(w or 1.0))` when both endpoints are known (skip dangling). GO outgoing counts each edge once.
- `build_graph_edge_export(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 3. `extract_entity_edges` rewire (`community_leiden.py`)
`extract_entity_edges(store, *, batch_size=50_000)` → `exp = build_graph_edge_export(store); names = exp.stream_names(batch_size=batch_size); edges = exp.stream_edges(batch_size=batch_size); return edges, names`. The two Cypher constants + pagination loops MOVE into `Neo4jGraphEdgeExport` (their new home). The `logger.info(... streamed ...)` line stays in `extract_entity_edges`.

### 4. Tests (DB-free)
- Neo4j export: fake store records `(cypher, param_map)`; assert `_NODES_CYPHER`/`_EDGES_CYPHER` issued with keyset params across pages (multi-page via canned pages); result `(edges, names)` identical to today (byte-for-byte guard for the default path).
- Nebula export: fake store returns canned rows per substring; assert `stream_names` keyset LOOKUP advances `after` and dedups pages; `stream_edges` GO over RELATED, maps vids→names via `entity_vid`, drops dangling endpoints, reads `weight`. Inline (no param_map).
- `upsert_relations` writes the `weight` column (batched) with `mention_count`-derived value; RELATED schema DDL has `weight double`.
- Dispatch + `extract_entity_edges` routes through the seam.

### 5. Manual gate (live-verify, controller-run)
On the running cluster, `GRAPH_BACKEND=nebula`: seed a few entities + weighted RELATED edges (via `upsert_nodes`/`upsert_relations`), then run `detect_communities` (community_backend=leidenalg) END-TO-END → confirm it reads the edges (extract), computes Leiden, and materialises `:Community`/`IN_COMMUNITY` via the BUILD seam. Verify weights flowed (edge `weight` present).

## Out of scope (deferred)

- **Billion-scale direct-read** into the compute layer (GraphScope connector / offline dump) — the GO-based full extract is correct but streams through the query layer (the known bottleneck). Separate effort.
- Distributed centralities (GraphScope) — next scale-blocker after this.
- Backfilling `weight` onto RELATED edges written before this slice (they default to 1.0 via the column default).

## Interfaces produced

- `src/graph/nebula_schema.py`: `RELATED` gains `weight double`; `ensure_schema` best-effort `ALTER EDGE`.
- `src/graph/nebula_store.py`: `upsert_relations` writes `weight`.
- `src/graph/graph_edge_export.py`: `GraphEdgeExport`, `Neo4jGraphEdgeExport`, `NebulaGraphEdgeExport`, `build_graph_edge_export`.
- `src/graph/community_leiden.py`: `extract_entity_edges` routes through the seam.

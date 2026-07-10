# Phase 2 (vertical slice) — Nebula read-path design

**Status:** approved 2026-07-10. Sub-project of the NebulaGraph migration (`docs/superpowers/plans/2026-07-09-nebulagraph-migration.md`, Phase 2).

## Goal

With `GRAPH_BACKEND=nebula`, one real lexical graph-search flow answers **end-to-end** against NebulaGraph with output parity to Neo4j, verified on the live cluster: (a) find a seed entity by name, (b) walk its bounded N-hop `RELATED` neighborhood, returning the same Python shape the Neo4j path returns so downstream code is unchanged.

This is a **vertical slice**, not the full read-surface translation. It proves "Nebula answers a query" and is benchmarkable; the remaining read sites are later Phase-2 expansion.

## Global Constraints

- Default `GRAPH_BACKEND` stays `neo4j`; the Neo4j read path is untouched and remains the behavior for the default backend. nGQL is reached only when `backend == "nebula"`.
- Preserve the downstream contract: `awalk`/`afind_entities_by_name` return the SAME Python structures (`RoundGraphData` / list of `{name, label, description}` dicts) regardless of backend.
- Unit tests DB-free (fake session asserting generated nGQL + row→dict mapping). Live parity verified on the running cluster (`docker compose --profile nebula`).
- Local commits only (no push). Do not stage the unrelated `docs/bruno/collection.bru`.
- User input (entity name) interpolated into nGQL MUST be escaped via `_q` (nGQL has no param binding on `NebulaGraphStore.structured_query` yet); queries are issued with no `param_map`.
- Embedding/vector retrieve is OUT (Phase 3). This slice is lexical-entry + structural walk only.

## In scope

1. **Data model: generic `RELATED` + `rel_type` property.**
   - `nebula_schema.py`: add `rel_type string DEFAULT ''` to the `RELATED` edge DDL.
   - `nebula_store.py::upsert_relations`: entity–entity relations always write edge type `RELATED`, with the original relation label stored in the `rel_type` **property** (quoted via `_q` — a value, not an identifier, so no edge-label injection risk). Structural edges (`MENTIONS`/`IN_COMMUNITY`/`PARENT_OF`) keep their own types. `_safe_edge_label` is retained only where a caller still supplies a structural type; the entity–entity path no longer splices a caller label into the edge identifier.
   - Consequence: the fixture must be re-written to re-verify (the parity harness already does this).

2. **find-by-name → nGQL `LOOKUP`.**
   - New `_FIND_BY_NAME_NGQL`: `LOOKUP ON \`Entity\` WHERE \`Entity\`.name == "<escaped-name>" YIELD properties(vertex) AS p` (exact; prefix later). Maps to `[{name, label, description}]`.
   - Known parity gap vs Neo4j `db.index.fulltext.queryNodes` (Lucene tokenization / OR-of-terms): documented, accepted for the slice. Full-text via ES is a later enhancement.

3. **bounded walk → nGQL `GET SUBGRAPH`.**
   - New `_WALK_NGQL`: VID is deterministic (`entity_vid(name)`), so no pre-lookup — `GET SUBGRAPH WITH PROP <hops> STEPS FROM "<vid>" OVER \`RELATED\` BOTH YIELD VERTICES AS nodes, EDGES AS rels`.
   - Mapper builds a `vid → name` map from the returned VERTICES, then emits `RoundGraphData`: entities `{name, label, description}`; relations `{src, tgt, label: <rel_type prop>, polarity, valid_from, valid_to}` (src/tgt resolved from edge src/dst VIDs via the vid→name map). Node/edge caps applied in post-processing. `rel_filter` filters on the `rel_type` property (was `type(rel)`). polarity/temporal filtering unchanged.
   - `apoc.coll.flatten` disappears (GET SUBGRAPH returns the edge set directly).
   - **Live-verify risk:** the exact `GET SUBGRAPH ... YIELD VERTICES/EDGES` result shape (per-step lists vs aggregated) must be confirmed against the running cluster; the mapper is the fiddly part.

4. **Backend dispatch.**
   - `retriever.py` `awalk` / `afind_entities_by_name` branch on `settings.graph.backend`: `neo4j` → existing `_CYPHER` via `structured_query`; `nebula` → new `_NGQL` (name/vid interpolated + `_q`-escaped, issued with no `param_map`), plus a small mapper turning nGQL rows into the same dicts the Neo4j path returns. Downstream (`aretrieve` orchestration, callers) is unchanged.

## Verification

- **Live parity:** extend `tests/eval/migration/parity_write.py` (or a sibling `parity_read.py`) to, after writing the fixture, run find-by-name + walk on both backends and compare normalized dicts (Neo4j baseline; if Neo4j is down, assert nebula results match the expected fixture structure). Run on the live cluster.
- **Unit (DB-free):** fake session asserts the generated `LOOKUP` / `GET SUBGRAPH` nGQL (name escaped, vid computed, hops/caps applied) and the row→dict mapping produces the correct `RoundGraphData` from canned rows.

## Out of scope (deferred)

- Vector + synonym retrieve (`pg_index.as_retriever`, `er_vec`) → Phase 3 (Milvus).
- Full Lucene full-text (Elasticsearch mixed index) → later.
- Community / global-search reads (`workflow/search/activities/*`), analytics primitives (`analytics/primitives/*`), `graph/analysis.py`, admin reads → later Phase-2 expansion.
- The remaining ~60 read call-sites.

## Known gaps to close before enabling `GRAPH_BACKEND=nebula` under load

Surfaced by the whole-branch review; none blocks a neo4j-default merge, but each must be closed or accepted before nebula reads are turned on in any workload:

- **`GET SUBGRAPH` has no server-side cap.** The neo4j walk enforces `LIMIT $node_cap` in-query; the nebula path fetches the whole N-hop subgraph into Python and caps only in `_map_walk_rows`. On a hub node at hops=3 this is an unbounded fetch/memory risk. Bound it (or accept it) as part of the parity/p95 benchmark gate.
- **`rel_filter` is applied *after* the cap under nebula** (`retriever.py` awalk nebula branch). Neo4j filters relation types before `$edge_cap`; the nebula branch caps all types first, then filters — so on a dense >cap neighborhood a rare filtered type can be truncated away, diverging from neo4j. Push the filter into `subgraph()` / before the cap when productionizing the walk.
- **Schema-evolution hazard:** `CREATE EDGE IF NOT EXISTS \`RELATED\`` does NOT add the `rel_type` column to a *pre-existing* nebula space's `RELATED` edge, so `upsert_relations` (which now writes `rel_type`) would fail there. Fine for a fresh space (the live gate drops it); for a real data migration, `ALTER EDGE RELATED ADD (rel_type string)` first. Also note: `LOOKUP` needs `REBUILD TAG INDEX entity_name_idx` to cover data written before the index existed.

## Interfaces produced

- `nebula_schema.py`: `RELATED` DDL gains `rel_type string DEFAULT ''`.
- `nebula_store.py`: `upsert_relations` writes `RELATED` + `rel_type`; (mapper helpers as needed for reads may live here or in `retriever.py`).
- `retriever.py`: `_FIND_BY_NAME_NGQL`, `_WALK_NGQL` constants + backend branch in `awalk`/`afind_entities_by_name` + row→dict mappers. Public method signatures and return types unchanged.

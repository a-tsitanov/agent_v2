# Nebula entity-resolution graph ops — verdict cache + edge-redirect merge + canonical stamp

**Status:** proposed (autonomous, delegated — user chose ER) 2026-07-11. NebulaGraph migration. Closes the CRITICAL ingest-side ER gap so entity dedup/merge works under nebula. Branch `feat/nebula-entity-resolution` off `main` (base `0f4bcf0`).

## Goal

Under `GRAPH_BACKEND=nebula`, entity resolution's GRAPH operations run via nGQL: the `ERVerdict` decision cache, the `er_canonical_name` canonical stamp, and the entity-merge (loser→canonical edge-redirect + delete). Without these, nebula ingest accumulates duplicate entities (ER's vector-candidate half already runs on Milvus from the er_vec slice, but the graph half raises/no-ops under nebula). Neo4j path byte-for-byte unchanged.

## Background (grounded; scope narrowed by investigation)

ER (`entity_resolution.py`) under nebula: the candidate kNN already routes through the Milvus `EntityVectorStore` (`_load_candidates_via_store`, er_vec slice) when `use_native_vector_knn` + `vector_store` are set (the nebula config). So the node-window read `_load_existing_canonicals` (`:1144`, `MATCH __Entity__ ... ORDER BY mention_count`) and the native `db.index.vector.queryNodes` (`:1245`) + `er_embedding_vec` index are **neo4j-only, bypassed under nebula — NOT gaps**.

The remaining raw-Cypher ER graph ops that RAISE / silently-drop under nebula:
- **Verdict cache** (`:ERVerdict {key, same}`): read (`:830` `MATCH (v:ERVerdict) WHERE v.key IN $keys`), constraint (`:853`), write (`:857` `UNWIND $rows MERGE (v:ERVerdict {key}) SET v.same, v.updated`). Lets ER skip re-judging a pair.
- **Canonical stamp**: `er_canonical_name` is set as an Entity property (`:1596` `ent.properties.setdefault("er_canonical_name", ent.name)`) and written via `upsert_nodes` — but the nebula `Entity` TAG has no `er_canonical_name` column, so `upsert_nodes` drops it.
- **Entity-merge / edge-redirect** (`_cleanup_stored_losers`, `:1103`): `apoc.merge.relationship` copies each loser's out-edges (`loser→t` ⇒ `canon→t`) and in-edges (`s→loser` ⇒ `s→canon`), then `DETACH DELETE loser`. **Fail-open safety (must preserve): on ANY error the loser is LEFT INTACT with its edges (a recoverable duplicate) — never delete-without-repointing.**

Nebula facts: `Entity` VID = `entity_vid(name)`; `RELATED` edge key = `(src_vid, dst_vid, rank=0)` with props `(rel_type, polarity, valid_from, valid_to, weight)`; GO default = outgoing, `REVERSELY` = incoming.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, every ER graph op issues the IDENTICAL Cypher/APOC + params. Nebula reached only under `GRAPH_BACKEND=nebula`.
- Local commits only (**no push — migration policy: no push until FULL migration**). Never stage `docs/bruno/collection.bru`. Unit tests DB-free.
- Nebula inline nGQL (no param_map); `_q` quoting; VID via `entity_vid`.
- Mirror the merged seam pattern (`community_writeback`/`graph_edge_export`). Preserve ER's fail-open safety semantics exactly.

## Design

### 1. Schema (`nebula_schema.py`)
- `Entity` TAG gains `er_canonical_name string DEFAULT ''` (CREATE for fresh + best-effort `ALTER TAG \`Entity\` ADD (er_canonical_name string DEFAULT '')` for existing + extend the Entity write-readiness probe to cover the new column — same propagation-lag class already handled for RELATED.weight).
- New `ERVerdict` TAG: `(er_key string, same bool DEFAULT false, updated int DEFAULT 0)` + `CREATE TAG INDEX \`er_verdict_key_idx\` ON \`ERVerdict\`(er_key(256))` (backs LOOKUP by key). VID = `blake2b(key)` (a dedicated `verdict_vid(key)`).

### 2. `upsert_nodes` writes `er_canonical_name`
`nebula_store.py::upsert_nodes` adds the column: `INSERT VERTEX \`Entity\` (name, description, mention_count, created_at, label, er_canonical_name) VALUES ...:(..., <_q(props.get("er_canonical_name",""))>)`. Keep batching. (Neo4j path untouched — it writes the property via the PGStore already.)

### 3. Seam: `ERGraphOps` (`src/graph/er_graph_ops.py`, new)
```python
class ERGraphOps(Protocol):
    def ensure_verdict_schema(self) -> None: ...
    def load_verdicts(self, keys: list[str]) -> dict[str, bool]: ...
    def store_verdicts(self, entries: dict[str, bool]) -> None: ...
    def merge_loser_into_canonical(self, *, loser: str, canon: str) -> None: ...
```
- **`Neo4jERGraphOps(store)`** — runs the existing Cypher/APOC verbatim (the 3 constants + the merge Cypher moved here). Byte-for-byte.
- **`NebulaERGraphOps(store)`**:
  - `ensure_verdict_schema`: no-op (ERVerdict TAG+index created by `nebula_schema.ensure_schema`).
  - `load_verdicts(keys)`: for each key `verdict_vid(key)`; `LOOKUP ON \`ERVerdict\` WHERE \`ERVerdict\`.er_key IN [<_q keys>] YIELD ... ` OR (nebula IN on index) fetch by vids: `FETCH PROP ON \`ERVerdict\` <vids> YIELD \`ERVerdict\`.er_key AS key, \`ERVerdict\`.same AS same` → `{key: bool(same)}`.
  - `store_verdicts(entries)`: batched `INSERT VERTEX \`ERVerdict\` (er_key, same, updated) VALUES verdict_vid(k):(<k>, <same>, <now_ms>), ...` (upsert-by-VID = the MERGE-on-key idempotency).
  - `merge_loser_into_canonical(loser, canon)`: **the hard one, safety-preserving order**:
    1. `lv, cv = entity_vid(loser), entity_vid(canon)`; if `lv == cv` return.
    2. Out-edges: `GO FROM "<lv>" OVER \`RELATED\` YIELD dst(edge) AS t, \`RELATED\`.rel_type AS rt, ...props...` → for each `t != cv`, `INSERT EDGE \`RELATED\` (...) VALUES "<cv>" -> "<t>":(...props...)`.
    3. In-edges: `GO FROM "<lv>" OVER \`RELATED\` REVERSELY YIELD src(edge) AS s, ...props...` → for each `s != cv`, `INSERT EDGE \`RELATED\` (...) VALUES "<s>" -> "<cv>":(...props...)`.
    4. ONLY after 2+3 succeed: `DELETE VERTEX "<lv>" WITH EDGE;`.
    - If ANY nGQL step raises, do NOT delete the loser (propagate/log so the caller's existing try/except leaves the loser intact) — preserves the "never drop edges" safety. (Re-inserts are idempotent upserts, so a retry re-copies harmlessly.)
- `build_er_graph_ops(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 4. Integration (`entity_resolution.py`)
`_load_verdict_cache` / `_store_verdicts` route through `build_er_graph_ops(store).load_verdicts/store_verdicts` (the 3 verdict Cypher constants move into `Neo4jERGraphOps`). `_cleanup_stored_losers`' inner merge Cypher moves into `Neo4jERGraphOps.merge_loser_into_canonical`; the loop calls `ops.merge_loser_into_canonical(loser=, canon=)` inside the SAME try/except (fail-open leave-intact). The Milvus candidate path + all ER decision logic are UNTOUCHED.

### 5. Tests (DB-free)
- Neo4j impl: fake store records `(cypher, param_map)`; assert the 3 verdict constants + the merge Cypher (incl `apoc.merge.relationship`, `DETACH DELETE loser`) issued verbatim with exact params.
- Nebula impl: fake store records nGQL; verdict LOOKUP/FETCH + batched INSERT (verdict_vid, upsert); merge = out-GO + in-GO(REVERSELY) + re-INSERTs on canon vid with props + `DELETE VERTEX ... WITH EDGE` LAST; assert delete NOT issued if a re-insert raises (fail-open safety); self-edge (lv==cv) → no-op.
- `upsert_nodes` writes `er_canonical_name`; Entity DDL has the column; ERVerdict TAG+index in SCHEMA_DDL; ensure_schema probes the new Entity column.
- Integration: `_store_verdicts`/`_load_verdict_cache`/`_cleanup_stored_losers` route through a fake ops; neo4j default unchanged.

### 6. Manual gate (live-verify)
On the cluster, `GRAPH_BACKEND=nebula`: seed canon+loser entities with edges; `merge_loser_into_canonical` → verify canon gains the loser's edges (both directions, props incl weight) and loser is deleted; verdict store→load round-trips; `er_canonical_name` persists on an upserted entity.

## Out of scope (deferred)

- The node-window `_load_existing_canonicals` + native vector index (neo4j-only; nebula uses Milvus candidates).
- Edge-property MERGE semantics divergence: neo4j `apoc.merge.relationship` merges by (type, endpoints); nebula RELATED collapses to one edge per (src,dst,rank=0) so a re-inserted edge overwrites (last-wins). Equivalent for dedup; record for the parity gate.
- Decoupling Milvus ER-vector upsert from `use_native_vector_knn` (er_vec deferred item).

## Interfaces produced

- `src/graph/nebula_schema.py`: `Entity.er_canonical_name`; `ERVerdict` TAG + index; Entity-column probe.
- `src/graph/nebula_store.py`: `upsert_nodes` writes `er_canonical_name`.
- `src/graph/er_graph_ops.py`: `ERGraphOps`, `Neo4jERGraphOps`, `NebulaERGraphOps`, `build_er_graph_ops`, `verdict_vid`.
- `src/graph/entity_resolution.py`: verdict cache + merge route through the seam.

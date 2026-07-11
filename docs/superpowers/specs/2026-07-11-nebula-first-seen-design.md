# Nebula entity first-seen — created_at/first_doc_id preservation + stamp

**Status:** proposed (autonomous, delegated — user chose the upsert-preserve rework) 2026-07-11. NebulaGraph migration. Makes ENTITY first-seen provenance (`created_at`, `first_doc_id`) correct under nebula (first-write-wins). Branch `feat/nebula-first-seen` off `main` (base `75dec13`).

## Goal

Under `GRAPH_BACKEND=nebula`, an entity's `created_at`/`first_doc_id` are stamped ONCE (first ingest) and PRESERVED on re-mention — matching neo4j's ON-CREATE semantics. Today they're broken under nebula: `upsert_nodes` uses `INSERT VERTEX` which (empirically confirmed on the cluster) OVERWRITES/RESETS all listed-or-omitted columns on re-upsert, so `created_at` resets every ingest, and `stamp_first_seen`'s raw Cypher raises under nebula. **Scope: ENTITIES.** Relationship first-seen (`RELATED.created_at`/`first_doc_id` + `upsert_relations` preserve) is a documented follow-up (edges need the same rework + columns; harder to batch-read).

## Background (grounded; empirically verified)

- `first_seen.py::stamp_first_seen(store, entity_names, relations, ingest_epoch, doc_id)` runs `_STAMP_ENTITIES` (`UNWIND $names ... MATCH (e:__Entity__ {name}) WHERE e.created_at IS NULL SET e.created_at=$ts, e.first_doc_id=$doc_id`) + `_STAMP_RELS` (rel variant), via `store.structured_query(cypher, param_map=...)` — RAISES under nebula. Fail-open (try/except → warning). Called post-upsert in `build_property_graph.py:149`.
- **Cluster-verified nebula semantics:** `INSERT VERTEX` (full OR partial column list) RESETS omitted/all columns to default — does NOT preserve. `UPDATE VERTEX ... SET` PRESERVES unlisted columns. So the only way to keep `created_at` across upserts is to NOT reset it — via `UPDATE VERTEX`, or by reading it back and re-writing the preserved value in the `INSERT`.
- Nebula `Entity` TAG has `created_at int DEFAULT 0` but NO `first_doc_id`. `upsert_nodes` (batched INSERT) writes `created_at` from props each time.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** `stamp_first_seen` on neo4j issues the SAME `_STAMP_ENTITIES`/`_STAMP_RELS` + params; `upsert_nodes` on neo4j untouched. Nebula reached only under `GRAPH_BACKEND=nebula`.
- Local commits only (**no push until FULL migration**). Never stage `docs/bruno/collection.bru`. Unit tests DB-free. Nebula inline nGQL (no param_map); `_q`/`entity_vid`.
- `upsert_nodes` stays fail-open (writes never crash ingest): the read-back is best-effort (on FETCH failure → no preservation, treat as new, ingest continues).
- Preserve-columns are ONLY `created_at` + `first_doc_id` (first-write-wins). All other Entity columns overwrite (latest-wins), as today.

## Design

### 1. Schema (`nebula_schema.py`)
`Entity` TAG gains `first_doc_id string DEFAULT ''` (CREATE for fresh + best-effort `ALTER TAG \`Entity\` ADD (first_doc_id string DEFAULT '')` for existing + extend the Entity write-readiness probe's sentinel INSERT to include `first_doc_id`).

### 2. `upsert_nodes` preserve-rework (`nebula_store.py`) — the core
Per chunk (keep `write_batch_size` batching):
1. Compute `by_vid = {entity_vid(n.name): n for n in chunk}` (unique — deduped upstream).
2. **Read-back (best-effort):** `FETCH PROP ON \`Entity\` <_q vids> YIELD id(vertex) AS vid, \`Entity\`.created_at AS ca, \`Entity\`.first_doc_id AS fdi;` → `preserved = {vid: (ca, fdi)}` for EXISTING vids. Wrap in try/except → `{}` on failure (fail-open; no preservation → new-entity path). Use the store's raising query for the read but catch it here.
3. For each node: if `vid in preserved` → use its `(ca, fdi)`; else → `ca = int(props.get("created_at",0) or 0)`, `fdi = _q(props.get("first_doc_id","") or "")`.
4. Batched `INSERT VERTEX \`Entity\` (name, description, mention_count, created_at, label, er_canonical_name, first_doc_id) VALUES ...` with the preserved-or-new `created_at`/`first_doc_id` and the fresh other columns. `self._exec` (fail-open) for the INSERT.
Net: existing entities keep their original `created_at`/`first_doc_id` (read-back + re-write); new entities get props' values; all other columns overwrite. Cost: +1 FETCH per batch (bounded), batched INSERT preserved. Neo4j store untouched.

### 3. Seam: `FirstSeenStamp` (`src/graph/first_seen.py`, extend in-place)
`stamp_first_seen` dispatches on `settings.graph.backend`:
- **neo4j:** the existing `_STAMP_ENTITIES` + `_STAMP_RELS` via `structured_query(param_map=...)` — verbatim, byte-for-byte.
- **nebula:** ENTITIES — for each name in `entity_names`, `vid=entity_vid(name)`; read `created_at` (FETCH batch) and for vids with `created_at == 0` (new-this-pass) issue `UPDATE VERTEX ON \`Entity\` "<vid>" SET created_at = <ts>, first_doc_id = <_q(doc_id)>;` (per-vid UPDATE — bounded to new entities; UPDATE preserves other cols). RELATIONS — **no-op under nebula** (RELATED lacks `created_at`/`first_doc_id`; deferred follow-up; log once at debug). Keep the outer fail-open try/except.
Keep `stamp_first_seen`'s signature + fail-open contract identical.

### 4. Integration
`build_property_graph.py` call site is UNCHANGED (it already calls `stamp_first_seen(store, ...)`); the dispatch is internal. `upsert_nodes` change is transparent to `build_property_graph`.

### 5. Tests (DB-free)
- Schema: `Entity` DDL has `first_doc_id string`; ensure_schema issues `ALTER TAG \`Entity\` ADD (first_doc_id`; probe INSERT includes `first_doc_id`.
- `upsert_nodes` preserve: fake store returns a read-back with an EXISTING vid (ca=111, fdi="d0") → assert the INSERT for that vid carries `111`/`"d0"` (preserved), while a NEW vid uses props' (or 0/""); read-back FETCH failure → all treated as new (fail-open). Batching intact.
- `stamp_first_seen` nebula: fake store; a name whose `created_at==0` → an `UPDATE VERTEX ON \`Entity\` "<vid>" SET created_at = <ts>, first_doc_id =` issued; a name with `created_at>0` → NOT updated; relations → no UPDATE EDGE issued (no-op). neo4j backend → the two verbatim Cypher + params (byte-for-byte guard). Fail-open on error.

### 6. Manual gate (live-verify)
On the cluster, `GRAPH_BACKEND=nebula`: upsert an entity (ingest 1) + stamp → `created_at=<day1>`, `first_doc_id=<doc1>`; upsert the SAME entity again with a new description (ingest 2, different day) + stamp → description updated BUT `created_at=<day1>`/`first_doc_id=<doc1>` PRESERVED (first-write-wins). Controller-run.

## Out of scope (deferred)

- **Relationship first-seen** (`RELATED.created_at`/`first_doc_id` columns + `upsert_relations` preserve-rework + `_STAMP_RELS` nGQL) — same rework for edges, batch-reading edge props is harder; separate slice. Under nebula, rel first-seen is a no-op until then.
- Backfilling `first_doc_id` onto entities written before this slice (default `''`).

## Interfaces produced

- `src/graph/nebula_schema.py`: `Entity.first_doc_id` + ALTER + probe.
- `src/graph/nebula_store.py`: `upsert_nodes` read-back-preserve of `created_at`/`first_doc_id`.
- `src/graph/first_seen.py`: backend-dispatched `stamp_first_seen` (nebula entity UPDATE; rel no-op).

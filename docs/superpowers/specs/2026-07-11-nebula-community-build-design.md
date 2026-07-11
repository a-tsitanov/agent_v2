# Nebula community-BUILD (nGQL) — full BUILD stage, backend-dispatched

**Status:** approved 2026-07-11. Sub-project of the NebulaGraph migration (variant B, taken because the project decision is to make **nebula the default backend**). This is the **BUILD** stage of the community lifecycle on nebula; SUMMARIZE and READ are sibling slices. Branch `feat/nebula-community-build` off `main` (base `e66fbd9`).

## Goal

Under `GRAPH_BACKEND=nebula`, community detection **materialises** `:Community` vertices + `IN_COMMUNITY` / `PARENT_OF` edges into nebula (nGQL), instead of fail-opening on raw Cypher as today. Covers the **full BUILD stage** — both single-level (`detect_communities`) and hierarchy (`detect_hierarchy`, `PARENT_OF`) — behind a `CommunityWriteback` seam. The neo4j path is **byte-for-byte unchanged** (default). SUMMARIZE (report write) and community READ stay neo4j-only for now (sibling slices).

## Background (current state, grounded)

The community write-back is **raw Cypher run through `_run_query(store, cypher, params)`**, so under nebula it fail-opens (the try/except at `communities.py:519-534` warns; communities never materialise). All BUILD-stage Cypher:

- **`src/graph/communities.py`**:
  - `_MERGE_COMMUNITY_CYPHER` (`:178`) — `MERGE (c:Community {id,level})` + SET member_count/members_hash/updated + FOREACH-carry report fields + clear stale `IN_COMMUNITY` + UNWIND members `MERGE (e)-[:IN_COMMUNITY]->(c)`.
  - `_MERGE_SUBCOMMUNITY_CYPHER` (`:203`) — same + `MATCH (p:Community {id:$parent_id, level:$level-1}) MERGE (p)-[:PARENT_OF]->(c)` (parent MATCHed, not merged — caller writes coarsest-first, ordering-dependent by design).
  - `_PRUNE_LEVEL_CYPHER` (`:143`) — `MATCH (c:Community {level:$level}) DETACH DELETE c`.
  - `_PRUNE_ALL_CYPHER` (`:152`) — `MATCH (c:Community) DETACH DELETE c` (hierarchy rebuild prunes every level).
  - `_READ_OLD_REPORTS_CYPHER` (`:160`) — read prior reports BEFORE prune-all so unchanged `(level, members_hash)` communities carry their report over instead of re-summarising.
  - `_COMMUNITY_CONSTRAINT` (`:169`) — `CREATE CONSTRAINT community_key ... REQUIRE (c.id, c.level) IS UNIQUE`; plus `ensure_community_indexes(store)` (from `src.graph.index`).
  - Call sites: `detect_communities` write-back (`:518-540`) = constraint + indexes + prune_level + merge_community loop; `detect_hierarchy` write-back (`:680-705`) = read_old_reports + constraint + indexes + prune_all + merge_community/merge_subcommunity loop.
- **Invariant that makes nebula INSERT-overwrite ≡ neo4j MERGE+FOREACH:** BOTH paths **prune before merge**, so every merged node is fresh. neo4j's FOREACH-carry ("set report cols to carry, or leave empty when None") on a fresh node is equivalent to nebula putting the carry values (or defaults) directly into the INSERT columns. No divergence.
- **`src/graph/nebula_schema.py`**: `SCHEMA_DDL` already has `IN_COMMUNITY (level int)` and `PARENT_OF ()` edges, but **no `Community` TAG**. `ensure_schema` runs SPACE_DDL → USE-retry → per-stmt DDL → probe-write readiness. VID = 128-bit blake2b hex, `FIXED_STRING(32)` (`entity_vid` in `nebula_store.py`).

## Global Constraints

- **Default neo4j BUILD path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, every write-back statement + param dict is IDENTICAL to today (the neo4j impl wraps the existing Cypher constants verbatim). Nebula is reached only under `GRAPH_BACKEND=nebula`.
- **Full BUILD stage** (both `detect_communities` and `detect_hierarchy`). SUMMARIZE + READ out of scope.
- Opt-in / strangler-fig. Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store / fake session). `report_vec` is NOT written on the nebula `Community` vertex (it lives in Milvus after Phase 3).
- Mirror the merged vector-store seam shape (`src/graph/entity_vector_store.py`, `community_vector_store.py`) — a parallel write-back seam, NOT a forced generalisation.
- Nebula VID scheme identical to `entity_vid`: `blake2b(digest_size=16).hexdigest()`.
- Fail-open unchanged: the existing try/except around the write-back (`communities.py:519-534`, `:679-705`) stays; seam methods may raise and are caught there exactly as the raw Cypher was.

## Design

### 1. Seam: `CommunityWriteback` (`src/graph/community_writeback.py`, new)

```python
class CommunityWriteback(Protocol):
    def ensure_schema(self) -> None: ...
    def read_old_reports(self) -> list[dict]: ...
    def prune_level(self, level: int) -> None: ...
    def prune_all(self) -> None: ...
    def merge_community(self, *, community_id: str, level: int, member_count: int,
                        members_hash: str, members: list[str], carry: dict | None) -> None: ...
    def merge_subcommunity(self, *, community_id: str, level: int, parent_id: str,
                           member_count: int, members_hash: str, members: list[str],
                           carry: dict | None) -> None: ...
```

`carry` is `None` or `{"report","title","summary","report_vec","summarized_at"}` (mirrors today's `carry_*` params; the single-level path always passes `None`).

Impls:
- **`Neo4jCommunityWriteback(store)`** — each method runs the EXACT existing Cypher constant with the same param dict via `_run_query`. `ensure_schema` = `_COMMUNITY_CONSTRAINT` + `ensure_community_indexes(store)`. `merge_community` reconstructs today's param dict (`carry` → `carry_report`/`carry_title`/`carry_summary`/`carry_report_vec`/`carry_summarized_at`, `None`→`None`). Zero behaviour change.
- **`NebulaCommunityWriteback(store)`** — nGQL (see §3). `ensure_schema` is a no-op (the `Community` TAG + index are created by the global `nebula_schema.ensure_schema`).
- `build_community_writeback(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 2. Schema additions (`src/graph/nebula_schema.py`)

Append to `SCHEMA_DDL` (both `IF NOT EXISTS`, idempotent):
```
CREATE TAG IF NOT EXISTS `Community` (id string, level int DEFAULT 0,
  member_count int DEFAULT 0, members_hash string DEFAULT '', updated int DEFAULT 0,
  report string DEFAULT '', title string DEFAULT '', summary string DEFAULT '',
  summarized_at int DEFAULT 0);
CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level);
```
Report columns (`report`/`title`/`summary`/`summarized_at`) are declared now so the SUMMARIZE sibling slice adds only write logic, not a schema migration. `report_vec` is intentionally absent (Milvus). The `community_level_idx` backs `prune_level` / `prune_all` / `read_old_reports` LOOKUPs (nebula requires an index to LOOKUP by property).

### 3. Nebula nGQL translation (`NebulaCommunityWriteback`)

- **`community_vid(community_id, level)`** = `blake2b((f"{community_id}:{level}").encode(), digest_size=16).hexdigest()` — `FIXED_STRING(32)`, same scheme as `entity_vid`.
- **`merge_community`**: `INSERT VERTEX Community(id, level, member_count, members_hash, updated, report, title, summary, summarized_at) VALUES "<cvid>":(...)` (nebula INSERT = upsert-by-VID; `carry` fills report cols, else defaults; `updated` is computed INSIDE the impl via `int(time.time() * 1000)`, mirroring neo4j's in-Cypher `timestamp()` — `communities.py` is a regular Temporal activity, not a Workflow orchestration script, so wall-clock is available; NO new seam param). Then delete existing `IN_COMMUNITY` edges into `<cvid>` (idempotency; safe because prune-first already removed them), then for each member `INSERT EDGE IN_COMMUNITY(level) VALUES "<entity_vid(member)>"->"<cvid>":(<level>)` (batched). `report_vec` from `carry` is dropped (not a TAG column).
- **`merge_subcommunity`**: same as `merge_community` + `INSERT EDGE PARENT_OF() VALUES "<community_vid(parent_id, level-1)>"->"<cvid>":()`.
- **`prune_level(level)`**: `LOOKUP ON Community WHERE Community.level == <level> YIELD id(vertex) AS vid` → collect vids → `DELETE VERTEX <vids> WITH EDGE` (batched; no-op when empty).
- **`prune_all()`**: `LOOKUP ON Community YIELD id(vertex) AS vid` (index-backed) → `DELETE VERTEX <vids> WITH EDGE`.
- **`read_old_reports()`**: LOOKUP Community vids → `FETCH PROP ON Community <vids> YIELD ...` (or LOOKUP with YIELD of the props) filtered to non-blank `report` → list of `{"level","h","report","title","summary","report_vec","summarized_at"}` dicts (same shape/keys as `_READ_OLD_REPORTS_CYPHER`; `report_vec` always `None`/absent under nebula since it's not stored on the vertex — the carry-over then relies on Milvus for the vector, consistent with Phase 3).
- Statement escaping reuses `nebula_store._q()`-style quoting for string literals.

### 4. Dispatch in `communities.py`

`detect_communities` and `detect_hierarchy` build `writeback = build_community_writeback(store)` once and call `writeback.ensure_schema()` / `.prune_level(level)` / `.prune_all()` / `.read_old_reports()` / `.merge_community(...)` / `.merge_subcommunity(...)` in place of the current inline `_run_query(store, <cypher>, params)` calls. The surrounding try/except fail-open and the GDS-only `finally` projection-drop are unchanged. Compute branches (gds/leidenalg/graphscope) are UNTOUCHED. The `updated` timestamp is NOT a seam param: the neo4j impl keeps `timestamp()` inside its Cypher (byte-for-byte), and the nebula impl computes it internally (§3) — so the `merge_*` signatures carry no clock argument.

### 5. Tests (DB-free)

- **Neo4j impl** (`tests/test_graph/test_community_writeback_neo4j.py`): a fake store records `(cypher, params)`; assert each method issues the EXACT current constant + param dict — byte-for-byte parity proof (guards the default path).
- **Nebula impl** (`tests/test_graph/test_community_writeback_nebula.py`): a fake session records nGQL statements; assert `community_vid` hashing, `INSERT VERTEX Community`, `INSERT EDGE IN_COMMUNITY` (level in edge + entity_vid endpoints), `PARENT_OF` (parent vid), `LOOKUP ... WHERE level == N` + `DELETE VERTEX ... WITH EDGE`, `read_old_reports` shape. No real nebula.
- **Dispatch** (`test_community_writeback_dispatch`): `backend=nebula` → Nebula impl; else Neo4j.
- **Integration** (`tests/test_graph/test_communities.py` extension): `detect_communities`/`detect_hierarchy` route through a fake writeback (records calls); gds/leidenalg/graphscope compute branches provably unchanged.

### 6. Manual gate (live-verify)

On the running nebula cluster, `GRAPH_BACKEND=nebula`: run `detect_communities` then `detect_hierarchy`; verify `:Community` vertices + `IN_COMMUNITY`/`PARENT_OF` edges via `LOOKUP` / `GET SUBGRAPH`; verify a second run prunes cleanly (no ghost communities / orphaned edges). Controller-run.

## Out of scope (deferred)

- **SUMMARIZE on nebula** — `_MEMBER_CONTEXT_CYPHER` / `_CHILD_REPORTS_CYPHER` reads + `_WRITE_REPORT_CYPHER` write (`community.py`) → nGQL. Sibling slice; until then community summaries don't persist under nebula.
- **Community READ on nebula** — `map_communities` / lexical select / descent / doc↔community linkage (`global_search.py`, `documents.py`) → nGQL. Sibling slice / read-path slice C.
- Dropping the neo4j `community_key` constraint / community indexes — only at cutover.

## Interfaces produced

- `src/graph/community_writeback.py`: `CommunityWriteback`, `Neo4jCommunityWriteback`, `NebulaCommunityWriteback`, `build_community_writeback`, `community_vid`.
- `src/graph/nebula_schema.py`: `Community` TAG + `community_level_idx` in `SCHEMA_DDL`.
- `src/graph/communities.py`: `detect_communities` + `detect_hierarchy` route the BUILD write-back through the seam.

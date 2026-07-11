# Nebula community-SUMMARIZE (nGQL) — backend-dispatched

**Status:** approved (autonomous, delegated) 2026-07-11. Sub-project of the NebulaGraph migration (nebula-becomes-default). This is the **SUMMARIZE** stage of the community lifecycle on nebula — sibling of the merged BUILD slice. Branch `feat/nebula-community-summarize` off `main` (base `d3d2e20`).

## Goal

Under `GRAPH_BACKEND=nebula`, community **summarization** reads its context (member entities/relations, or child reports) and persists the generated report on the `:Community` vertex — via nGQL, behind a `CommunitySummarize` seam. The neo4j path is **byte-for-byte unchanged** (default). The `report_vec` Milvus upsert is already backend-dispatched (report_vec slice) and is untouched here.

## Background (current state, grounded)

Three raw-Cypher ops in `src/workflow/search/activities/community.py` fail-open under nebula:

- `_MEMBER_CONTEXT_CYPHER` (`:50`) — level-0 context: from `(c:Community {id,level})<-[:IN_COMMUNITY]-(e:__Entity__)`, per member return `name`, `description`, and `collect(DISTINCT type(r))[..10]` over edges `(e)-[r]-(o:__Entity__)-[:IN_COMMUNITY]->(c)` (o constrained to the SAME community), ordered by name.
- `_CHILD_REPORTS_CYPHER` (`:69`) — level>0 context: `(c)-[:PARENT_OF]->(child:Community)` WHERE `child.report IS NOT NULL`, return `child.title`, `child.summary`, ordered by `child.member_count DESC`.
- `_WRITE_REPORT_CYPHER` (`:81`) — `MERGE (c:Community {id,level}) SET c.report/title/summary/report_vec, c.summarized_at = timestamp()`.

Call sites: `_gather_context(store, params)` (`:322`) routes level-0 → member-context, level>0 → child-reports (falls back to member-context if no children). `summarize_community_activity` (`:381`) computes `report_vec`, upserts it to Milvus via `build_community_report_vector_store(store)` (`:444`, ALREADY backend-dispatched — untouched here), then `_WRITE_REPORT_CYPHER` (`:461`) persists the report on the node.

Nebula facts: `Community` VID = `community_vid(id, level)` (blake2b, from the BUILD slice's `community_writeback`); `Entity` VID = `entity_vid(name)`; `store.structured_query(ngql)` (inline, no param_map) returns `list[dict]`; `RELATED` edge carries `rel_type` (the original Neo4j relationship type). `Community` TAG has `report/title/summary/summarized_at` columns (declared in the BUILD slice) — `report_vec` is NOT a column (Milvus owns it).

## Global Constraints

- **Default neo4j SUMMARIZE path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, the three ops issue the IDENTICAL Cypher + params as today (the neo4j impl wraps the constants verbatim). Nebula reached only under `GRAPH_BACKEND=nebula`.
- `report_vec` is NOT written on the nebula `Community` vertex (Milvus owns it, via the already-dispatched report-vector store). The Milvus upsert path is untouched by this slice.
- Opt-in / strangler-fig. Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store recording statements).
- Nebula binds NO params: fully-inline nGQL, values quoted via `nebula_store._q`, entity/community VIDs via `entity_vid`/`community_vid`.
- Mirror the merged `CommunityWriteback` seam shape (`src/graph/community_writeback.py`) — a parallel, focused `CommunitySummarize` seam, NOT a forced generalisation or a merge into `CommunityWriteback`.
- Fail-open unchanged: the existing try/except wrappers in `_gather_context`/`summarize_community_activity` stay; seam methods may raise and are caught there exactly as the raw Cypher was.

## Design

### 1. Seam: `CommunitySummarize` (`src/graph/community_summarize.py`, new)

```python
class CommunitySummarize(Protocol):
    def read_member_context(self, *, community_id: str, level: int) -> list[dict]: ...
    def read_child_reports(self, *, community_id: str, level: int) -> list[dict]: ...
    def write_report(self, *, community_id: str, level: int, report: str,
                     title: str, summary: str, report_vec: list[float] | None) -> None: ...
```

- `read_member_context` → rows `[{"name","description","rel_types": list[str]}]`, ordered by name (mirrors `_MEMBER_CONTEXT_CYPHER`'s return).
- `read_child_reports` → rows `[{"title","summary"}]`, ordered by `member_count` desc (mirrors `_CHILD_REPORTS_CYPHER`).
- `write_report`: `summarized_at` is NOT a param — each impl stamps "now" internally (neo4j `timestamp()` in-Cypher; nebula `int(time.time()*1000)`). This is what makes the previously-flagged `summarized_at` divergence a non-issue: both write current-time, and BUILD's `read_old_reports` reads it back consistently.

The 3 Cypher constants **move** from `community.py` into `community_summarize.py` (their new canonical home; `community.py` is their only user). Impls:
- **`Neo4jCommunitySummarize(store)`** — `read_member_context`/`read_child_reports` run `_MEMBER_CONTEXT_CYPHER`/`_CHILD_REPORTS_CYPHER` verbatim (same params `{community_id, level}`) and return the rows unchanged; `write_report` runs `_WRITE_REPORT_CYPHER` verbatim (params `{community_id, level, report, title, summary, report_vec}` — `summarized_at` is `timestamp()` inside the Cypher). Byte-for-byte.
- **`NebulaCommunitySummarize(store)`** — nGQL (§2).
- `build_community_summarize(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### 2. Nebula nGQL translation

- **`read_child_reports`**: `GO FROM "<cvid>" OVER \`PARENT_OF\` YIELD dst(edge) AS child;` → child VIDs; then `FETCH PROP ON \`Community\` <child_vids> YIELD \`Community\`.title AS title, \`Community\`.summary AS summary, \`Community\`.report AS report, \`Community\`.member_count AS mc;` → in Python drop rows whose `report` is blank, sort by `mc` desc, return `[{"title","summary"}]`. (No children → `[]`.)
- **`read_member_context`** (the intra-community edge filter is done in Python):
  1. Members: `GO FROM "<cvid>" OVER \`IN_COMMUNITY\` REVERSELY YIELD src(edge) AS m;` → member VIDs (set `M`).
  2. Props: `FETCH PROP ON \`Entity\` <M> YIELD \`Entity\`.name AS name, \`Entity\`.description AS description;` → `{vid: {name, description}}`.
  3. Edges: `GO FROM <M> OVER \`RELATED\` BIDIRECT YIELD src(edge) AS s, dst(edge) AS d, \`RELATED\`.rel_type AS rt;` → keep edges where BOTH `s` and `d` ∈ `M` (intra-community); for each such edge add `rt` to BOTH endpoints' rel_type sets (mirrors the undirected `(e)-[r]-(o)` context for each member).
  4. Assemble per member (ordered by name): `{"name", "description", "rel_types": sorted(distinct)[..10]}`. Blank/empty handled (a member with no intra-community edges → `rel_types: []`).
- **`write_report`**: `UPDATE VERTEX ON \`Community\` "<cvid>" SET report = <q>, title = <q>, summary = <q>, summarized_at = <int(time.time()*1000)>;` — `UPDATE VERTEX` is a PARTIAL update, preserving `member_count`/`members_hash` written by BUILD (unlike `INSERT VERTEX`, which overwrites all columns). `report_vec` is NOT written (Milvus owns it). Assumes BUILD materialised the vertex first (true in the CommunityBuildWorkflow order: detect→summarize); if absent, `UPDATE VERTEX` fails → caught by the caller's fail-open.

### 3. Integration (`community.py`)

- `_gather_context`: build `summ = build_community_summarize(store)`; level>0 → `summ.read_child_reports(...)` (fallback to member-context on empty, as today); level-0 → `summ.read_member_context(...)`. Map rows to the existing context-string builder unchanged. The surrounding try/except fail-open stays.
- `summarize_community_activity`: the `report_vec` Milvus upsert (`report_store.upsert`) is UNCHANGED; replace the `_WRITE_REPORT_CYPHER` call with `summ.write_report(community_id=, level=, report=, title=, summary=, report_vec=)`. The surrounding try/except stays.
- Remove the 3 now-moved Cypher constants from `community.py`; import nothing back except `build_community_summarize` (module-level import is fine — `community_summarize` does NOT import `community.py`, so no cycle; the constants live in `community_summarize.py`).

### 4. Tests (DB-free)

- **Neo4j impl**: fake store records `(cypher, param_map)`; assert each method issues the moved constant + exact params (byte-for-byte parity, guards default path).
- **Nebula impl**: fake store records nGQL; assert `read_child_reports` GO+FETCH + blank-report drop + member_count sort; `read_member_context` member GO + Entity FETCH + RELATED BIDIRECT + Python intra-community filter (an edge to a non-member is EXCLUDED; rel_types deduped + capped at 10 + on both endpoints); `write_report` uses `UPDATE VERTEX` (not INSERT) and omits `report_vec`.
- **Dispatch**: backend=nebula → Nebula; else Neo4j.
- **Integration**: `_gather_context` + `summarize_community_activity` route through a fake summarize seam; neo4j default unchanged; the report_vec Milvus upsert path untouched.

### 5. Manual gate (live-verify)

On the running nebula cluster, `GRAPH_BACKEND=nebula`: after BUILD materialises communities, drive `NebulaCommunitySummarize` (member-context read on a small community, child-reports read on a 2-level pair, write_report) and verify the report/title/summary/summarized_at land on the vertex (FETCH PROP) while member_count/members_hash are preserved (UPDATE, not overwrite). Controller-run.

## Out of scope (deferred)

- Community READ for search (map_communities / lexical select / descent / doc↔community) → nGQL — the sibling READ slice (slice C).
- The `report_vec` Milvus path (already dispatched) and the embedding/LLM prompt logic — unchanged.

## Interfaces produced

- `src/graph/community_summarize.py`: `CommunitySummarize`, `Neo4jCommunitySummarize`, `NebulaCommunitySummarize`, `build_community_summarize`; the 3 moved Cypher constants.
- `src/workflow/search/activities/community.py`: `_gather_context` + `summarize_community_activity` route through the seam; 3 constants removed.

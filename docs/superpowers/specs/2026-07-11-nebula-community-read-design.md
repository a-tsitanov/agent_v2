# Nebula community-READ (map summaries, nGQL) — backend-dispatched

**Status:** approved (autonomous, delegated) 2026-07-11. Sub-project of the NebulaGraph migration (nebula-becomes-default). The **READ** stage of the community lifecycle on nebula — final Stage-1 slice. Branch `feat/nebula-community-read` off `main` (base `e3181cd`).

## Goal

Under `GRAPH_BACKEND=nebula`, GraphRAG global-search's map phase (`map_communities`, lexical path) reads community summaries via nGQL, behind a `CommunityRead` seam. Neo4j path byte-for-byte unchanged. This is the one cleanly-translatable community read; two others are deferred with hard reasons (below).

## Background (grounded)

`_map_communities_lexical` (`global_search.py:294`) issues `_READ_SUMMARIES_CYPHER` (`:44`): `MATCH (c:Community {level:$level}) WHERE c.summary IS NOT NULL AND trim(c.summary) <> '' RETURN c.id AS community_id, c.level AS level, c.summary AS summary, coalesce(c.member_count,0) AS member_count ORDER BY member_count DESC, community_id ASC`, then `rank_summaries(rows, ...)`. Under nebula this raw Cypher fail-opens → empty communities → global search degraded. The `Community` node's `id`/`summary`/`member_count` are on the nebula vertex (written by BUILD + SUMMARIZE). Semantic select already → Milvus (report_vec slice); `community_vid` from `community_writeback`.

## Global Constraints

- **Default neo4j path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, `_READ_SUMMARIES_CYPHER` + params issue identically. Nebula reached only under `GRAPH_BACKEND=nebula`.
- Opt-in / strangler-fig. Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free.
- Nebula inline nGQL (no param_map); values via `nebula_store._q`; VID via `community_writeback.community_vid`.
- Mirror the merged `CommunityWriteback`/`CommunitySummarize` seam shape — a parallel `CommunityRead` seam.
- Fail-safe unchanged: `_map_communities_lexical`'s existing try/except stays.

## Design

### Seam: `CommunityRead` (`src/graph/community_read.py`, new)

```python
class CommunityRead(Protocol):
    def read_summaries(self, *, level: int) -> list[dict]: ...
```
Returns rows `[{"community_id","level","summary","member_count"}]`, summary non-blank, ordered by `member_count` desc then `community_id` asc (mirrors `_READ_SUMMARIES_CYPHER`).

- **`Neo4jCommunityRead(store)`** — runs `_READ_SUMMARIES_CYPHER` verbatim (constant MOVED here from `global_search.py`) with `{"level": level}`; returns rows unchanged. Byte-for-byte.
- **`NebulaCommunityRead(store)`** — `LOOKUP ON \`Community\` WHERE \`Community\`.level == <n> YIELD id(vertex) AS vid;` → `FETCH PROP ON \`Community\` <vids> YIELD \`Community\`.id AS community_id, \`Community\`.level AS level, \`Community\`.summary AS summary, \`Community\`.member_count AS member_count;` → Python: drop rows whose `summary` is blank/whitespace; sort by `(-member_count, community_id)`; return the 4-key dicts. (No vertices → `[]`.)
- `build_community_read(store)`: `settings.graph.backend == "nebula"` → Nebula; else Neo4j.

### Integration (`global_search.py`)

`_map_communities_lexical`: build `reader = build_community_read(store)`; replace the inline `store.structured_query(_READ_SUMMARIES_CYPHER, {"level": params.level})` with `await asyncio.to_thread(reader.read_summaries, level=params.level)`, keeping the try/except + `rank_summaries(rows, ...)` unchanged. Remove the moved `_READ_SUMMARIES_CYPHER` constant; add `from src.graph.community_read import build_community_read` (module-level, no cycle).

### Tests (DB-free)

- Neo4j impl: fake store records `(cypher, param_map)` → asserts `_READ_SUMMARIES_CYPHER` + `{"level": N}` + rows returned verbatim.
- Nebula impl: fake store records nGQL → LOOKUP by level + FETCH + blank-summary drop + `(member_count desc, community_id asc)` sort; reads the `id` PROPERTY as community_id (not the vid).
- Dispatch + integration (`_map_communities_lexical` routes through a fake reader; neo4j default unchanged).

### Manual gate (live-verify)

On the running nebula cluster: after BUILD+SUMMARIZE, `NebulaCommunityRead.read_summaries(level=0)` returns the summarized communities ordered by member_count, blank-summary ones dropped. Controller-run.

## Out of scope (DEFERRED — hard reasons, not corner-cuts)

- **descent selection** (`_DESCENT_ROOT_CYPHER`/`_DESCENT_CHILDREN_CYPHER`) — both `RETURN ... report_vec`, but `report_vec` is NOT on the nebula vertex (Milvus owns it after Phase 3). Descent under nebula requires Milvus-backed report-vector retrieval + `PARENT_OF` traversal — already deferred in the report_vec slice. Separate slice.
- **doc↔community linkage** (`_DOCS_FOR_COMMUNITIES_CYPHER`, `documents.py`) — traverses `(c:Chunk)-[:MENTIONS]->(:__Entity__)-[:IN_COMMUNITY]->(:Community)`, but **chunks are not nebula graph nodes** (no `Chunk` tag; chunks live in Milvus). Needs a different design (chunk doc_id via Milvus metadata or an entity-side MENTIONS model). Separate slice.

## Interfaces produced

- `src/graph/community_read.py`: `CommunityRead`, `Neo4jCommunityRead`, `NebulaCommunityRead`, `build_community_read`; the moved `_READ_SUMMARIES_CYPHER`.
- `src/workflow/search/activities/global_search.py`: `_map_communities_lexical` routes through the seam; constant removed.

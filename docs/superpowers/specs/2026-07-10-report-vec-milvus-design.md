# report_vec slice — semantic community-select via Milvus, backend-dispatched

**Status:** approved 2026-07-10. Sub-project of the NebulaGraph migration (Phase 3, report_vec half — sibling of the merged er_vec slice). Branch `feat/report-vec-milvus` off `main`.

## Goal

Community-report vectors (`report_vec`) live in **Milvus** (`community_report_vec`), and the **semantic** community-select (`select_communities_semantic`) reads from Milvus — backend-dispatched. The Neo4j path is unchanged by default (and semantic-select is opt-in: `AGENT_COMMUNITY_DYNAMIC_SELECTION` defaults to `lexical`). The `descent` mode is deferred.

## Background (current state, from the map)

- **Write:** `community.py::summarize_community_activity` computes `report_vec = await _embed_report(title, summary)` (`community.py:434`), then `_WRITE_REPORT_CYPHER` (`community.py:80-84`) `MERGE (c:Community {id,level}) SET c.report/title/summary/report_vec`.
- **Read (semantic):** `global_search.py::select_communities_semantic(store, query_vec, *, level, limit)` (`:124-156`) issues `_SELECT_SEMANTIC_CYPHER` (`:56-61`, `db.index.vector.queryNodes('community_report_vec', $limit, $vec)` filtered by `level` + non-blank `summary`) → `CommunitySummaryRef(community_id, level, summary)`, fail-open.
- **Read (descent):** `select_communities_descent` reads `report_vec` off nodes + Python `_cosine` over `PARENT_OF` — OUT of scope for this slice.
- Selection mode: `AGENT_COMMUNITY_DYNAMIC_SELECTION: Literal["lexical","semantic","descent"] = "lexical"` (default lexical → report_vec unused unless flipped). Milvus/pymilvus + `MilvusSettings` (dim=1536) as in the er_vec slice.

## Global Constraints

- **Default neo4j semantic-select unchanged.** With `GRAPH_BACKEND=neo4j` + default config, `select_communities_semantic` behaves exactly as today (native `db.index.vector` path); the graph `_WRITE_REPORT_CYPHER` is unchanged. Milvus is reached only under `nebula` or opt-in.
- Opt-in swaps, benchmark before adopting. Unit tests DB-free. Local commits only. Never stage `docs/bruno/collection.bru`. dim = `settings.milvus.dim`, cosine.
- Mirror the merged er_vec slice's shape (`src/graph/entity_vector_store.py`) — a parallel, community-shaped seam, NOT a forced generalization.

## Design

### 1. Seam: `CommunityReportVectorStore` (`src/graph/community_vector_store.py`, new)

```python
class CommunityRef(TypedDict):
    community_id: str
    level: int
    summary: str

class CommunityReport(TypedDict):      # upsert input
    community_id: str
    level: int
    summary: str
    embedding: list[float]

class CommunityReportVectorStore(Protocol):
    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]: ...
    def upsert(self, reports: list[CommunityReport]) -> None: ...
```

Impls:
- **`Neo4jCommunityReportVectorStore(graph_store)`** — `knn` wraps `_SELECT_SEMANTIC_CYPHER` verbatim (`queryNodes('community_report_vec', $limit, $vec)` + level/summary filter); `upsert` is a no-op (report_vec is written on the `:Community` node by `_WRITE_REPORT_CYPHER`, unchanged).
- **`MilvusCommunityReportVectorStore()`** — direct pymilvus, collection `community_report_vec`; `knn` → `client.search(..., filter="level == <level>")`; `upsert` → `client.upsert`.
- `build_community_report_vector_store(graph_store)`: `settings.graph.backend == "nebula"` OR `settings.agent.community_vector_backend == "milvus"` → Milvus; else Neo4j.

### 2. Milvus collection `community_report_vec`

PK `pk` VARCHAR = `f"{community_id}:{level}"` (Milvus PK is single-field), vector `report_vec` FLOAT_VECTOR dim=`settings.milvus.dim` (COSINE/HNSW from `MilvusSettings`), scalars `community_id` VARCHAR, `level` INT64, `summary` VARCHAR (truncated). `knn` uses `filter="level == <level>"` and `output_fields=["community_id","level","summary"]` → `CommunityRef` (**summary comes from Milvus**, so semantic-select needs no graph read). Idempotent `_ensure`, mirroring `MilvusEntityVectorStore`.

### 3. Config

`AgentSettings.community_vector_backend: Literal["native","milvus"] = "native"` (env `AGENT_COMMUNITY_VECTOR_BACKEND`, documented in `scripts/make_env.py`), mirroring `er_vector_backend`. nebula forces Milvus.

### 4. Write integration (`community.py::summarize_community_activity`)

After `report_vec = await _embed_report(...)` and the summary is confirmed non-empty, call `report_store.upsert([{community_id, level, summary, embedding=report_vec}])` when `report_vec` is not None (build the store via `build_community_report_vector_store(store)`; neo4j → no-op, Milvus → write). `_WRITE_REPORT_CYPHER` stays UNCHANGED (writes the structural `:Community` node incl. report_vec on neo4j; redundant-but-harmless under Milvus mode).

### 5. Read integration (`global_search.py::select_communities_semantic`)

Build `report_store = build_community_report_vector_store(store)` and route the kNN through `report_store.knn(query_vec, level=level, limit=limit)`; map its `CommunityRef`s to `CommunitySummaryRef` (same mapping/skip-blank logic as today). Neo4j impl → identical behavior; Milvus impl → Milvus search.

### 6. Backfill + parity

`scripts/backfill_report_vec_milvus.py`: read `:Community` `report_vec`/`summary`/`id`/`level` (non-blank summary) from Neo4j → upsert to `community_report_vec` (dry-run default). Parity: a small optional native-vs-milvus check (report_vec set is far smaller than entities — a lightweight recall spot-check, or reuse the eval pattern). DB-free unit tests with a fake store.

**Level-filter recall divergence (record for the parity benchmark / adoption gate):** the Neo4j `knn` does `queryNodes($limit)` THEN post-filters `WHERE node.level = $level`, so it can return FEWER than `limit` for the requested level; the Milvus `knn` applies `filter="level == N"` DURING search, so it returns up to `limit` for that level. Equivalent today (builds are mostly single-level, level 0), but they diverge once multi-level community hierarchies are live — the parity comparison must treat a per-level count difference as expected, not a regression (the Milvus during-search filter is arguably the more correct behavior).

### 7. Out of scope (deferred)

- `descent` selection mode (Python cosine over `PARENT_OF`).
- The nebula **community-BUILD** graph translation (Leiden + `:Community`/`PARENT_OF` MERGE Cypher→nGQL) — orthogonal; the neo4j-opt-in path is fully functional today; full nebula community-search additionally needs that translation.
- Removing the Neo4j `community_report_vec` index / dropping the node `report_vec` prop — only at cutover.

## Interfaces produced

- `src/graph/community_vector_store.py`: `CommunityRef`, `CommunityReport`, `CommunityReportVectorStore`, `Neo4jCommunityReportVectorStore`, `MilvusCommunityReportVectorStore`, `build_community_report_vector_store`.
- `src/config.py`: `AgentSettings.community_vector_backend` + env doc.
- `src/workflow/search/activities/community.py`: summarize upserts report_vec through the store.
- `src/workflow/search/activities/global_search.py`: `select_communities_semantic` routes through the store.
- `scripts/backfill_report_vec_milvus.py`.

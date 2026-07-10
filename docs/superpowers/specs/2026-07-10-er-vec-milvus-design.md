# Phase 3 (er_vec slice) — ER candidate-kNN via Milvus, backend-dispatched

**Status:** approved 2026-07-10. Sub-project of the NebulaGraph migration (`docs/superpowers/plans/2026-07-09-nebulagraph-migration.md`, Phase 3). Branch `feat/er-vec-milvus` off `main` (which already has Phase 0/1/2).

## Goal

ER's native-kNN candidate lookup works via **Milvus** under `GRAPH_BACKEND=nebula` (which has no in-graph vector index), while the **live Neo4j ER path stays byte-for-byte unchanged** by default. Milvus is available on the neo4j backend as an opt-in for the parity benchmark. This is the `er_vec` slice; `report_vec` (community semantic-select) is a separate later slice.

## Background (current state, from the map)

- ER native-kNN is ON in production: `merge_and_resolve.py` builds `ERConfig(use_native_vector_knn=settings.agent.er_use_native_vector_knn=True, vector_knn_k=20)`; `resolve_entities` → `_load_candidates_native` → per new entity `db.index.vector.queryNodes('er_embedding_vec', $k, $vec)` returns candidate entities (`name, labels, er_vec, er_embedding, mention_count, description`), fed into ER pre-pass + `_candidate_pairs` + LLM judge. Legacy fallback: `_load_existing_canonicals` (mention_count window, default 5000). (`src/graph/entity_resolution.py:1211-1284`, `:1370-1394`.)
- `er_vec` is written onto `__Entity__` nodes in `_build_canonical` (only when `use_native_vector_knn` and an embedding exist); also legacy `er_embedding` JSON. Nebula's `Entity` tag has NO `er_vec` (intentional).
- Milvus already used for chunks via LlamaIndex `MilvusVectorStore` (collection `kb_llamaindex`); chunk fetch-by-doc_id uses a direct `pymilvus.MilvusClient` (`src/storage/chunk_repository.py`). No shared vector abstraction. `MilvusSettings` (host/port/dim=1536/HNSW) in `src/config.py`.
- No labeled ER precision/recall golden set; `tests/eval/scale/bench_er_native.py` measures native-kNN recall/latency on synthetic vectors.

## Global Constraints

- **Default neo4j ER path unchanged.** With `GRAPH_BACKEND=neo4j` and default settings, ER behavior is byte-for-byte what it is today (native `db.index.vector` path). Milvus is reached only under `nebula` or an explicit opt-in.
- Follow project policy: opt-in swaps, benchmark before adopting. Flipping neo4j to Milvus is gated on the parity benchmark.
- Unit tests DB-free (fake `EntityVectorStore`). Local commits only (no push). Never stage `docs/bruno/collection.bru`.
- Embedding dim = 1536 (`MilvusSettings.dim`), cosine.

## Design

### 1. `EntityVectorStore` seam (`src/graph/entity_vector_store.py`, new)

A narrow protocol capturing exactly what ER does with vectors:

```python
class EntityCandidate(TypedDict):
    name: str
    label: str                    # candidate entity type (used to rebuild _Item.label)
    embedding: list[float]        # REQUIRED on knn results — _candidate_pairs cosines candidates
    mention_count: int
    description: str

class EntityVectorStore(Protocol):
    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]: ...
    def upsert(self, entities: list[EntityCandidate]) -> None: ...
```

**`knn` MUST return each candidate's embedding.** `resolve_entities` builds `all_items = new_items + stored_candidates` and `_candidate_pairs` computes pairwise cosines over ALL items (`entity_resolution.py:409,467`); a candidate with no embedding would break auto-merge/borderline generation. The Neo4j impl already returns `er_vec` (or the legacy `er_embedding` JSON); the Milvus impl MUST include the vector field in `search` output.

Two implementations:
- **`Neo4jEntityVectorStore(graph_store)`** — wraps the existing native path (the current `_load_candidates_native` query logic moves here, unchanged: ensure index, `db.index.vector.queryNodes('er_embedding_vec', $k, $vec)`, dedup-by-name). `upsert` is a **no-op** (the `er_vec` node property is persisted by the normal graph node upsert in `_build_canonical` + `upsert_nodes`, exactly as today).
- **`MilvusEntityVectorStore()`** — new, direct `pymilvus.MilvusClient` over collection `entity_er_vec`; `knn` → `client.search`, `upsert` → `client.upsert`.

### 2. Dispatch + config

`build_entity_vector_store(graph_store) -> EntityVectorStore`:
- `settings.graph.backend == "nebula"` → `MilvusEntityVectorStore` (forced — no in-graph index).
- `neo4j` → `Neo4jEntityVectorStore` by default; if `settings.agent.er_vector_backend == "milvus"` → `MilvusEntityVectorStore` (opt-in for the parity benchmark / eventual adoption).

New config: `AgentSettings.er_vector_backend: Literal["native", "milvus"] = "native"` (env `AGENT_ER_VECTOR_BACKEND`), documented in `scripts/make_env.py::_ENV_DESCRIPTIONS`.

### 3. Milvus collection `entity_er_vec` (direct pymilvus, separate from the chunk collection)

Created idempotently on first use (mirrors `chunk_repository.py`'s direct-client style; reuses `MilvusSettings` host/port/dim/HNSW params). Schema:
- PK `name` VARCHAR (max_length e.g. 512) — matches ER's existing dedup-by-name key.
- vector `er_vec` FLOAT_VECTOR dim=1536, metric COSINE, index HNSW (M/efConstruction from `MilvusSettings`).
- scalars `label` VARCHAR (e.g. 256), `mention_count` INT64, `description` VARCHAR (max_length e.g. 4096, truncated on write).

Only **canonical** entities are stored (the Neo4j query filters `er_canonical_name IS NOT NULL`; the Milvus collection only ever receives canonicals — `upsert` is called with the resolved canonicals — so no filter is needed at read time, and the backfill filters `er_canonical_name IS NOT NULL`).

`knn`: `client.search(collection, data=[query_vec], anns_field="er_vec", limit=k, output_fields=["name","label","mention_count","description","er_vec"], search_params={"metric_type":"COSINE","params":{"ef":hnsw_ef_search}})` → `EntityCandidate` dicts **including `embedding` (the returned `er_vec`)**.

### 4. ER integration (`src/graph/entity_resolution.py`)

- **Read:** `resolve_entities` obtains an `EntityVectorStore` (passed in from `merge_and_resolve` / built via the factory) and calls `store.knn(it.embedding, k)` per new item, replacing the direct `_load_candidates_native` call. `_load_candidates_native`'s query logic becomes `Neo4jEntityVectorStore.knn`. Same candidate shape → ER pre-pass / `_candidate_pairs` / judge untouched.
- **Write:** after canonicals are built, call `store.upsert(canonicals)`. On **nebula** this is the only vector persistence (the nebula `Entity` tag has no `er_vec` — already true). On **neo4j default** it is a no-op (the `er_vec` node prop write persists it, as today).
- Wiring: `merge_and_resolve.py` builds the vector store via `build_entity_vector_store(graph_store)` and passes it to `resolve_entities`.

### 5. Backfill

`scripts/backfill_er_vec_milvus.py`: read `er_vec` (or parse legacy `er_embedding`) + `name`/`mention_count`/`description` off existing Neo4j `__Entity__` and `upsert` into `entity_er_vec`. Dry-run default (mirrors `scripts/backfill_er_vector.py`). Greenfield nebula needs no backfill (writes go to Milvus from the start).

### 6. Parity gate

Extend `tests/eval/scale/bench_er_native.py` (or a sibling) to compare **native-kNN vs Milvus-kNN** recall + p95 latency on the synthetic vector set. This is the benchmark that must pass before flipping the neo4j backend to Milvus. Unit tests remain DB-free with a fake `EntityVectorStore`.

### 7. Out of scope (deferred)

- `report_vec` community semantic-select → Milvus → next slice.
- Removing the Neo4j `er_embedding_vec` index / dropping the `er_vec` node property → retained for the neo4j backend; removed only at final cutover.
- Live Milvus behavior of `entity_er_vec` (collection creation, search params) is verified during execution against the running Milvus, mirroring how Phase 2's nGQL was live-probed.

## Interfaces produced

- `src/graph/entity_vector_store.py`: `EntityCandidate`, `EntityVectorStore` protocol, `Neo4jEntityVectorStore`, `MilvusEntityVectorStore`, `build_entity_vector_store(graph_store)`.
- `src/config.py`: `AgentSettings.er_vector_backend` + env doc.
- `src/graph/entity_resolution.py`: `resolve_entities` consumes an `EntityVectorStore`; `_load_candidates_native` logic relocated into `Neo4jEntityVectorStore`.
- `src/workflow/activities/merge_and_resolve.py`: builds + passes the store.
- `scripts/backfill_er_vec_milvus.py`; extended `tests/eval/scale/bench_er_native.py`.

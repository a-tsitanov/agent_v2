# Architecture

`kb-llamaindex` is a multi-store, durable-execution RAG service. It
accepts documents in any language, normalises the knowledge graph to
Russian (entity names + descriptions + relations) while preserving the
source-language chunk text for citation fidelity, and serves four
search modes (`local` / `global` / `drift` / `auto`) of increasing
agentic sophistication.

This document is the **high-level map**. Deeper docs drill into each
layer:

| Doc | Covers |
|---|---|
| [`INGEST.md`](INGEST.md) | Ingest pipeline — activities, queues, claim-check staging, degradation |
| [`SEARCH-FLOW.md`](SEARCH-FLOW.md) / [`SEARCH.md`](SEARCH.md) | Four search modes + deterministic retrieval pipeline + GraphRAG map-reduce |
| [`QUEUES.md`](QUEUES.md) | Temporal task queues + per-queue concurrency caps |
| [`FEATURES.md`](FEATURES.md) | Every feature: what / why / how + the controlling env var |
| [`MODELS.md`](MODELS.md) | Per-role model guidance + swap procedure |
| [`DEPLOYMENT.md`](DEPLOYMENT.md) | Docker stack + ops |
| [`runbook/`](runbook/) | Operator playbooks (mcp, search-usage, multimodel, analytics, wikibase, wiki-editor, er-native-vector-knn) |

> Diagram: [`diagrams/system_architecture.svg`](diagrams/system_architecture.svg)
> (source [`diagrams/system_architecture.d2`](diagrams/system_architecture.d2)).
> Per-flow diagrams live next to their docs (`diagrams/ingest_flow.*`,
> `diagrams/search_modes.*`, `diagrams/kb_search_flow.*`).

---

## 1. Components at a glance

Two long-lived processes plus a stack of stateful backends:

- **API** (`src/api/`) — FastAPI on `:8000`. Thin HTTP surface
  (`/search/*`, `/ingest`, `/documents/{id}`, `/admin/*`), auth via
  `X-API-Key`. Routes validate, submit Temporal workflows, and stream
  results — business logic lives in the workflow/activity/graph
  modules, not the routes.
- **Temporal worker** (`src/workflow/worker.py`) — hosts the workflow
  definitions and all activities across several `Worker` pools (one per
  task queue) in a single process. Durable execution: automatic
  retries, heartbeats, idempotent activities, replay-safe code.
- **MCP servers** (`src/mcp/`) — two optional surfaces (stdio + HTTP/SSE)
  that expose search to external LLM clients (OpenWebUI / Claude
  Desktop / Cursor): **MCP-1** `:9001` (`kb_search`, submits the search
  workflow) and **MCP-2** `:9002` (8 atomic in-process retrieval tools).
  See [`runbook/mcp.md`](runbook/mcp.md).

Cross-cutting:

- **LiteLLM proxy** `:4000` — the single model gateway. All chat and
  embedding calls route through it (per-role models: extraction / judge
  / search / synthesis; multilingual embeddings).
- **LLMPool** (`src/retrieval/llm_pool.py`) — a per-process concurrency
  governor that sits *above* Temporal's queue caps: a per-tier ceiling
  (small = GPU capacity, large = API budget) plus per-role lanes,
  acquired lane-first then tier-global. Temporal caps are set generous
  so the pool is the real arbiter of concurrent LLM calls.

---

## 2. Data stores — what each one holds

| Store | Holds | Connection (default) |
|---|---|---|
| **Milvus** | Chunk vector index — one record per chunk (`id, text, embedding, metadata`). `text` is the **original-language** chunk; embeddings come from the multilingual embed model via LiteLLM. ANN index is **HNSW** (`MILVUS_INDEX_TYPE`, `FLAT` for exact). | `MILVUS_HOST:MILVUS_PORT` (`localhost:19530`); collection `MILVUS_COLLECTION` |
| **Neo4j** | Property graph **and** two native indexes in one store: `:__Entity__:<Type>` nodes + typed relations (names/descriptions **in Russian** post-merge), `:Chunk` nodes linked via `(:Chunk)-[:MENTIONS]->(:__Entity__)`, a **native vector index** over entity embeddings (`graph_search` kNN; plus `er_vec` for native-vector ER), a **fulltext index** on `__Entity__.name` (`find_entity_by_name`), and `:Community` hierarchy + reports (`community_report_vec`) for global search. | `NEO4J_URI` (`bolt://localhost:7687`) |
| **Postgres** | `documents` job/status table (doc_id → status → metadata) and `ingest_metrics` (per-activity durations + per-role model tags for analytics). | `POSTGRES_*` |
| **MinIO** | Uploaded source files (served back by `GET /documents/{id}`) **and** claim-check staging blobs — heavy ingest state (parsed nodes, KG, merged entities) pickled and passed between activities by URI. | `MINIO_*` (console `:9001`, S3 API `:9000`) |
| **Wikibase / MediaWiki** | The curated **canonical anchor**: a self-hosted Wikibase Item per entity (`push_wikibase`, opt-in) + per-entity MediaWiki article pages written by the continuous wiki editor. WDQS provides the SPARQL endpoint. | `wikibase` / `wdqs` containers |

Supporting infra: **etcd** + **MinIO** back Milvus; **wikibase-mysql**
(MariaDB) backs Wikibase; **temporal** (+ **temporal-ui** `:8080`) is
the durable-execution backend; **Prometheus** + **Grafana** are
observability. See [`DEPLOYMENT.md`](DEPLOYMENT.md) / `docker-compose.yml`.

Reset everything: `uv run python -m scripts.wipe_db --yes`.

---

## 3. Ingest path

`POST /ingest` uploads the file to MinIO, inserts a `pending` row in
Postgres, snapshots the per-role model names, and starts the durable
**`DocumentIngestWorkflow`** (queue `kb-ingest`). The workflow runs a
fixed activity sequence; heavy state travels as **MinIO blobs by URI**
(claim-check) so only small contracts ride Temporal payloads.

Two halves, deliberately separated:

- **Vector half** — `fetch_source` → `parse_and_chunk` (split →
  deterministic identifier canonicalization → optional translate-to-RU)
  → `index_vector` (embed → Milvus). If this fails, ingest fails
  (`mark_failed`).
- **Graph half** — `inject_canonical` (identifier entities into Neo4j)
  → `extract_kg` (LightRAG, one LLM call/chunk, queue `kb-ingest-llm`)
  → **`GraphBuildWorkflow` child** (queue `kb-ingest-merge`):
  `merge_and_resolve` (cross-chunk merge → phone consolidation →
  entity resolution) → `build_property_graph` (upsert to Neo4j).

**Degradation:** if the graph half raises/times out, the parent catches
it and sets `graph_status = "vector_only"` — the document is still
vector-searchable, the graph is just skipped (and `push_wikibase` with
it). Graph is augmentation, not a blocker.

Best-effort tails: `mark_entities_dirty` (flag entities for the wiki
editor) and `push_wikibase` (project into the Wikibase anchor, only if
the graph completed), then `finalize` writes the final status +
`ingest_metrics`.

Full activity table, sequence diagram, staging contracts → [`INGEST.md`](INGEST.md).

---

## 4. Search path

`POST /search/{local,global,drift,auto}` — all four are durable Temporal
workflows submitted from `src/api/routes/search_v2.py`, sharing one
`SearchRequest` / `SearchResponse` shape (including client-managed
`history` for multi-turn).

| Mode | Workflow | Shape |
|---|---|---|
| `local` | `SearchOrchestratorWorkflow` | plan sub-questions → fan-out parallel `SubQueryRetrievalWorkflow` → merge/dedup → coverage gate → bge rerank → large-tier synthesis |
| `global` | `GlobalSearchWorkflow` | GraphRAG map-reduce over community reports (MAP small-tier per community → REDUCE large-tier once) |
| `drift` | `DriftSearchWorkflow` | local pass, then global expansion seeded with the local sources; degrades to the local answer if global fails |
| `auto` | `AutoSearchWorkflow` | `route_query` classifies → dispatches local/global/drift (fail-safe → local) |

Key properties:

- **Deterministic retrieval, not a ReAct loop.** Each sub-question runs
  a fixed tool sequence — `vector_search` (Milvus) → `graph_search`
  (Neo4j native-vector entity kNN + LLM synonyms) → `find_entity_by_name`
  (Neo4j fulltext) → `graph_walk` (bounded N-hop, dual-seeded from both
  the top graph_search and fulltext entity). Results merge + dedup by
  `chunk_id`. (The older Self-RAG / ReAct path was **removed** in the
  R7b cutover.)
- **Conversation history** — when `history` is present, a
  `contextualize_query` activity rewrites the follow-up into a
  standalone question once at the start of the workflow.
- **Hierarchical communities** — global/drift select over a Leiden
  **hierarchy** of `:Community` nodes with structured reports, via
  lexical / semantic-kNN / hierarchy-descent selection. Communities are
  built **offline** by `CommunityBuildWorkflow` (queue `kb-graph-build`,
  admin-triggered), fully decoupled from the query hot path.
- **Russian-only output** — `synthesize_answer` wraps the query with a
  Russian-output instruction so the answer language matches the graph
  normalisation regardless of source-language chunks.

Modes, retrieval tools, community selection → [`SEARCH-FLOW.md`](SEARCH-FLOW.md)
and [`SEARCH.md`](SEARCH.md).

---

## 5. Durable execution & queues

The worker hosts one `Worker` pool per task queue so GPU / LLM pressure
on one workload can't starve another (head-of-line blocking). Queues:

| Queue | Hosts | Cap (default) |
|---|---|---|
| `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding activities | 4 |
| `kb-ingest-llm` | `extract_kg` only (extract lane) | 18 |
| `kb-ingest-merge` | `GraphBuildWorkflow` + merge/build (merge lane) | 14 |
| `kb-search-small` | search workflows + plan/retrieve/coverage/rerank/route/map activities | 4 |
| `kb-search-large` | `synthesize_answer` only (large-tier final synthesis) | 2 |
| `kb-graph-build` | `CommunityBuildWorkflow` (offline GDS-Leiden communities) | 2 |
| `kb-wiki` | `WikiSweepWorkflow` (continuous per-entity MediaWiki editor) | 4 |

Temporal's per-queue caps bound how many activities *schedule*; the
per-process **LLMPool** then bounds how many LLM calls actually run
concurrently (tier ceiling + role lanes). Caps are kept ≥ the pool's
lane ceilings so the pool arbitrates first. Full rationale + the
`TEMPORAL_*_ACTIVITY_CONCURRENCY` knobs → [`QUEUES.md`](QUEUES.md).

---

## 6. Knowledge anchor & wiki editor

Beyond the RAG stores, entities flow into a curated identity layer:

- **Wikibase populator** (`push_wikibase`, opt-in `WIKIBASE_ENABLED`) —
  ingest mints/patches a Wikibase Item per entity keyed on
  `wikibase_qid`, folding identifier-type entities as external-id
  statements; queryable via WDQS SPARQL.
- **Continuous wiki editor** (`WikiSweepWorkflow`, queue `kb-wiki`,
  opt-in `WIKI_ENABLED`) — ingest marks touched entities `wiki_dirty`;
  a scheduled sweep rewrites a bot-managed MediaWiki article section
  per entity **from graph facts only** (anti-drift, cited), preserving
  human edits, skipping unchanged entities via a subgraph hash.

→ [`runbook/wikibase.md`](runbook/wikibase.md),
[`runbook/wiki-editor.md`](runbook/wiki-editor.md), [`FEATURES.md`](FEATURES.md#3-knowledge-anchors).

---

## 7. Observability

- **Temporal UI** `:8080` — workflow/activity timelines, retries,
  failures.
- **Prometheus** + **Grafana** — the worker exports Temporal SDK metrics
  via a Prometheus exporter (`src/workflow/worker.py::_build_runtime`,
  gated on `METRICS_ENABLED`); Grafana dashboards (Ingest Overview,
  Version compare, Run drill-down) read those plus the Postgres
  `ingest_metrics` table.
- **`ingest_metrics`** — per-activity durations + per-role model tags
  (snapshotted at submit), so dashboards attribute each step to the
  exact model that ran it even after a model swap.
- **Answer-quality eval** — `tests/eval/` grades endpoint responses
  (fact/entity recall, citation precision, hallucination bound)
  deterministically and offline.

→ [`runbook/analytics.md`](runbook/analytics.md).

---

## 8. Configuration

All settings flow through `src/config.py` (pydantic-settings, reads
`.env`), namespaced per subsystem: `API_`, `MILVUS_`, `NEO4J_`,
`POSTGRES_`, `MINIO_`, `LITELLM_`, `TEMPORAL_`, `INGESTION_`, `AGENT_`,
`LLM_POOL_`, `WIKIBASE_` / `WIKI_`, `METRICS_`. New-feature toggles
(native-vector ER, conversation history, dual walk-seed, hierarchical
communities, Milvus index type) are listed in
[`FEATURES.md`](FEATURES.md#config-quick-reference-new-feature-env-vars);
the model-swap procedure is in [`MODELS.md`](MODELS.md).

---

## 9. Not yet wired

- Hybrid retriever (BM25 + vector RRF) exists in
  `src/retrieval/hybrid.py` but isn't in the live retrieval path.
- Multi-tenant data isolation — `department` flows through metadata but
  isn't enforced at retrieve time.
- A configured Temporal **Schedule** for community/wiki rebuilds (today
  they're admin-triggered).

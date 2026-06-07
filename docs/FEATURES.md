# Features

Reference for every feature in kb-llamaindex: what it is, why it exists, how it works, and the env/config that controls it. Process flows live in [`INGEST.md`](INGEST.md) and [`SEARCH-FLOW.md`](SEARCH-FLOW.md); operator playbooks in [`runbook/`](runbook/).

**Legend:** 🆕 = added in the 2026-06 scale/GraphRAG work (deep-dived in [§ New features](#new-features-deep-dive)). All 🆕 search/ER/community behaviours are **opt-in with defaults equal to prior behaviour**.

---

## 1. Ingestion

### Parsing & chunking
LlamaIndex `IngestionPipeline`: a reader extracts the document, a splitter (`SentenceSplitter` or `SemanticSplitterNodeParser`) produces chunks (`INGESTION_CHUNK_SIZE` / `_OVERLAP`). Output is `BaseNode` chunks carrying text + metadata. — `ingestion/pipeline.py`, `activities/parse_and_chunk.py`

### Multilingual translation
Optional translate-to-Russian transform (per-document or per-chunk by size threshold) so a heterogeneous corpus normalises for embedding/extraction while **entity names stay in the source language**. Translation metadata is scrubbed before downstream stores. — `runbook/multimodel.md`

### Deterministic identifier canonicalization
24 structured identifier types (phones→E.164, INN/OGRN/BIC with checksums, email, URLs, postal addresses via libpostal, dates, amounts, IMEI/MAC/VIN/plates …) extracted **without an LLM**, stored on chunk metadata, appended to chunk text (so the LLM sees canonical forms), and pre-injected as `:__Entity__` nodes **before** KG extraction. Guarantees identifier dedup even when the LLM extracts a verbatim mention. — `ingestion/identifiers.py`, `ingestion/identifier_transform.py`, `activities/inject_canonical.py`

### LightRAG knowledge-graph extraction
Per chunk, one LLM call (the `LightRAGExtractor`, ported from HKUDS/LightRAG) emits typed entities (name + type + 1–2 sentence description) and relations (src + tgt + keywords + description) in a single structured response. Multilingual prompt; `/no_think` for qwen3. — `graph/lightrag_extract.py`, `graph/lightrag_prompts.py`, `activities/extract_kg.py`

### Entity Resolution (ER) 🆕(native-vector)
Cross-chunk + cross-document deduplication of semantically-equal entities ("BCC" ≡ "Базальноклеточный рак"; "Иванов И.И." ≡ "Иван Иванов"). Pipeline: within-batch name merge → phone consolidation → **ER** (`resolve_entities`): candidate generation (vectorised cosine + name-overlap), an LLM judge for borderline pairs (with a Neo4j `:ERVerdict` cache), union-find clustering, hyper-hub clamp, canonical selection. Identifier-type labels are excluded (they have deterministic canon). See [§ Native-vector ER](#native-vector-er-) for the new kNN path. — `graph/entity_resolution.py`, `activities/merge_and_resolve.py`

### Property-graph build
Merged entities/relations upserted to Neo4j with `(:Chunk)-[:MENTIONS]->(:__Entity__)` edges; entity embeddings written to the native vector index; fulltext + range indexes ensured. — `activities/build_property_graph.py`, `graph/index.py`

### Multimodel & analytics
Per-role model names snapshotted at submit and written per-activity into the Postgres `ingest_metrics` table (durations + version tags), so dashboards reflect the exact model that ran each step. — `runbook/multimodel.md`, `runbook/analytics.md`

---

## 2. Search

The four modes, the deterministic tool pipeline, reranking, and coverage are documented in [`SEARCH-FLOW.md`](SEARCH-FLOW.md). Summary:

- **local** — plan → parallel deterministic retrieve (vector_search, graph_search, find_entity_by_name, graph_walk) → coverage check → bge rerank → large-tier synthesis.
- **global** — map-reduce over community reports.
- **drift** — local then global, with graceful fallback 🆕.
- **auto** — a router classifies the query and dispatches one mode.
- **Reranker** — bge-reranker-v2-m3 cross-encoder, top-N to synthesis.
- **Coverage check** — detects an evidence gap and runs one extra targeted round.

New search behaviours: [conversation history](#conversation-history-), [dual walk-seed](#dual-walk-seed-), [drift fallback](#drift-graceful-fallback-), [community indexes](#community-indexes-).

---

## 3. Knowledge anchors

### Wikibase canonical anchor
A self-hosted Wikibase instance is the curated identity layer: ingest projects entities/relations into it (`push_wikibase`, best-effort), minting/patching Items keyed by `wikibase_qid`, folding identifier-type entities as external-id statements. Off by default (`WIKIBASE_ENABLED`). — `runbook/wikibase.md`, `activities/push_wikibase.py`

### Continuous wiki editor (Project A)
Turns Neo4j entities into per-entity MediaWiki article pages. Ingest marks affected entities `wiki_dirty`; a scheduled `WikiSweepWorkflow` (queue `kb-wiki`) rewrites a bot-section between markers **from graph facts only** (anti-drift), preserving human edits, with a content-hash skip for unchanged entities. Off by default (`WIKI_ENABLED`). — `runbook/wiki-editor.md`

---

## 4. Platform

### Durable Temporal workflows + queues
Ingest and search are durable workflows with automatic retries, heartbeats, and idempotent activities. Dedicated queues isolate LLM bursts from Neo4j-write/merge work: `kb-ingest`, `kb-ingest-llm`, `kb-ingest-merge`, `kb-search-small`, `kb-search-large`, `kb-graph-build`, `kb-wiki`. — [`QUEUES.md`](QUEUES.md)

### LLMPool (per-process concurrency)
A single per-process pool owns LLM concurrency with hierarchical gates — a tier ceiling (small=GPU capacity, large=API budget) and per-role lanes (extraction/judge/search/…), acquired lane-first then tier-global — so Temporal's queue caps can be generous while actual concurrent LLM calls match the GPU. — `retrieval/llm_pool.py`

### Claim-check staging
Heavy state (nodes, entities) is pickled to MinIO and passed between activities by URI; only small contracts travel in Temporal payloads; orphan blobs from crashed runs are swept. — `workflow/staging.py`

### MCP servers
Two MCP surfaces expose search to OpenWebUI / Claude Desktop / Cursor: MCP-1 (`kb_search` via the Temporal search workflow) and MCP-2 (atomic retrieval tools in-process). — `runbook/mcp.md`

### Scale-bench harness 🆕
A synthetic, zero-prod-data benchmark suite (`tests/eval/scale/`) that brackets the scaling cliffs (ER candidate-gen O(N²), Milvus FLAT vs HNSW, graph_walk hub cost, native-vector ER reach vs the window) by generating realistic data shapes locally. — `tests/eval/scale/README.md`

---

## New features deep dive

### Native-vector ER 🆕
**Problem:** incremental ER loaded at most a 5000-entity window per ingest; at 250k canonicals that window reaches ~2 % of true nearest matches (measured), so new mentions silently fragment into duplicates — degrading every search mode.
**Fix (opt-in):** store the ER embedding as a native Neo4j vector (`er_vec`) + a `er_embedding_vec` index, and replace the window load with a per-entity `db.index.vector.queryNodes` kNN over the **whole graph**. Measured: ~96 % recall at ~6 ms/query vs ~2 % for the window.
**Also fixed:** the window load now `ORDER BY mention_count DESC` (hub entities always in-window); candidate generation vectorised (~118× — `_normalized_matrix` BLAS cosine + per-item token cache); stored-loser cleanup is safe-by-inaction (no silent edge loss).
**Enable (after a Neo4j backup):**
```bash
python -m scripts.backfill_er_vector --no-dry-run   # parse er_embedding JSON → er_vec + build index
AGENT_ER_USE_NATIVE_VECTOR_KNN=true                 # restart ingest worker
```
Default OFF. — runbook [`runbook/er-native-vector-knn.md`](runbook/er-native-vector-knn.md), `graph/entity_resolution.py`

### Conversation history 🆕
**Problem:** `/search` was stateless — follow-ups ("а что по цене?") had no referent.
**Fix:** the client passes `history` (turns); a small-LLM `contextualize_query` activity rewrites the follow-up into a **standalone question** once at the start of each workflow (only when history is non-empty); `params.model_copy(query=…)` makes the whole pipeline use it. Client-managed (no server sessions, stays stateless/replay-safe); the enable gate is resolved at submit time (`contextualize_enabled`). Drift contextualises once and clears children history.
**Config:** `AGENT_CONVERSATION_HISTORY_ENABLED` (default true, inert without history), `AGENT_HISTORY_MAX_TURNS` (6), `AGENT_HISTORY_MAX_CHARS` (4000). — `activities/contextualize.py`

### Hierarchical communities + dynamic selection 🆕
**Problem:** flat level-0 communities + short summaries + O(N) Python lexical ranking — weak, semantically blind ("GPU"≠"видеокарта"), and re-summarised in full every build.
**Fix (GraphRAG-style, opt-in):**
- **Hierarchy** — one GDS Leiden run with `includeIntermediateCommunities` materialises multi-level `:Community` + `PARENT_OF` edges (level 0 = coarsest, back-compat; `members_hash` per community). — `graph/communities.py::detect_hierarchy`
- **Structured reports** — `{title, summary, findings:[{statement, importance}]}` generated bottom-up (level-0 from members, level>0 from child reports), embedded into a native `community_report_vec` index. — `activities/community.py`
- **Incremental** — a community whose `(level, members_hash)` is unchanged **carries its report over** (no LLM); the build runs **level-by-level finest-first** and summarises only changed communities. — `community_wf.py`
- **Dynamic selection** for global/drift — **v1 semantic** (kNN over `community_report_vec`) and **v2 descent** (start coarsest, rank by cosine, descend `PARENT_OF` into relevant children → finest relevant), with **lexical fallback** on empty/error. — `activities/global_search.py`
**Config:** `AGENT_COMMUNITY_MAX_LEVELS` (default 1 = single-level/today; raise to build the hierarchy), `AGENT_COMMUNITY_DYNAMIC_SELECTION` (`lexical`|`semantic`|`descent`, default `lexical` = today). Build the hierarchy via the community-rebuild admin trigger, then flip selection. Spec/plan in [`superpowers/specs`](superpowers/) ; backlog (recursive coarsening, claims) in [`superpowers/backlog-graph-scale.md`](superpowers/backlog-graph-scale.md).

### Dual walk-seed 🆕
`graph_walk` is seeded from **both** the top `graph_search` entity and the top `find_entity_by_name` entity when they differ (results deduped by chunk_id), so a fulltext-matched entity contributes its neighbourhood even when `graph_search` already returned something. `AGENT_GRAPH_WALK_DUAL_SEED` (default on). — `activities/retrieve.py`

### Drift graceful fallback 🆕
If the global pass of a drift query fails/times out, the request **degrades to the local answer** (mode kept `"drift"`) instead of failing the whole request. — `search/router_wf.py::_drift_local_fallback`

### Community indexes 🆕
Range indexes on `Community.level` (global summary read) and `Chunk.doc_id` (community→document traversal) — the `community_level` index is required despite the `(id,level)` composite constraint (composite can't serve a level-only lookup). — `graph/index.py::ensure_community_indexes`

---

## Config quick-reference (new feature env vars)

| Env | Default | Effect |
|---|---|---|
| `AGENT_ER_USE_NATIVE_VECTOR_KNN` | false | ER kNN over the whole graph (after backfill) instead of the 5000-window |
| `AGENT_ER_VECTOR_KNN_K` | 20 | neighbours per new entity (native ER) |
| `AGENT_CONVERSATION_HISTORY_ENABLED` | true | contextualise follow-ups when `history` is provided |
| `AGENT_HISTORY_MAX_TURNS` / `_CHARS` | 6 / 4000 | bound the history fed to contextualisation |
| `AGENT_GRAPH_WALK_DUAL_SEED` | true | seed graph_walk from graph_search + fulltext |
| `AGENT_COMMUNITY_MAX_LEVELS` | 1 | Leiden hierarchy depth to materialise (1 = today) |
| `AGENT_COMMUNITY_DYNAMIC_SELECTION` | lexical | global/drift community selection: lexical \| semantic \| descent |
| `MILVUS_INDEX_TYPE` | HNSW | chunk ANN index (set FLAT for exact) — applied on (re)create |

# Architecture

`kb-llamaindex` is a multi-store, async RAG service.  It accepts
documents in any language, normalises the knowledge graph to
Russian (entities + descriptions + relations), preserves the
source-language chunk text for citation fidelity, and serves three
parallel search endpoints with increasing levels of agentic
sophistication.

This document is the **single map** of the system — every other
doc (`DEPLOYMENT.md`, `SEARCH.md`, `runbook/search-usage.md`,
`MODELS.md`) drills into a specific layer.

> ⚠️ **Note (2026-05-19):** Section 3 «Ingestion data flow» below
> describes the **taskiq-era** pipeline; the current implementation
> uses **Temporal workflows** with a `DocumentIngestWorkflow` parent
> + `GraphBuildWorkflow` child (for `merge_and_resolve` +
> `build_property_graph`).  Per-activity model attribution lives in
> the Postgres `ingest_metrics` table.  Read the updated story in
> [`runbook/multimodel.md`](runbook/multimodel.md) (current state +
> code excerpts) and [`runbook/analytics.md`](runbook/analytics.md)
> (observability).  This section is retained as a reference for the
> pre-Temporal design — it will be rewritten in a future sprint.

---

## 1. Top-level data flow

```
┌─────────────┐                                        ┌──────────────┐
│  user       │                                        │  user        │
│  upload     │                                        │  query       │
└──────┬──────┘                                        └──────┬───────┘
       ▼                                                      ▼
┌──────────────┐    ┌──────────┐                       ┌──────────────┐
│ POST         │    │ RabbitMQ │                       │ POST         │
│ /api/v1/     │───►│ taskiq   │                       │ /api/v1/     │
│ ingest       │    │ broker   │                       │ search       │
└──────────────┘    └────┬─────┘                       │ /agent       │
                         │                             │ /selfrag     │
                         ▼                             │ /legacy/agent│
                  ┌──────────────┐                     └──────┬───────┘
                  │ Taskiq       │                            │
                  │ worker       │                            │
                  │ process_doc  │                            ▼
                  └──────┬───────┘            ┌───────────────────────────┐
                         │                    │ Retrieval stack:          │
            ┌────────────┼────────────┐       │  Milvus  · Neo4j · FS     │
            ▼            ▼            ▼       └────────────┬──────────────┘
       ┌────────┐  ┌─────────┐  ┌─────────┐                │
       │ Milvus │  │ Neo4j   │  │Postgres │                ▼
       │chunks  │  │KG nodes │  │job state│        ┌──────────────┐
       │embedded│  │+typed   │  │+source  │        │ LLM via      │
       │vectors │  │rels     │  │ path    │        │ LiteLLM      │
       └────────┘  └─────────┘  └─────────┘        │ (OpenAI /    │
                                                   │  Ollama)     │
                                                   └──────────────┘
```

Four storage backends, one shared LLM/embed gateway (LiteLLM),
two long-lived processes (API + taskiq worker).

---

## 2. Storage components

| Store | Role | Connection | Wipe target |
|---|---|---|---|
| **Milvus** | Vector index over chunks. One record per chunk: `id, text, embedding, metadata`. The `text` is the **original-language** chunk; embeddings are produced by the multilingual `text-embedding-3-small` (1536-dim). | `MILVUS_HOST:MILVUS_PORT` (default `localhost:19530`); collection `MILVUS_COLLECTION` (default `kb_llamaindex`). | Drop collection. |
| **Neo4j** | Property graph: `:__Entity__:<EntityType>` nodes (typed), `:Chunk` nodes (linked to entities via `:MENTIONS`), semantic relations between entities (`:CAUSATION`, `:RISK_FACTOR`, ...). Entity names + descriptions stored **in Russian** post-merge. | `NEO4J_URI` (default `bolt://localhost:7687`). | `MATCH (n) DETACH DELETE n` + drop non-constraint indexes. |
| **Postgres** | Job-status table `documents(id, path, department, doc_type, status, error, summary, created_at, updated_at)`. Maps `doc_id` UUID → on-disk source path → upload metadata. | `POSTGRES_*` env block. | `TRUNCATE documents`. |
| **RabbitMQ** | taskiq's broker. One queue: `process_document`. | `RABBITMQ_URL` (default `amqp://guest:guest@localhost:5672/`). | Force-recreate container + remove `rabbitmq_data` volume. |
| **Filesystem** (`API_UPLOAD_DIR`) | Raw uploaded files. Used by `read_full_document` tool to surface the original text to the ReAct agent. | `API_UPLOAD_DIR` (default `/tmp/kb-uploads`). | `rm -rf $API_UPLOAD_DIR`. |
| **LiteLLM proxy** | Single entry point for LLM + embedding calls. Routes to OpenAI by default (`gpt-4o-mini`, `text-embedding-3-small`). Container env reads `OPENAI_API_KEY` from host `.env`. | `LITELLM_BASE_URL` (default `http://localhost:4000`). | Container restart (stateless). |

Reset all of them at once: `uv run python -m scripts.wipe_db --yes`
(see `scripts/wipe_db.py`).

---

## 3. Ingestion data flow (per document)

```
Document file
    │
    ▼  POST /api/v1/ingest  (multipart upload)
┌──────────────────────────────────────────────────┐
│ src/api/routes/ingest.py:upload_document         │
│   1. write file → ${API_UPLOAD_DIR}/{uuid}_{name}│
│   2. INSERT documents(status='pending')          │
│   3. process_document.kiq(doc_id, path)          │
└────────────────────┬─────────────────────────────┘
                     │ taskiq → RabbitMQ
                     ▼
┌──────────────────────────────────────────────────┐
│ src/ingestion/tasks.py:process_document          │
│  → UPDATE documents.status = 'processing'        │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│ src/ingestion/pipeline.py:build_ingestion_pipeline│
│  Transformations (run by IngestionPipeline.arun):│
│                                                  │
│  1. SimpleDirectoryReader.load_data()            │
│       → Document objects                          │
│                                                  │
│  2. SentenceSplitter(chunk_size=512, overlap=50) │
│       → list[TextNode] (original-language text)  │
│                                                  │
│  3. IdentifierCanonicalizationTransform          │
│       per chunk:                                 │
│       • extract_identifiers(text) — regex sweep  │
│         + checksum-validated detectors for 19    │
│         types across three groups:               │
│           business:  PhoneNumber, Email, INN,    │
│                      OGRN, BIC, SNILS,           │
│                      ContractNumber,             │
│                      PostalAddress,              │
│                      DocumentDate, Amount.       │
│           digital:   URL, Domain, TelegramHandle,│
│                      VKProfile, TwitterHandle,   │
│                      InstagramHandle,            │
│                      LinkedInProfile,            │
│                      YouTubeChannel,             │
│                      GitHubProfile, UUID.        │
│           device:    IMEI (Luhn), MACAddress,    │
│                      LicensePlate (RU pattern    │
│                      + context-anchored generic),│
│                      VIN (mod-11).               │
│         Overlap resolver favours specialised     │
│         types over generic (URL > Domain etc.).  │
│       • node.metadata['canonical_identifiers']   │
│         ← list[dict]  (canonical+original+span)  │
│       • node.text += "\\nКанонические идентификато│
│         ры: ..."  (augment block — feeds the LLM │
│         extractor in-band)                       │
│                                                  │
│  4. TranslateToRussianTransform  (NEW)           │
│       per chunk:                                 │
│       • _looks_russian(text)? → skip (no LLM)    │
│       • else: LLM call with TRANSLATE_PROMPT     │
│         (preserves proper nouns / IDs /          │
│          inline-code / drug names)               │
│       • node.metadata['translated_text'] ← RU   │
│       • node.text is UNCHANGED                  │
└────────────────────┬─────────────────────────────┘
                     │ list[TextNode]
                     ▼
┌──────────────────────────────────────────────────┐
│ Vector indexing                                  │
│   src/retrieval/vector_index.py:index_nodes      │
│   → MilvusVectorStore.upsert(nodes)              │
│     stores original-language text + embedding    │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│ Graph build (best-effort, wrapped in try/except) │
│                                                  │
│  Step A — canonical identifier nodes             │
│   src/ingestion/identifier_transform.py          │
│   :inject_canonical_entities                     │
│       reads canonical_identifiers metadata,      │
│       upserts EntityNode(label='Email', ...) etc │
│       to Neo4j with snippet±80chars description  │
│                                                  │
│  Step B — LightRAG-style extraction              │
│   src/graph/lightrag_extract.py:LightRAGExtractor│
│       per chunk: ONE LLM call reading            │
│         node.metadata['translated_text'] (RU)    │
│       prompt outputs entity<|#|>name<|#|>type<|#|>│
│         description and relation<|#|>...<|#|>... │
│       node.metadata[KG_NODES_KEY] ← EntityNode[] │
│       node.metadata[KG_RELATIONS_KEY] ← Relation[]│
│                                                  │
│  Step C — cross-chunk merge                      │
│   src/graph/merge.py:merge_kg_extraction         │
│       aggregates per name; concat (<8 mentions,  │
│       <12k chars) OR LightRAG summarize-LLM call.│
│       Same for relations (undirected pair key).  │
│                                                  │
│  Step D — PropertyGraphIndex(NoOpKGExtractor)    │
│       writes :Chunk nodes (ORIGINAL text),       │
│       creates :Chunk-[:MENTIONS]→:__Entity__,    │
│       embeds entities inline for graph search.   │
│       Pops the per-chunk metadata.               │
│                                                  │
│  Step E — graph_store.upsert_nodes/upsert_relations│
│       Overwrites per-chunk descriptions with     │
│       cross-chunk merged versions.               │
└────────────────────┬─────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────┐
│ UPDATE documents.status = 'completed' (or        │
│ 'failed' + error message on any uncaught raise)  │
└──────────────────────────────────────────────────┘
```

### Why graph work is best-effort

If Neo4j is unreachable OR the LLM extraction fails on a chunk,
the worker swallows the exception and continues.  The vector
index (Milvus) is still populated; `/api/v1/search` will work,
just without the graph layer.  This is intentional — graph is
**augmentation**, not blocking.  Error visible in logs and the
`documents.error` field.

### Re-ingest cost (1 MB English corpus, ~514 chunks)

| Step | LLM calls |
|---|---|
| Translate to Russian | ~514 |
| LightRAG extract | ~514 |
| Cross-chunk merge summary (≥8 occurrences) | ~100-150 |
| Entity description (embedding only, not LLM) | ~2 500 embeddings |
| **Total LLM chat calls** | **~1 200** |
| Wall time on gpt-4o-mini | ~15-25 minutes |

Set `INGESTION_TRANSLATE_TO_RUSSIAN=false` to drop ~514 calls
(graph stays in source language).

---

## 4. Query data flow (`/api/v1/search/{local,global,drift,auto}`)

Architecture detail in `docs/SEARCH.md`; usage + tuning in
`docs/runbook/search-usage.md`.  All four modes share
`SearchRequest`/`SearchResponse` and run as Temporal workflows.  The
legacy ReAct/Self-RAG routes (`/search`, `/agent`, `/selfrag`,
`/legacy/agent`) and the monolithic `SearchWorkflow` were REMOVED in the
R7b cutover.

```
POST /api/v1/search/local ──► SearchOrchestratorWorkflow (plan-execute):
  plan_subquestions (small)
    → fan-out N× SubQueryRetrievalWorkflow in parallel, each running
        vector_search + graph_search + find_entity_by_name (+ auto-seeded
        bounded graph_walk)
    → merge/dedup by chunk_id
    → coverage gate (one extra sub-question on a named gap)
    → rerank (bge cross-encoder, top-N)
    → synthesize_answer (large tier, single call)
  ⇒ SearchResponse { answer, mode, sources[], documents[], latency_ms }

POST /api/v1/search/global ──► GlobalSearchWorkflow: map-reduce over the
  GDS-Leiden community summaries (small-tier MAP per community → large REDUCE).

POST /api/v1/search/drift  ──► local pass, then global expansion seeded with
  the local sources (documents[] = union of both).

POST /api/v1/search/auto   ──► route_query (small) classifies → dispatches to
  local / global / drift; fail-safe → local.

POST /api/v1/admin/communities/rebuild ──► offline CommunityBuildWorkflow
  (GDS Leiden + per-community summaries) on the kb-graph-build queue.

GET  /api/v1/documents/{doc_id} ──► stream the original uploaded file from MinIO.
```

### Russian-only output

The `synthesize_answer` activity
(`src/workflow/activities/synthesize_answer.py`) wraps the query with a
Russian-output instruction before synthesis, so the answer language
matches the graph normalisation regardless of source-language chunks.

---

## 5. Layer responsibilities

| Layer | Module | Responsibility |
|---|---|---|
| **API** | `src/api/main.py`, `src/api/routes/*.py` | HTTP surface, auth via `X-API-Key`, request validation. Routes are thin — business logic lives in retrieval modules. |
| **DI** | `src/di/providers.py` | Long-lived singletons. Two containers: API and worker. See section 6. |
| **Ingestion** | `src/ingestion/{pipeline,identifier_transform,translate_transform,embeddings,run}.py` | Parse → chunk → identifier-canon → translate-to-RU → vector index + KG. Orchestrated by Temporal (`DocumentIngestWorkflow`). |
| **Retrieval** | `src/retrieval/{vector_index,hybrid,atomic_tools,reranker,query_planner,llm,llm_semaphore,hf_offline,_common}.py` | Retrieval primitives (vector/graph atomic tools), bge reranker, query planner, LiteLLM tiers. Self-contained — no API dependencies. |
| **Graph** | `src/graph/{schema,store,index,retriever,communities,entity_resolution,canonical_linker,lightrag_extract,lightrag_parse,lightrag_prompts,merge}.py` | Entity/relation taxonomy, LightRAG extraction + parser, cross-chunk merge, entity resolution, GDS-Leiden communities, Neo4j wiring. |
| **Storage** | `src/storage/{postgres,chunk_repository,minio}.py` | Document-status table; doc-id keyed access to chunks (Milvus) + original files (MinIO). |
| **Observability** | `src/observability/trace.py` | Per-request `Trace` bound via ContextVar. `record_event(...)` collects tool/llm/refinement events. |
| **Models** | `src/models/search.py` | Pydantic shapes shared API ↔ services. |

---

## 6. Dependency Injection

`src/di/providers.py` uses Dishka.  Two containers:

### `CommonProvider` (both API + worker)

* `postgres: AsyncPostgres` — connection-per-call thin wrapper.
* `llm: LLM` — `OpenAILike` pointing at LiteLLM proxy.
* `embed_model: BaseEmbedding` — same proxy, embedding endpoint.

### `ApiProvider` (API only)

* `retriever: RetrieverProtocol` — `VectorIndexRetriever` over the
  Milvus index, `similarity_top_k=10`.
* `judge: JudgeProtocol` — `LLMJudge` for the legacy agentic loop.
* `synthesizer: BaseSynthesizer | SynthesizerProtocol` — LlamaIndex
  `COMPACT` synthesizer.
* `chunk_repository: ChunkRepository` (NEW) — wraps Milvus + Postgres
  for the agent's `get_chunks_by_doc_id` / `read_full_document` tools.
* `graph_retriever: GraphRetrieverProtocol | None` — attaches to the
  already-populated Neo4j store via PropertyGraphIndex.  Falls back
  to `None` if Neo4j is unreachable; all agent paths handle that
  natively.

`build_api_container()` / `build_worker_container()` produce these.

---

## 7. Observability

Every request bound to a search endpoint is wrapped in a
`trace_request(endpoint, query)` context manager from
`src/observability/trace.py`:

* `record_event("tool_call", payload={"tool_name": "..."})` —
  every retriever / graph / chunk-repo / synthesizer call.
* `record_event("llm_call", payload={"kind": "reasoning"})` —
  every ReAct or reflective LLM call.
* `record_event("refinement_round", ...)` — per Self-RAG iteration.
* `record_timed(name, ...)` — times a block, attaches duration.

`Trace.summary()` aggregates totals + tool breakdown — logged at
request end via loguru.  Contextvar-scoped so concurrent requests
stay isolated and async tasks inherit the trace automatically.

The R9 answer-quality eval (`tests/eval/answer_quality.py` +
`run_answer_eval.py`) is deterministic and offline — grades
endpoint responses by substring fact recall, entity recall,
citation precision, hallucination upper bound, and uncertainty
honesty.  Optional `--medical-sample N` flag (from
`tests/eval/medical_fixture.py`) adds N items from a 2 062-Q
medical benchmark.

---

## 8. Configuration

All settings flow through `src/config.py` (pydantic-settings)
which reads `.env`.  Per-subsystem namespaces:

| Prefix | Drives |
|---|---|
| `API_` | host/port, log level, upload dir, X-API-Key allow-list |
| `MILVUS_` | host/port, collection name, timeout, vector dim (must match embed model) |
| `NEO4J_` | URI, auth, database |
| `POSTGRES_` | DSN bits |
| `RABBITMQ_` | URL, timeout |
| `LITELLM_` | base_url, api_key, llm_model, embedding_model, embedding_dim, timeout, retries |
| `INGESTION_` | chunk size/overlap, cache_dir, **translate_to_russian**, **translation_concurrency** |
| `AGENT_` | max_iterations, max_refinements, top_k, enable_legacy_agent |
| `OPENAI_API_KEY` | propagated into the LiteLLM container |

Model swap procedure → `docs/MODELS.md`.

---

## 9. What's NOT in the architecture (yet)

* Hybrid retriever (BM25 + vector RRF) is implemented in
  `src.retrieval.hybrid` but not wired into DI — production BM25
  needs a separate docstore decision.
* Periodic graph deduplication / cross-document alias-merge
  (different Russian renderings of the same concept) — not yet.
* Multi-tenant data isolation — `department` field flows through
  metadata but no enforcement at retrieve time.
* Caching of agent tool results across requests.
* Streaming responses (SSE) on the search endpoints.
* Document-level summary (the `documents.summary` Postgres column
  is reserved but unused).

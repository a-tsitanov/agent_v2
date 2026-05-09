# Changelog

All notable changes to **kb-llamaindex** are recorded here. The project
is built incrementally per the plan in
`~/.claude/plans/hashed-rolling-llama.md` — one commit per stage, each
with a section in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
loosely (Added / Changed / Fixed / Notes per stage).

## [Stage 5] — 2026-05-09 — Hybrid retrieval (BM25 + vector + RRF)

### Added
- `src/retrieval/hybrid.py`:
  - `build_bm25_retriever(nodes, similarity_top_k)` — pure-Python
    BM25 over an in-memory node list.
  - `build_hybrid_retriever(vector_index, bm25_nodes, ...)` —
    `QueryFusionRetriever` (mode `reciprocal_rerank`) over the
    dense + BM25 retrievers.  ``num_queries=1`` disables built-in
    query expansion (the agent loop already covers expansion).
- `src/retrieval/reranker.py` — `build_reranker(model_name, top_n)`
  factory for `SentenceTransformerRerank`.  Default:
  `BAAI/bge-reranker-v2-m3`.  Heavy (~1 GB on first run), kept
  out of the default test path.
- `tests/test_retrieval/test_hybrid.py` — 3 tests: BM25 surfaces a
  keyword match, hybrid combines both retrievers, reranker factory
  importable.

### Notes
- `QueryFusionRetriever` resolves an LLM at construction even with
  expansion disabled — the factory now takes an explicit ``llm``
  arg so callers (and tests via `MockLLM`) avoid surprise OpenAI
  dependency.
- Agent module is **not modified** — Stage 5 swaps the retriever
  the caller passes into `agentic_search`.  This is the
  composable seam the plan promised.
- Suite total: 34 tests green.

## [Stage 4] — 2026-05-09 — Agentic loop (PRIORITY)

### Added
- `src/models/search.py` — Pydantic shapes (`SearchRequest`,
  `SearchResponse`, `SourceCitation`, `AgenticRoundStat`) mirroring
  enterprise-kb wire format.
- `src/retrieval/judge.py` — `LLMJudge` with the same JSON contract
  and defensive-fallback semantics as `enterprise-kb._judge_context`.
  Strips markdown fences, swallows JSON parse errors and LLM
  exceptions → `sufficient=True` with reason carrying the error
  text.
- `src/retrieval/agent.py` — `agentic_search()` async function:
  - Stub-friendly via `RetrieverProtocol`, `JudgeProtocol`,
    `SynthesizerProtocol`.
  - Per-round node dedup by `node.node_id`, delta tracking, early
    exit on barren follow-up rounds (round > 1 and `new_sources=0`).
  - `AgenticRoundStat` recorded per executed round, including
    skipped-judge entry on early-exit (`sufficient=None`,
    `reason="no new info"`).
  - Defensive: judge exceptions don't crash the loop.
  - Anti-loop: `follow_up == current_query` → break.
  - Final synthesis via `ResponseSynthesizer` over the *enriched*
    query (original + appended unique follow-ups) and *accumulated*
    nodes — matches enterprise-kb Stage F semantics.
- `tests/test_retrieval/test_agent.py` — 15 tests covering helpers
  (dedup, enriched query), end-to-end loop (1-round, 2-round,
  max_rounds, early-exit, follow-up loop guard, dedup across
  rounds, round_stats per round, judge defensive defaults), and
  `LLMJudge` direct unit tests (plain JSON, markdown fences,
  invalid JSON, LLM exception).

### Notes
- This is the project's **agentic-first** milestone — the agent
  loop works end-to-end on stub deps, hybrid retrieval / KG /
  canonicalisation will plug in without touching `agent.py`.
- Plan called for `AgentWorkflow`/Workflow primitives; opted for a
  plain async function instead — keeps the port from
  `enterprise-kb/agent_search.py` 1:1 readable, easier to reason
  about and benchmark. Migrating to Workflow primitives later is
  mechanical if needed.
- `hl_keywords` (Stage F in enterprise-kb) is intentionally absent
  here — Stage 4 has no KG, so there are no entity names to feed
  back. Stage 6 will introduce the graph-search tool and
  re-enable `hl_keywords`-style enrichment.
- Suite total: 31 tests green.

## [Stage 3] — 2026-05-09 — Vector index + basic query engine

### Added
- `src/retrieval/vector_index.py`:
  - `build_vector_store(overwrite=False)` — Milvus
    `BasePydanticVectorStore` from settings (cosine similarity).
  - `build_vector_index(store, embed_model)` — wraps any vector
    store into `VectorStoreIndex` (tests pass `SimpleVectorStore`
    in-memory).
  - `index_nodes(index, nodes)` — inserts pre-chunked nodes,
    returns count for diagnostics.
- `src/retrieval/query_engine.py`:
  - `build_basic_query_engine(index, llm, similarity_top_k=10)` —
    dense retriever + LLM synthesis. Stage 5 swaps the retriever
    underneath; this API stays stable.
- `src/retrieval/llm.py` — `build_llm()` factory using
  `OpenAILike` against the LiteLLM proxy.
- `src/ingestion/run.py` — CLI to ingest a directory end-to-end:
  read docs → pipeline → vector index. Flags:
  `--semantic`, `--overwrite-collection`, `--recursive/--no-recursive`.
- `tests/test_retrieval/test_vector_index.py` — 3 tests using
  `SimpleVectorStore` + `MockEmbedding` + `MockLLM`.

### Notes
- Live Milvus deliberately not exercised in unit tests — tests use
  the in-memory `SimpleVectorStore` so the suite stays fast and
  hermetic. Live Milvus is verified manually via
  `python -m src.ingestion.run`.
- Suite total: 16 tests green.

## [Stage 2] — 2026-05-09 — IngestionPipeline (parsing + chunking)

### Added
- `src/ingestion/embeddings.py` — `build_embedding_model()` factory
  returning `OpenAILikeEmbedding` wired to LiteLLM proxy.
- `src/ingestion/pipeline.py`:
  - `build_ingestion_pipeline(embed_model=None, semantic=False,
    cache_dir=None, extra_transformations=None)` — composes a
    LlamaIndex `IngestionPipeline`.
  - `read_documents(input_dir)` — `SimpleDirectoryReader` wrapper
    used by tests and (later) the worker.
  - Two splitter modes: `SentenceSplitter` (default, deterministic,
    no embed dep) and `SemanticSplitterNodeParser` (opt-in with
    `semantic=True` + embed_model).
  - Persistent disk cache via `IngestionCache` + `SimpleKVStore`.
  - `extra_transformations` hook reserved for Stage 7 (canonical
    identifier injection).
- `tests/test_ingestion/fixtures/sample.txt` — minimal multi-paragraph
  fixture.
- `tests/test_ingestion/test_pipeline.py` — 5 tests covering reader,
  sentence-splitter pipeline, semantic-splitter with `MockEmbedding`,
  cache persistence, and the `extra_transformations` hook.

### Notes
- Plan called for SemanticSplitter as default; downgraded to
  SentenceSplitter to avoid embedding round-trip per test run.
  Semantic mode is opt-in via the factory and exercised in tests
  with a `MockEmbedding`.
- Embedding *transformation* (i.e. attaching embeddings to nodes) is
  intentionally NOT in this pipeline yet — it gets bolted on in
  Stage 3 alongside the vector store, matching LlamaIndex's typical
  ingestion → vector-index split.
- Suite total: 13 tests green.

## [Stage 1] — 2026-05-09 — Minimal infra (Milvus + Postgres + LiteLLM)

### Added
- `docker-compose.yml` — etcd, minio, milvus 2.4.17, postgres 16,
  litellm proxy.  Neo4j + RabbitMQ deferred (Stage 6 / 8).
- `docker/litellm_config.yaml` — sample proxy config wiring LiteLLM
  to Ollama on the host (`host.docker.internal:11434`).  Replace
  with your upstream (OpenAI, Anthropic, Azure) for production.
- `scripts/start.sh` — `up`/`down`/`logs`/`ps` wrapper, prints
  service URLs after start.
- `scripts/setup_db.py` — idempotent Postgres `documents` table
  bootstrap + Milvus connectivity ping (collection lifecycle owned
  by `MilvusVectorStore` from Stage 3).
- `tests/test_scripts/test_setup_db.py` — DDL invariants, idempotency
  contract, callable signatures.

### Notes
- `documents` schema mirrors enterprise-kb:
  `id (UUID PK), path, department, doc_type, status, error, summary,
  created_at, updated_at`.  Status FSM:
  pending → processing → completed | failed.
- Milvus collection deliberately not provisioned in setup_db —
  LlamaIndex's `MilvusVectorStore` handles schema creation and is
  the source of truth for fields/dim.
- Suite total: 8 tests green.

## [Stage 0] — 2026-05-09 — Bootstrap

### Added
- `pyproject.toml` with uv-managed deps (Python 3.12, llama-index-core
  0.13.x, milvus/neo4j/openai-like/bm25/sbert-rerank connectors,
  FastAPI, Taskiq, dishka, pydantic-settings, loguru, phonenumbers,
  dateparser).  `postal` kept as `[postal]` extra — requires
  system libpostal, falls back to rule-based normalisation.
- Directory tree mirroring `enterprise-kb`:
  `src/{api/routes,di,ingestion,retrieval,graph,models,storage,utils}`,
  `tests/{test_api,test_ingestion,test_retrieval,test_scripts,test_storage,eval/golden_identifiers}`,
  `docs/{plans,specs}`, `scripts/`, `docker/`.
  `src/graph/` is the LlamaIndex-specific addition (PropertyGraphIndex
  in Stage 6).
- `src/config.py` — nested pydantic-settings (Api, Milvus, Neo4j,
  Postgres, LiteLLM, RabbitMQ, Ingestion, Agent).  Composed
  `Settings` with cached_property accessors.
- `src/utils/logging.py` — loguru bootstrap, JSON or human format.
- `tests/test_config.py` — 5 smoke tests covering imports, settings
  defaults, CSV parsing for API keys, Postgres DSN assembly, logging
  configuration.
- `.env.example`, `.gitignore`, `.python-version` (3.12), `README.md`
  with quickstart and stage status.

### Notes
- Initial `llama-index-vector-stores-milvus<0.7` constraint conflicted
  with `llama-index-core>=0.13` (milvus 0.5/0.6 require core 0.12).
  Relaxed sub-package upper bounds — uv resolved into core 0.13.6 +
  current connector versions.
- Verified key imports: `Workflow`, `IngestionPipeline`,
  `MilvusVectorStore`, `Neo4jPropertyGraphStore`, `OpenAILike`,
  `BM25Retriever`.
- `pytest` — 5/5 passing.

### Decision: directory tree
- Mirrors `enterprise-kb` 1:1 with one addition: `src/graph/` for
  PropertyGraphIndex code (LightRAG buried KG inside its own engine,
  LlamaIndex exposes graph as a separate index family — separate
  module is cleaner).
- `src/di/` reserved for dishka providers (Stage 8).
- `src/storage/` reserved for low-level Milvus/Neo4j/PG client
  wrappers if needed (Stage 1+).

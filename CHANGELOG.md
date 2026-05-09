# Changelog

All notable changes to **kb-llamaindex** are recorded here. The project
is built incrementally per the plan in
`~/.claude/plans/hashed-rolling-llama.md` — one commit per stage, each
with a section in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
loosely (Added / Changed / Fixed / Notes per stage).

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

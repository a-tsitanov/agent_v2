# Architecture

## Overview

`kb-llamaindex` is a multi-store RAG service: documents flow
through an async ingestion pipeline into a vector index (Milvus)
and a property graph (Neo4j); queries hit three parallel
endpoints with increasing sophistication.

```
            ┌─────────────────┐
            │  Document       │
            │  upload         │
            └────────┬────────┘
                     ▼
            ┌─────────────────┐
            │ POST /ingest    │  → file saved → PG row pending
            │ (FastAPI route) │  → process_document.kiq()
            └────────┬────────┘
                     ▼ RabbitMQ
            ┌─────────────────┐
            │ Taskiq worker   │
            │ process_document│
            └────────┬────────┘
                     │
                     ▼
   ┌─────────────────────────────────────┐
   │ IngestionPipeline                   │
   │   1. SimpleDirectoryReader          │
   │   2. SentenceSplitter               │
   │   3. IdentifierCanonicalization     │
   │      Transform                      │
   └────┬─────────────────────────┬──────┘
        │                         │
        ▼                         ▼
   ┌─────────┐             ┌────────────────┐
   │ Milvus  │             │ Neo4j          │
   │ vector  │             │ property graph │
   │ index   │             │  - canonical   │
   │         │             │    entities    │
   │         │             │  - KG extractor│
   │         │             │  - description │
   │         │             │    enricher    │
   └─────────┘             └────────────────┘
```

## Query side — three endpoints

```
                          ┌──────────────────┐
POST /api/v1/search ─────►│ HybridRetriever  │──► chunks
                          └────────┬─────────┘
                                   ▼
                          ┌──────────────────┐
                          │ Synthesizer (LLM)│──► answer
                          └──────────────────┘

POST /api/v1/agent ──► ReAct agent loop (R7):
  ┌──────────────────────────────────────────┐
  │ LLM (qwen3:8b) with function calls       │
  │   tools:                                 │
  │   - vector_search(query)                 │
  │   - graph_search(query)                  │
  │   - find_entity_by_id(name)              │
  │   - find_neighbours(name)                │
  │   - filter_by_metadata(...)              │
  │   - submit_answer(query, source_ids)     │◄── triggers Synthesizer
  └──────────────────────────────────────────┘

POST /api/v1/selfrag ──► same ReAct loop + Reflective synth (R8):
  submit_answer triggers reflective_synthesize, which:
    1. drafts answer with inline markers:
         [NEED:topic] → trigger retrieve+redraft
         [SUPPORTED:chunk_id] → claim with citation
         [UNCERTAIN:why] → known gap, no hallucination
    2. parses, retrieves for NEED markers, redrafts
    3. terminates after max_refinements (default 3)

POST /api/v1/legacy/agent  ──► judge-based loop (R10 baseline):
  Only mounted when AGENT_ENABLE_LEGACY_AGENT=true.  Kept as a
  comparative baseline for the answer-quality eval —
  `run_answer_eval.py --include-legacy` exercises this route to
  validate that R7+R8 outperform the legacy judge across
  single-fact, multi-hop, and summary categories.
```

## Observability

Every request bound to one of the three (or four) endpoints is
wrapped in a `trace_request(endpoint, query)` context manager from
`src/observability/trace.py`.  Inside the trace:

* `record_event("tool_call", payload={"tool_name": "..."})` fires
  for every retriever / graph / synthesizer call.
* `record_event("llm_call", payload={"kind": "reasoning"})` fires
  for every ReAct or refinement LLM call.
* `record_event("refinement_round", ...)` fires per Self-RAG
  refinement loop iteration.
* `record_timed(...)` is the timing variant — wraps a block and
  records its wall duration as the event's `duration_ms`.

`Trace.summary()` aggregates totals + tool breakdown.  The
collector is contextvar-scoped so concurrent requests stay
isolated and async tasks inherit the trace automatically.

The R9 answer-quality eval (`tests/eval/answer_quality.py` +
`run_answer_eval.py`) is deterministic and offline — it grades
endpoint responses by substring fact recall, entity recall,
citation precision, hallucination upper bound, and
uncertainty honesty.  Run it across `/search`, `/agent`,
`/selfrag` (and optionally `--include-legacy`) to compare
strategies on the project's 15 golden Q&A cases (5 per
doc_type).

## Layer responsibilities

| Layer | Module | Responsibility |
|---|---|---|
| **API** | `src/api/main.py`, `src/api/routes/*.py` | HTTP surface, auth, request validation. Routes thin — business logic lives in retrieval modules. |
| **DI** | `src/di/providers.py` | Long-lived singletons (LLM, embed, retriever, judge, synthesizer, graph retriever). Tests override providers for stubs. |
| **Ingestion** | `src/ingestion/{pipeline,tasks,identifier_transform,run}.py` | Parse → chunk → identifier canon → vector index + KG. Async via taskiq. |
| **Retrieval** | `src/retrieval/{vector_index,hybrid,query_engine,agent,react_agent,reflective_synth,judge,llm}.py` | Plain / agentic / reflective search. Self-contained — no API dependencies. |
| **Graph** | `src/graph/{schema,store,index,retriever,enrich}.py` | Universal entity / relation taxonomy, KG extractors, Neo4j wiring, description enrichment. |
| **Storage** | `src/storage/postgres.py` | Document-status table operations. |
| **Models** | `src/models/search.py` | Pydantic shapes shared API ↔ services. |

## Data shapes flowing between layers

### Ingestion output (per document)

* **Milvus**: N vector records (one per chunk), each carrying
  text + metadata (`doc_id`, `file_path`, `canonical_identifiers`).
* **Neo4j**:
  * EntityNodes for every canonical identifier (`+74952345678`
    PhoneNumber, `7707083893` INN, ...) with `description` =
    text snippet around the original mention.
  * EntityNodes for every LLM-extracted entity (Person,
    Organization, Concept, Topic, Issue, ...) with `description` =
    LLM-generated 10-30 word summary from `EntityDescriptionEnricher`.
  * Relations between them (typed when schema mode worked,
    free-text predicates when simple mode is used).
* **Postgres**: one row per uploaded file with `status` flowing
  `pending` → `processing` → `completed` / `failed`.

### Search response (`SearchResponse`)

* `query`, `answer`, `mode`, `latency_ms` — universal.
* `sources: [SourceCitation]` — chunks that contributed.
* `agentic_step_stats: [AgenticStepStat]` — set by `/agent` and
  `/selfrag`: per-step tool call name + args + reasoning excerpt.
* `answer_detail: ReflectiveAnswerDetail` — set by `/selfrag`:
  per-claim citations and uncertainty list.
* `agentic_rounds`, `follow_up_queries`, `agentic_round_stats` —
  set by the legacy judge-based `agentic_search` (kept until
  R10 retirement decision).

## Configuration

All settings flow through `src/config.py` (pydantic-settings)
which reads `.env`.  Per-subsystem namespaces with `LITELLM_`,
`MILVUS_`, `NEO4J_`, `POSTGRES_`, `RABBITMQ_`, `INGESTION_`,
`AGENT_`, `API_` prefixes.  See `.env.example` for the full
list.

Model swap procedure → `docs/MODELS.md`.

## Deployment

Local: `bash scripts/start.sh` brings up Docker Compose
(etcd, minio, milvus, postgres, neo4j, rabbitmq, litellm).
Ollama runs on the host; LiteLLM container reaches it via
`host.docker.internal`.  Worker + API run as separate
processes; `uv run uvicorn` + `uv run taskiq worker`.

Production: same compose, but with `master_key` auth on
LiteLLM (requires a backing database — see
`docker/litellm_config.yaml` for hooks), real credentials in
`.env`, Neo4j with auth, RabbitMQ behind VPC, etc.  None of
those bits are baked in — they're operational decisions left
to deployment time.

## What's NOT in the architecture (yet)

* Hybrid retriever wiring in DI — BM25 docstore decision
  pending.
* Periodic graph deduplication / description consolidation —
  one-shot merge job exists, but no daemon yet.
* Multi-tenant data isolation — `department` field flows
  through metadata but no enforcement at retrieve time.
* Caching of agent tool results across requests.
* Streaming responses (SSE) on the search endpoints.

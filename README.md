# kb-llamaindex

GraphRAG knowledge-base service built on **LlamaIndex 0.13+**.  Durable
Temporal ingest turns heterogeneous documents into a Neo4j property
graph + Milvus vectors (MinIO claim-check, Postgres statuses, RabbitMQ
backlog); retrieval comes as four search modes
(`/api/v1/search/{local,global,drift,auto}`), a graph-analytics layer
(`/api/v1/analyze` over a 42-primitive catalog), and three MCP servers
for agent clients.  All LLM traffic goes through a LiteLLM proxy —
local Ollama models by default (gemma4 QAT tiers + `nomic-embed-text`),
OpenAI as the canonical cloud profile; per-role tiers in
`docs/MODELS.md`.

## Status

The R1–R10 refactor and the **R7b cutover are done**: the only search
surface is `/api/v1/search/{local,global,drift,auto}` (legacy
`/search`, `/agent`, `/selfrag` removed).  Now in the
production-hardening phase.  Recent additions: RabbitMQ ingest backlog
(default backend) + multi-queue, per-process **LLMPool** (K+N
concurrency model), `leidenalg` community backend (Leiden off the
Neo4j JVM), the **analytics layer** (Waves 0–3: primitive catalog,
offline materialization, Arc-2 monitoring — see
[`docs/runbook/graph-analytics.md`](docs/runbook/graph-analytics.md)),
the continuous wiki editor, and the `tg_ingest` backfill harness.

Full history: `CHANGELOG.md`; per-sprint map: «Недавно выпущенное» in
[`docs/runbook/README.md`](docs/runbook/README.md).

## Prerequisites

- Docker Compose stack (etcd / minio / milvus / neo4j / postgres /
  temporal / rabbitmq / redis / litellm + Prometheus / Grafana /
  Temporal UI).
- **LLM access:** canonical profile is **OpenAI** (`text-embedding-3-small`
  + `gpt-4o-mini`).  Ollama is opt-in — uncomment the Ollama block in
  `.env.example` (`gemma4:e4b` / QAT tiers `gemma4:{e2b,e4b}-it-qat` +
  `nomic-embed-text`, `MILVUS_DIM=768`) and pull the models separately.
  LiteLLM proxy inside Docker reaches Ollama via
  `host.docker.internal:11434` — see `docker/litellm_config.yaml`.
- Python 3.12.

## Quick start

```bash
cd kb-llamaindex
cp .env.example .env
uv sync --extra dev

make up                                # backends (healthy) + schema init

# Ingest a directory
uv run python -m src.ingestion.run ./tests/test_ingestion/fixtures/

# Optional ingest gates (both opt-in, off by default):
#  - Classifier (CLASSIFIER_ENABLED): a classify_document step between
#    fetch and parse drops junk via deterministic rules + an LLM gate;
#    dropped docs end in a `skipped` status.  force=true on /ingest
#    bypasses the rules and forces the document through.
#  - Admission control (INGEST_ADMISSION_ENABLED,
#    INGEST_ADMISSION_MAX_INFLIGHT): a singleton scheduler admits at most
#    K documents at once, each run to completion FIFO.

# Start the app on the host:
uv run python -m src.workflow.worker &
uv run uvicorn src.api.main:app --port 8000 &

# Search — four modes: local / global / drift / auto
curl -X POST localhost:8000/api/v1/search/local \
  -H "X-API-Key: dev-local-key" -H "Content-Type: application/json" \
  -d '{"query": "..."}'

# Answer templates: shape the synthesized answer with a named template
# (prompts/answer_templates/<name>.md) or an inline string; empty -> default.
#   -d '{"query": "...", "answer_template": "executive-summary"}'

# Graph analytics — plan→compute→synthesize over a 42-primitive catalog.
# Numbers live in provenance.steps[].rows (answer is an LLM gloss):
curl -X POST localhost:8000/api/v1/analyze \
  -H "X-API-Key: dev-local-key" -H "Content-Type: application/json" \
  -d '{"query": "Сколько сущностей каждого типа?"}'

# Graph admin (GDS-backed, POST, X-API-Key):
#   /admin/graph/stats | /pagerank | /personalized-pagerank | /components
#   | /shortest-path | /materialize   <- run materialize after bulk ingest
#   (precomputes centrality / link-prediction / risk for /analyze)

# Backfill Telegram channels into the ingest queue (see scripts/tg_ingest.py):
#   TG_API_ID=… TG_API_HASH=… uv run python -m scripts.tg_ingest \
#     --channels @foo --limit 50 --api-key dev-local-key

# Tests
uv run pytest -q

# Tag an ingest batch for analytics (Grafana compare-by-version):
curl -F file=@doc.txt -H "X-API-Key: $API_KEY" \
     -H "X-Version-Tag: gemma4-e2b-baseline" \
     localhost:8000/api/v1/ingest

# Per-role LLM swap (e.g. cheaper judge model for high-volume ER calls):
export LITELLM_JUDGE_MODEL=gemma4:e2b-it-qat   # only merge_and_resolve
export LITELLM_EXTRACTION_MODEL=gemma4:e4b-it-qat  # extract_kg + translator
export LITELLM_SEARCH_MODEL=gemma4:e4b-it-qat  # search/analyze workflows
# (restart worker + API to pick up env)
# ingest_metrics rows carry the per-activity model — see docs/MODELS.md.
```

### Production deployment

```bash
# Whole app (api + worker + ingest-consumer + mcp) + backends + rabbitmq
# + redis in one compose, EXCLUDING litellm/ollama — point at an
# external LiteLLM (see .env.prod.example):
export LITELLM_BASE_URL=https://litellm.internal:4000
docker compose -f docker-compose.prod.yml up -d
# RabbitMQ + ingest-consumer start by default (INGEST_QUEUE_BACKEND=rabbitmq).
# Wikibase is opt-in behind a profile: add --profile wikibase.
```

### Observability

- **Grafana** — http://localhost:3001 (admin/admin).  Three
  dashboards under `kb-llamaindex` folder: Ingest Overview (live),
  Version compare, Run drill-down.  See `docs/runbook/analytics.md`.
- **Prometheus** — http://localhost:9092 (scrapes worker on :9090).
- **Temporal UI** — http://localhost:8080 (workflow timeline).
- **RabbitMQ management** — http://localhost:15672 (guest/guest; ingest
  backlog queues).

### Documentation map

| Where to start | What you'll find |
|---|---|
| [`docs/QUICKSTART.md`](docs/QUICKSTART.md) | **Run it in 5 steps** — bring up the stack, init schemas, start worker + API, smoke-test ingest/search; key env params + the feature-enable matrix (native ER, communities, wiki) |
| [`docs/CONCEPTS.md`](docs/CONCEPTS.md) | **Start here to understand the service** — every technique from scratch (Temporal, claim-check, LLMPool, identifier canonicalization, entity resolution, HNSW, **Leiden** communities, local/global/drift search, Wikibase anchor, wiki-editor) — what it is, how the algorithm works, why we chose it, where it lives in code |
| [`docs/adr/README.md`](docs/adr/README.md) | **Architecture Decision Records** — the *why* of each major choice in record form (Context/Decision/Consequences/Alternatives) |
| [`docs/FEATURES.md`](docs/FEATURES.md) | **Every feature** — what/why/how + env, with a deep dive on the new scale/GraphRAG work (native-vector ER, conversation history, hierarchical communities + dynamic selection, dual walk-seed, drift fallback) |
| [`docs/INGEST.md`](docs/INGEST.md) | **Ingest pipeline** — blocks/activities/queues/staging, with Mermaid + D2 diagrams (vector half + graph-build child + identifiers + multimodel) |
| [`docs/SEARCH-FLOW.md`](docs/SEARCH-FLOW.md) | **Search flow** — local/global/drift/auto + retrieval tools + community selection, with Mermaid + D2 diagrams |
| [`docs/runbook/README.md`](docs/runbook/README.md) | Index of operator runbooks (**mcp**, search, multimodel, analytics, graph-analytics, wikibase, wiki-editor, er-native-vector-knn) |
| [`docs/runbook/mcp.md`](docs/runbook/mcp.md) | Three MCP servers — MCP-1 `kb_search` (Temporal-backed, high-level), MCP-2 atomic retrieval tools (in-process, GPU-protected via BoundedLLM semaphore), and MCP-3 exact-statistics tools (`stat_indicators_search` / `stat_series` / `stat_align`, plain Postgres, no LLM).  Stdio + HTTP/SSE transports.  Configuring OpenWebUI / Claude Desktop / Cursor / Continue. |
| [`docs/SEARCH.md`](docs/SEARCH.md) | Deep search reference — modes (local/global/drift/auto), orchestrator + coverage loop, dynamic community selection, rerank, knob table (companion to SEARCH-FLOW.md diagrams) |
| [`docs/runbook/search-usage.md`](docs/runbook/search-usage.md) | Operator search usage — request shapes, conversation history, source download, wiki rebuild |
| [`docs/runbook/multimodel.md`](docs/runbook/multimodel.md) | Per-role LLM (extraction/judge/search) + child workflow + per-activity model in `ingest_metrics`.  Detailed reading guide with code excerpts. |
| [`docs/runbook/analytics.md`](docs/runbook/analytics.md) | Grafana / Prometheus / `ingest_metrics` schema + version_tag mechanics |
| [`docs/runbook/graph-analytics.md`](docs/runbook/graph-analytics.md) | **Graph analytics** — `POST /api/v1/analyze` (plan→compute→synthesize over a 42-primitive catalog), the provenance-is-ground-truth rule, offline materialization (`/admin/graph/materialize`), Arc-2 monitoring (`MONITOR_*`), MCP surface (`kb_analyze` + GDS tools) |
| [`docs/ANALYTICS-GUIDE.md`](docs/ANALYTICS-GUIDE.md) | **Analyst's theory guide** — the network science behind every analytics tool (centralities, Leiden/modularity + resolution limit, link prediction, burst detection, composite risk as MCDA), with live examples from the current graph and an interpretation-pitfalls checklist |
| [`docs/runbook/wikibase.md`](docs/runbook/wikibase.md) | Wikibase population + SPARQL examples |
| [`docs/runbook/wiki-editor.md`](docs/runbook/wiki-editor.md) | Continuous per-entity MediaWiki article editor from the Neo4j graph (dirty-mark + scheduled sweep, bot-section rewrite, anti-drift, citations) |
| [`docs/MODELS.md`](docs/MODELS.md) | Per-role model guidance + escalation table |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | **Top-level system map** — components, data stores, ingest vs search paths, durable execution, degradation, observability (links out to the deep docs) |
| [`docs/diagrams/system_architecture.svg`](docs/diagrams/system_architecture.svg) | Rendered container-view diagram (source `docs/diagrams/system_architecture.d2`) |

## Directory layout

```
kb-llamaindex/
├── pyproject.toml
├── .env.example                 # dev profile (.env.prod.example for prod)
├── docker-compose.yml           # dev: backends + litellm + observability
├── docker-compose.prod.yml      # prod: whole app, external LiteLLM
├── src/
│   ├── config.py                # nested pydantic-settings
│   ├── utils/logging.py         # loguru bootstrap
│   ├── api/
│   │   ├── main.py              # FastAPI app, lifespan
│   │   └── routes/              # search_v2 (/search/*), analyze, ingest,
│   │                            #   documents, admin, graph_admin, health
│   ├── di/providers.py          # dishka wiring
│   ├── ingestion/               # IngestionPipeline + identifier canon
│   ├── ingest_queue/            # RabbitMQ backlog: publisher + consumer
│   ├── retrieval/               # vector index, atomic tools, LLMPool
│   ├── graph/                   # PropertyGraphIndex, retriever, communities
│   ├── analytics/               # 42-primitive catalog, planner, materialize,
│   │                            #   risk, events, provenance
│   ├── workflow/                # Temporal: ingest, search, analytics,
│   │                            #   wiki, monitor + worker launcher
│   ├── mcp/                     # MCP-1 (search/analyze) + MCP-2 (atomic+GDS)
│   │                            #   + MCP-3 (exact stats, no LLM)
│   ├── observability/           # metrics, role map, litellm model validator
│   ├── models/                  # Pydantic request/response shapes
│   └── storage/                 # Postgres clients, chunk repository
├── tests/                       # unit + eval suites
├── scripts/                     # setup_db, tg_ingest, backfills, wipe_db
├── docker/                      # LiteLLM config
├── infra/                       # Prometheus / Grafana provisioning
└── docs/                        # architecture, runbooks, ADRs, bruno
```

## Domain

The system is built for a heterogeneous corpus:

- Analytical reports (long-form prose with concepts, metrics,
  findings).
- Email correspondence (`.eml` with headers + threaded body).
- Support call transcripts (speaker-turns, issues, resolutions).

EntityType taxonomy (set in R3) is universal — `Person`,
`Organization`, `Concept`, `Metric`, `Topic`, `Issue`,
`Resolution`, `EventOrAction`, `Product`, `Document` — plus 19
identifier types detected deterministically by the
canonicalisation transform when they appear:

- **Business / financial:** `PhoneNumber`, `Email`, `INN`,
  `OGRN`, `BIC`, `SNILS`, `ContractNumber`, `PostalAddress`,
  `DocumentDate`, `Amount`.
- **Digital identity:** `URL`, `Domain`, `TelegramHandle`,
  `VKProfile`, `TwitterHandle`, `InstagramHandle`,
  `LinkedInProfile`, `YouTubeChannel`, `GitHubProfile`, `UUID`.
- **Device / hardware:** `IMEI` (Luhn-validated), `MACAddress`,
  `LicensePlate` (Russian Cyrillic-format directly, plus
  country-agnostic generic plates when a context phrase like
  "license plate" / "гос. номер" / "vehicle reg. number" precedes
  the token), `VIN` (mod-11 checksum).

Detectors apply checksums where the spec defines one (Luhn for
IMEI, mod-11 for VIN, the SNILS algorithm, INN/OGRN/BIC checks)
so false positives stay out of the graph.

# kb-llamaindex

Production-bound RAG service built on **LlamaIndex 0.13+** with three
parallel search endpoints — plain hybrid retrieve, ReAct agent, and
Self-RAG-style reflective synthesis — running on local Ollama via
LiteLLM proxy.

Recommended model: **`qwen3:8b`** (reliable tool calling + structured
output).  Escalation path to `qwen3:14b` / `32b` if quality on your
corpus demands it — see `docs/MODELS.md` (created by R6).

Plan: `~/.claude/plans/hashed-rolling-llama.md`.

## Status

Prototype build (9 stages) shipped 2026-05-09 and live-tested.  Now
in **refactor + production-architecture phase** (stages R1-R10):

- **R1 — Model migration to qwen3:8b** — current.
- R2 — Function calling + structured output.
- R3 — Universal entity types + rich entity descriptions in graph.
- R4 — DI hygiene + split `/search` into 3 endpoints
  (`/search`, `/agent`, `/selfrag`).
- R5 — Test coverage (≥115 green).
- R6 — `docs/MODELS.md`, `docs/ARCHITECTURE.md`.
- R7 — ReAct agent (`POST /api/v1/agent`).
- R8 — Reflective synthesis (`POST /api/v1/selfrag`).
- R9 — Observability + answer-quality eval over multi-domain
  golden Q&A.
- R10 — Decommission legacy judge-based path.

History of the initial 9-stage build is in `CHANGELOG.md`.

## Prerequisites

- Docker Compose stack (etcd / minio / milvus / postgres / neo4j /
  rabbitmq / litellm).
- **Ollama running on the host** with required models pulled:
  ```bash
  ollama pull qwen3:8b
  ollama pull nomic-embed-text
  ```
  LiteLLM proxy inside Docker reaches Ollama via
  `host.docker.internal:11434` — see `docker/litellm_config.yaml`.
- Python 3.12.

## Quick start

```bash
cd kb-llamaindex
cp .env.example .env
uv sync --extra dev

bash scripts/start.sh                  # bring up docker stack
uv run python -m scripts.setup_db      # init PG schema + Milvus ping

# Ingest a directory
uv run python -m src.ingestion.run ./tests/test_ingestion/fixtures/

# Serve API + worker
uv run uvicorn src.api.main:app --port 8000 &
uv run taskiq worker src.ingestion.tasks:broker --workers 1 &

# Search (three endpoints land in R7-R8; until then only /search works)
curl -X POST localhost:8000/api/v1/search \
  -H "X-API-Key: dev-local-key" -H "Content-Type: application/json" \
  -d '{"query": "..."}'

# Tests
uv run pytest -q
```

## Directory layout

```
kb-llamaindex/
├── pyproject.toml
├── .env.example
├── src/
│   ├── config.py                # nested pydantic-settings
│   ├── utils/logging.py         # loguru bootstrap
│   ├── api/
│   │   ├── main.py              # FastAPI app, lifespan
│   │   └── routes/
│   │       ├── search.py        # /api/v1/search (plain hybrid)
│   │       ├── agent.py         # /api/v1/agent (ReAct, R7)
│   │       ├── selfrag.py       # /api/v1/selfrag (Reflective, R8)
│   │       └── ingest.py        # /api/v1/ingest
│   ├── di/providers.py          # dishka wiring
│   ├── ingestion/               # IngestionPipeline + identifier canon
│   ├── retrieval/               # vector + hybrid + agent + judge
│   ├── graph/                   # PropertyGraphIndex + schema
│   ├── models/                  # Pydantic request/response shapes
│   └── storage/postgres.py      # documents-table client
├── tests/                       # unit + eval suites
├── scripts/                     # start, setup_db, ingest, smoke, diag
├── docker/                      # compose + LiteLLM config
└── docs/                        # ARCHITECTURE, MODELS (created by R6)
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

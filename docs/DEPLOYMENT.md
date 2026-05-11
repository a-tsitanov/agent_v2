# Deployment Guide

Step-by-step from a clean machine to a running `kb-llamaindex`
stack.  Covers local dev (macOS / Linux), staging, and the
operational knobs needed to move toward production.

For the system architecture, see `docs/ARCHITECTURE.md`.

---

## 1. Prerequisites

| Tool | Version | Why |
|---|---|---|
| **Docker + Compose** | 24+ | All four backing stores run in containers |
| **Python** | 3.11 or 3.12 | API + worker |
| **uv** | 0.4+ | Project package manager (`uv run ...`) |
| **OpenAI API key** | — | LiteLLM upstream (gpt-4o-mini + text-embedding-3-small) |

Optional (only if running a local model instead of OpenAI):

| Tool | Version | Why |
|---|---|---|
| **Ollama** | 0.5+ | Local LLM (`qwen3:8b`) + embeddings (`nomic-embed-text`) |

---

## 2. Clone and install

```bash
git clone <repo-url> kb-llamaindex
cd kb-llamaindex

# uv reads pyproject.toml + uv.lock, creates .venv automatically.
uv sync
```

Verify:

```bash
uv run python -c "import llama_index, fastapi, taskiq; print('ok')"
```

---

## 3. Configure environment

Copy the example file and edit values:

```bash
cp .env.example .env
$EDITOR .env
```

Critical knobs:

```env
# ── OpenAI upstream (REQUIRED if litellm_config.yaml points at openai/*)
OPENAI_API_KEY=sk-...

# ── API auth (rotate per environment)
API_KEYS=dev-local-key

# ── LiteLLM proxy → OpenAI (default).  Embedding dim must match
# the model: 1536 for text-embedding-3-small, 3072 for -3-large.
LITELLM_LLM_MODEL=gpt-4o-mini
LITELLM_EMBEDDING_MODEL=text-embedding-3-small
LITELLM_EMBEDDING_DIM=1536
MILVUS_DIM=1536          # MUST match LITELLM_EMBEDDING_DIM

# ── Russian normalisation of the knowledge graph (set false to
# skip the ~+30% LLM cost on ingest; graph stays in source language)
INGESTION_TRANSLATE_TO_RUSSIAN=true
INGESTION_TRANSLATION_CONCURRENCY=4

# ── Off by default — gates the /api/v1/legacy/agent route
AGENT_ENABLE_LEGACY_AGENT=false
```

Switching to a local model setup → see `docs/MODELS.md`.

---

## 4. Bring up the storage stack

```bash
docker compose up -d
```

This starts **7 containers**:

| Container | Port | Purpose |
|---|---|---|
| `etcd` | — | Milvus metadata |
| `minio` | 9000, 9001 | Milvus object store |
| `milvus` | 19530, 9091 | Vector index |
| `postgres` | 5432 | Job-status table |
| `neo4j` | 7474 (web), 7687 (bolt) | Property graph |
| `rabbitmq` | 5672, 15672 (mgmt) | Task broker |
| `litellm` | 4000 | LLM gateway (reads `OPENAI_API_KEY`) |

Wait for everything to become healthy:

```bash
docker compose ps
# All STATUS columns should show "healthy"
```

Manual probe:

```bash
curl -fsS http://localhost:4000/health/liveliness   # LiteLLM
curl -fsS http://localhost:19530/healthz            # Milvus
docker exec kb-llamaindex-postgres-1 pg_isready -U postgres
```

---

## 5. Initialise schemas

`scripts/setup_db.py` is idempotent — safe to re-run.

```bash
uv run python -m scripts.setup_db
```

What it does:
* Creates `documents` table + status / department indexes in Postgres.
* Pings Milvus (collection is created lazily by `MilvusVectorStore`
  on first insert).
* Neo4j needs no schema — labels and indexes are emitted at insert
  time by `PropertyGraphIndex`.
* RabbitMQ queue is declared by the worker on startup.

---

## 6. Smoke-test the LLM gateway

Confirm the LiteLLM proxy reaches OpenAI with the key you pasted:

```bash
curl -fsS http://localhost:4000/v1/chat/completions \
  -H "Authorization: Bearer sk-litellm-stub" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Reply with one word: OK"}],"max_tokens":10}'

curl -fsS http://localhost:4000/v1/embeddings \
  -H "Authorization: Bearer sk-litellm-stub" \
  -H "Content-Type: application/json" \
  -d '{"model":"text-embedding-3-small","input":"smoke test"}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('dim:', len(d['data'][0]['embedding']))"
```

Expected: `OK` reply and `dim: 1536`.  If you see 401 — `OPENAI_API_KEY`
is wrong / missing.  If you see "connection refused" — the
litellm container is down (`docker compose logs litellm`).

---

## 7. Start the API + worker

Two long-lived processes.  Run each in its own terminal (or use
`tmux` / `systemd` in production):

```bash
# Terminal 1 — API
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — ingestion worker
uv run taskiq worker src.ingestion.tasks:broker --workers 1
```

Sanity:

```bash
curl -fsS http://localhost:8000/health        # {"status":"ok"}
```

Worker should log `Listening started.` within ~5 seconds.

> **Note**: the API uses `--reload` for dev convenience.  Disable
> in production (`gunicorn -k uvicorn.workers.UvicornWorker`
> with multiple workers; the `lifespan` hook starts the taskiq
> client side automatically).

---

## 8. Run a first ingest

Two ways:

### 8a. Through the API (curl)

```bash
curl -fsS -X POST http://localhost:8000/api/v1/ingest \
  -H "X-API-Key: dev-local-key" \
  -F "file=@/path/to/document.txt;type=text/plain" \
  -F "department=demo"

# → {"job_id": "abc-...uuid..."}
```

Poll status:

```bash
curl -fsS http://localhost:8000/api/v1/ingest/<job_id> \
  -H "X-API-Key: dev-local-key"
# → {"status": "pending" → "processing" → "completed"}
```

### 8b. Through the helper script (preferred for big files)

```bash
uv run python -m scripts.enqueue_ingest /path/to/document.txt
```

The script copies the file to `API_UPLOAD_DIR`, inserts the
Postgres row, fires the taskiq task, and polls until completion.
Useful when the API isn't running.

For the test medical corpus:

```bash
uv run python -m scripts.ingest_medical
```

Converts `tests/eval/corpora/medical/medical.json` → `medical_corpus.txt`,
uploads via the API, polls until done.

---

## 9. Smoke-test search

```bash
# Plain hybrid retrieve + single synth
curl -fsS -X POST http://localhost:8000/api/v1/search \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?"}' \
  | python3 -m json.tool

# ReAct agent
curl -fsS -X POST http://localhost:8000/api/v1/agent \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?"}' \
  | python3 -m json.tool

# Reflective Self-RAG
curl -fsS -X POST http://localhost:8000/api/v1/selfrag \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?"}' \
  | python3 -m json.tool
```

Expected: Russian answer + `sources[].content` containing the
original-language chunk text.  Latency: ~5-20 s on `/search`,
~20-90 s on `/agent`, ~30-120 s on `/selfrag` (model-bound).

---

## 10. Run the test suite

```bash
uv run pytest -q
```

All ~240 tests are offline (stub LLM, stub Milvus / Neo4j) —
safe to run in CI without any backing services.

For the deterministic answer-quality eval (also offline):

```bash
uv run pytest tests/eval/ -q
```

For live end-to-end eval against the actual API:

```bash
uv run python -m tests.eval.run_answer_eval \
  --no-golden --medical-sample 5 --endpoints search,agent \
  --search-timeout 180 --agentic-timeout 900
```

---

## 11. Reset / wipe operations

### Full reset (nuclear)

```bash
uv run python -m scripts.wipe_db --yes
```

Runs:
* `TRUNCATE documents`
* drop Milvus collection
* `MATCH (n) DETACH DELETE n` + drop non-constraint indexes in Neo4j
* `rm -rf $API_UPLOAD_DIR` and `INGESTION_CACHE_DIR`
* re-runs `setup_db` to recreate schemas

### RabbitMQ reset (rare)

If the broker state itself goes sideways (stuck messages, schema
mismatch after a major version bump):

```bash
docker compose stop rabbitmq
docker compose rm -fv rabbitmq
docker volume rm kb-llamaindex_rabbitmq_data
docker compose up -d rabbitmq
# Restart worker so it reconnects:
pkill -f "taskiq worker src.ingestion"
uv run taskiq worker src.ingestion.tasks:broker --workers 1 &
```

### Only re-ingest (keep schemas)

```bash
uv run python -m scripts.wipe_db --yes --keep-files
# Then re-upload your docs
```

---

## 12. Switching models

Detailed swap procedures in `docs/MODELS.md`.  Two scenarios:

* **OpenAI → Ollama**: edit `docker/litellm_config.yaml`
  (point at `ollama_chat/...`), drop `LITELLM_LLM_MODEL` /
  `LITELLM_EMBEDDING_MODEL` env, set `MILVUS_DIM` to match the
  new embedding model (e.g. 768 for `nomic-embed-text`), wipe
  Milvus (`scripts/wipe_db.py`), `docker compose up -d --force-recreate litellm`.
* **gpt-4o-mini → gpt-4o**: edit `LITELLM_LLM_MODEL=gpt-4o` in
  `.env`, restart API + worker.  No re-ingest required (the
  embedding model didn't change).

---

## 13. Production checklist

Beyond local dev:

| Item | Action |
|---|---|
| **Auth** | Rotate `API_KEYS` per env. Add a real master_key + database for LiteLLM (see `docker/litellm_config.yaml` comments). |
| **Persistence** | Mount Docker volumes onto durable storage. Schedule Postgres + Neo4j backups. Milvus has its own snapshot tooling. |
| **Logging** | `API_LOG_JSON=true` for structured logs. Aggregate via loki / cloudwatch. |
| **Process supervisor** | Replace `uv run uvicorn` with `gunicorn -k uvicorn.workers.UvicornWorker` (4+ workers). Run taskiq worker under `supervisord` / `systemd` with restart policy. |
| **Scaling** | Multiple taskiq workers share the same queue automatically. API workers are stateless behind a load balancer. |
| **Health probes** | `/health` is the liveness path. For readiness, additionally probe `/api/v1/search` with a known cached query. |
| **Network isolation** | RabbitMQ, Postgres, Neo4j, Milvus, LiteLLM all bind to public ports by default in `docker-compose.yml` — restrict to VPC / private network in production. |
| **Cost monitoring** | LiteLLM proxy exposes usage metrics on `/v1/usage` when configured with a database. Set per-model rate limits via its UI / DB. |
| **Re-ingest discipline** | Changing `INGESTION_CHUNK_SIZE`, the embedding model, or `MILVUS_DIM` invalidates the corpus — wipe and re-ingest. |
| **Translation cost** | If running on heavy multilingual corpus and translation cost matters, set `INGESTION_TRANSLATE_TO_RUSSIAN=false`. Graph stays in source language; cross-language entity dedup degrades. |

---

## 14. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Worker logs `asyncio.run() cannot be called from a running event loop` | Old code path before the `pipeline.arun` / `asyncio.to_thread` fix landed | Pull latest, restart worker |
| `Property values can only be of primitive types or arrays thereof` in worker | `canonical_identifiers` (list[dict]) leaked into PropertyGraphIndex | Already fixed in `tasks.py:_strip_neo4j_unsafe_metadata`; pull latest |
| `AssertionError` in PropertyGraphIndex._insert_nodes | Stripper accidentally removed `KG_NODES_KEY` | Already fixed via `_PRESERVE_METADATA_KEYS` allow-list |
| 401 on every /v1/chat/completions | `OPENAI_API_KEY` missing from container env | Edit `.env`, `docker compose up -d --force-recreate litellm` |
| `/api/v1/search` returns 500 with "rejected request: vector dim mismatch" | Embedding model changed (1536 vs 768) but collection wasn't recreated | `scripts.wipe_db` + re-ingest |
| Worker eats LLM calls forever, no completion | Translation concurrency = OpenAI tier limit. Throttle in proxy or lower `INGESTION_TRANSLATION_CONCURRENCY` |
| `ChannelInvalidStateError` after long ingest | RabbitMQ heartbeat timeout on long-running task | Raise `RABBITMQ_TIMEOUT_S`; not a correctness issue (task already finished) |

---

## 15. Where to look

| Need | Open this |
|---|---|
| Understand the whole picture | `docs/ARCHITECTURE.md` |
| Trace one query end-to-end | `docs/QUERY.md` |
| Swap the LLM / embedding model | `docs/MODELS.md` |
| Diagnose KG extraction | `scripts/diag_kg.py`, `scripts/diag_kg_medical.py`, `scripts/diag_kg_lightrag.py` |
| Inspect ingest pipeline state | `docker exec kb-llamaindex-postgres-1 psql -U postgres -d kb_llamaindex -c "SELECT id, status, error FROM documents ORDER BY created_at DESC LIMIT 10;"` |
| Inspect Neo4j entities | `uv run python -c "from neo4j import GraphDatabase; ..."` (snippets in `scripts/wipe_db.py` and `docs/ARCHITECTURE.md`) |
| Inspect Milvus chunks | Use `pymilvus` from any python REPL: `MilvusClient('http://localhost:19530').query(collection_name='kb_llamaindex', filter='doc_id=="..."', output_fields=["*"])` |

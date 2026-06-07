# Deployment Guide

Step-by-step from a clean machine to a running `kb-llamaindex`
stack.  Covers local dev (macOS / Linux), staging, and the
operational knobs needed to move toward production.

The stack is **Temporal-orchestrated**: the API enqueues a Temporal
`DocumentIngestWorkflow`, and a separate worker process polls Temporal
task queues to do the work.  For the queue topology + per-queue
concurrency, see `docs/QUEUES.md`.  For the system architecture, see
`docs/ARCHITECTURE.md`.

---

## 1. Prerequisites

| Tool | Version | Why |
|---|---|---|
| **Docker + Compose** | 24+ | All backing stores + Temporal run in containers |
| **Python** | 3.11 or 3.12 | API + worker |
| **uv** | 0.4+ | Project package manager (`uv run ...`) |
| **OpenAI API key** | — | LiteLLM upstream (default `large` tier = `gpt-4o-mini`) |

Optional (only if running a local model instead of OpenAI):

| Tool | Version | Why |
|---|---|---|
| **Ollama** / vLLM | 0.5+ | Local `small`-tier LLM + embeddings |

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
uv run python -c "import llama_index, fastapi, temporalio; print('ok')"
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

# ── Two physical model tiers (you manage exactly TWO names).  Every
# logical role resolves to one of these (small = high-volume local,
# large = final synthesis only).  See docs/MODELS.md.
LITELLM_MODEL_SMALL=gemma4:e4b
LITELLM_MODEL_LARGE=gpt-4o-mini

# ── Embedding dim must match the model AND Milvus: 1536 for
# text-embedding-3-small, 768 for nomic-embed-text, 3072 for -3-large.
LITELLM_EMBEDDING_MODEL=text-embedding-3-small
LITELLM_EMBEDDING_DIM=1536
MILVUS_DIM=1536          # MUST match LITELLM_EMBEDDING_DIM

# ── Russian normalisation of the knowledge graph (set false to
# skip the LLM translation cost on ingest; graph stays in source language)
INGESTION_TRANSLATE_TO_RUSSIAN=true
INGESTION_TRANSLATION_CONCURRENCY=4

# ── Opt-in subsystems (default OFF) ────────────────────────────────
WIKIBASE_ENABLED=false   # push canonical entities into self-hosted Wikibase
WIKI_ENABLED=false       # continuous per-entity MediaWiki article editor
```

Switching to a local model setup → see `docs/MODELS.md`.

---

## 4. Bring up the storage + orchestration stack

```bash
docker compose up -d
```

This starts the following containers:

| Container | Host port(s) | Purpose | Healthcheck |
|---|---|---|---|
| `etcd` | — | Milvus metadata | `etcdctl endpoint health` |
| `minio` | 9000, 9001 (console) | Milvus object store + user uploads (`kb-uploads` bucket) | `/minio/health/live` |
| `milvus` | 19530, 9091 | Vector index (HNSW by default) | `:9091/healthz` |
| `neo4j` | 7474 (web), 7687 (bolt) | Property graph (+ APOC + GDS) | `:7474` spider probe |
| `postgres` | 5432 | Job-status table + ingest metrics; also backs Temporal (separate DBs) | `pg_isready` |
| `temporal` | 7233 | Workflow engine (`auto-setup` image) | — (depends on postgres) |
| `temporal-ui` | 8080 | Temporal Web UI | — |
| `litellm` | 4000 | LLM gateway (reads `OPENAI_API_KEY`) | `/health/liveliness` |
| `prometheus` | 9092 → 9090 | Scrapes worker + Temporal metrics | — |
| `grafana` | 3001 → 3000 | Dashboards | — |
| `wikibase-mysql` | — | MariaDB backing Wikibase/MediaWiki | `mariadb-admin ping` |
| `wikibase` | 8181 → 80 | Wikibase + MediaWiki (opt-in target) | `Special:Version` |
| `wdqs` | 8989 → 9999 | Wikibase Query Service (SPARQL) | — (can flap; optional at runtime) |

Notes:
* The `wikibase` / `wikibase-mysql` / `wdqs` containers always start with
  the stack but are only **used** when `WIKIBASE_ENABLED=true` or
  `WIKI_ENABLED=true`.  `wdqs` has no healthcheck and is known to flap on
  boot — it is optional at runtime and does not block ingest/search.
* Temporal here shares the app `postgres` instance (separate `temporal` /
  `temporal_visibility` DBs).  `NUM_HISTORY_SHARDS` defaults to 512 (the
  prod floor) and is **immutable after the cluster's first init** — set
  `TEMPORAL_NUM_HISTORY_SHARDS=4` on a small dev box *before* the first
  boot.  See the prod-hardening comments in `docker-compose.yml`.
* The **worker and API run on the host** (not in compose).  Prometheus
  scrapes the host worker via `host.docker.internal` (exporter on
  `METRICS_BIND_ADDRESS`, default `0.0.0.0:9090`).

Wait for everything to become healthy:

```bash
docker compose ps
# Core STATUS columns should show "healthy"
```

Manual probe:

```bash
curl -fsS http://localhost:4000/health/liveliness   # LiteLLM
curl -fsS http://localhost:9091/healthz             # Milvus
docker exec kb-llamaindex-postgres-1 pg_isready -U postgres
```

---

## 5. Initialise schemas

`scripts/setup_db.py` is idempotent — safe to re-run.

```bash
uv run python -m scripts.setup_db
```

What it does:
* Creates the `documents` table (+ status / department indexes) and the
  `ingest_metrics` table in Postgres.
* Pings Milvus (the collection is created lazily by `MilvusVectorStore`
  on first insert).
* Ensures the MinIO upload bucket (`MINIO_BUCKET`, default `kb-uploads`)
  exists.
* Registers Temporal custom Search Attributes used by the analytics layer
  (no-op / warning if the Postgres visibility store doesn't support them).
* Neo4j needs no schema — labels and indexes are emitted at insert time
  by `PropertyGraphIndex`.

### 5b. (Optional) Bootstrap Wikibase + the wiki schedule

Only if `WIKIBASE_ENABLED=true` and/or `WIKI_ENABLED=true`.

```bash
# One-time Wikibase bootstrap: creates base-class Items + Properties,
# provisions the runtime bot account, caches QIDs/PIDs into Neo4j.
uv run python -m scripts.setup_wikibase
#   --dry-run        report planned creates without writing
#   --refresh-cache  re-pull existing QIDs/PIDs into the Neo4j cache only

# Create/refresh the Temporal Schedule that runs WikiSweepWorkflow every
# WIKI_SWEEP_INTERVAL_MINUTES.  No-op when WIKI_ENABLED=false.
uv run python -m scripts.setup_wiki_schedule
```

---

## 6. Smoke-test the LLM gateway

Confirm the LiteLLM proxy reaches its upstream with the key you pasted:

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

Two long-lived host processes.  Run each in its own terminal (or use
`tmux` / `systemd` in production):

```bash
# Terminal 1 — API
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 — Temporal worker (hosts ALL queue pools in one process)
uv run python -m src.workflow.worker
```

The single worker process hosts seven Worker pools against the same
Temporal client — ingest (`kb-ingest`), extract (`kb-ingest-llm`), merge
(`kb-ingest-merge`), search (`kb-search-small`), large-tier synthesis
(`kb-search-large`), offline community build (`kb-graph-build`), and the
wiki editor (`kb-wiki`).  See `docs/QUEUES.md` for the full table.  On
boot it logs each queue + its concurrency cap; with `METRICS_ENABLED=true`
it also starts the Prometheus exporter on `METRICS_BIND_ADDRESS`.

Sanity:

```bash
curl -fsS http://localhost:8000/health        # {"status":"ok", ...}
```

> **Note**: the API uses `--reload` for dev convenience.  Disable in
> production (`gunicorn -k uvicorn.workers.UvicornWorker` with multiple
> workers).  For a multi-machine deployment, run separate worker
> processes and point each at the queues it should poll — keep the LLM
> lanes (`kb-ingest-llm` / `kb-ingest-merge`) on the GPU box (see the
> module docstring in `src/workflow/worker.py`).

---

## 8. Run a first ingest

The ingest endpoint stores the file in MinIO, inserts the Postgres
`documents` row, and **starts a Temporal `DocumentIngestWorkflow`**; the
worker then runs it end-to-end (fetch → parse/chunk → embed/index →
extract KG → merge graph).

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
#   (a vector-indexed doc whose graph half failed shows "vector_only")
```

### 8b. The test medical corpus

```bash
uv run python -m scripts.ingest_medical
```

Converts `tests/eval/corpora/medical/medical.json` → a corpus file,
uploads it via `/api/v1/ingest`, and polls until done.

---

## 9. Smoke-test search

```bash
# Local — plan-execute (the default mode)
curl -fsS -X POST http://localhost:8000/api/v1/search/local \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?","top_k":10}' \
  | python3 -m json.tool

# Auto — router picks local/global/drift
curl -fsS -X POST http://localhost:8000/api/v1/search/auto \
  -H "X-API-Key: dev-local-key" \
  -H "Content-Type: application/json" \
  -d '{"query":"какие риски развития рака кожи?","top_k":10}' \
  | python3 -m json.tool

# Global/drift need community summaries first (offline build, kb-graph-build):
curl -fsS -X POST http://localhost:8000/api/v1/admin/communities/rebuild \
  -H "X-API-Key: dev-local-key"
```

Expected: Russian answer + `sources[].content` (original-language chunk
text) + `documents[]` (download links to the source files used).  Latency
is model-bound (the large synthesis tier dominates).  Endpoints + tuning:
`docs/runbook/search-usage.md`.

---

## 10. Run the test suite

```bash
uv run pytest -q
```

The test suite is offline (stub LLM, stub Milvus / Neo4j) — safe to run
in CI without any backing services.

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

`scripts/wipe_db.py` now wipes **Temporal workflows and MediaWiki pages**
in addition to the data stores.  In order, it:

* **Temporal** — terminates every RUNNING workflow, then deletes every
  execution (open + closed) in the namespace, so Temporal matches the
  wiped stores.  *(Best-effort; fail-open if the server is down.)*
* **Postgres** — `TRUNCATE documents`.
* **Milvus** — drops the configured collection.
* **Neo4j** — `MATCH (n) DETACH DELETE n` + drops non-system indexes.
* **MediaWiki** — deletes the wiki-editor article pages (every
  main-namespace page except `Main Page`).  *(Best-effort; logs a warning
  and continues if the wiki stack is down or admin login fails.  Does NOT
  touch Wikibase Items/Properties — those belong to `setup_wikibase`; the
  per-entity QIDs were already removed with the Neo4j wipe.)*
* **Filesystem** — clears `API_UPLOAD_DIR` and `INGESTION_CACHE_DIR`.
* Re-runs `setup_db` to recreate schemas.

Flags:

| Flag | Effect |
|---|---|
| `--yes` / `-y` | Skip the interactive confirmation (CI / scripts) |
| `--keep-temporal` | Don't terminate/delete Temporal workflow executions |
| `--keep-wiki` | Don't delete MediaWiki article pages |
| `--keep-files` | Don't touch the upload dir / ingestion cache |
| `--no-setup` | Skip running `setup_db` after the wipe |

The API / worker do not need to be stopped — they'll see empty stores on
the next request.  Any in-flight ingest will fail mid-flight (its
`documents` row disappears and its workflow is terminated).

### Only re-ingest (keep schemas + Temporal + wiki)

```bash
uv run python -m scripts.wipe_db --yes --keep-files --keep-temporal --keep-wiki
# Then re-upload your docs
```

---

## 12. Switching models

Detailed swap procedures in `docs/MODELS.md`.  Two scenarios:

* **OpenAI → local (Ollama/vLLM)**: edit `docker/litellm_config.yaml`
  (point the `small`/`large` tiers at `ollama_chat/...`), set
  `LITELLM_MODEL_SMALL` / `LITELLM_MODEL_LARGE`, set `MILVUS_DIM` /
  `LITELLM_EMBEDDING_DIM` to match the new embedding model (e.g. 768 for
  `nomic-embed-text`), wipe Milvus (`scripts/wipe_db.py`), then
  `docker compose up -d --force-recreate litellm`.
* **Escalate one role to the large tier**: set
  `LITELLM_ROLE_TIERS='{"plan":"large"}'` in `.env`, restart API + worker.
  No re-ingest required (the embedding model didn't change).

---

## 13. Production checklist

Beyond local dev:

| Item | Action |
|---|---|
| **Auth** | Rotate `API_KEYS` per env. Add a real master_key + database for LiteLLM (see `docker/litellm_config.yaml` comments). |
| **Temporal hardening** | The `auto-setup` image re-runs schema setup every boot — switch to `temporalio/server` + migrate schema once via `temporal-sql-tool`. Give Temporal its OWN Postgres (it is history-write-heavy). Never bump the Temporal image in place on a cluster with live data. See `docker-compose.yml` comments. |
| **Persistence** | Mount Docker volumes onto durable storage. Schedule Postgres + Neo4j backups. Milvus has its own snapshot tooling. |
| **Logging** | `API_LOG_JSON=true` for structured logs. Aggregate via loki / cloudwatch. |
| **Process supervisor** | Replace `uv run uvicorn` with `gunicorn -k uvicorn.workers.UvicornWorker` (4+ workers). Run the Temporal worker under `supervisord` / `systemd` with a restart policy. |
| **Scaling** | Run more worker processes (they share the same task queues — Temporal load-balances). For a GPU split, run separate worker processes pinned to the LLM lanes. Per-queue concurrency: `docs/QUEUES.md`. API workers are stateless behind a load balancer. |
| **LLM concurrency** | Real LLM concurrency is owned by the per-process `LLMPool` (`LLM_POOL_*`), not the Temporal queue caps — size `LLM_POOL_TIER_SMALL_TOTAL` / `LLM_POOL_TIER_LARGE_TOTAL` to your GPU + upstream budget. Keep the Temporal `*_ACTIVITY_CONCURRENCY` caps ≥ the matching pool lane ceiling. |
| **Health probes** | `/health` is the liveness path. For readiness, additionally probe a known-cached search query. |
| **Network isolation** | Postgres, Neo4j, Milvus, Temporal, LiteLLM all bind to public ports by default in `docker-compose.yml` — restrict to VPC / private network in production. |
| **Re-ingest discipline** | Changing `INGESTION_CHUNK_SIZE`, the embedding model, or `MILVUS_DIM` invalidates the corpus — wipe and re-ingest. |
| **Translation cost** | On a heavy multilingual corpus, set `INGESTION_TRANSLATE_TO_RUSSIAN=false`. Graph stays in source language; cross-language entity dedup degrades. |

---

## 14. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Worker won't start, logs a LiteLLM model-config error at boot | `validate_litellm_models` caught a bad model name before any activity ran | Fix `docker/litellm_config.yaml` / `LITELLM_MODEL_*`, restart worker |
| Ingest stuck at `pending`/`processing`, no progress | Temporal worker not running, or not polling the right queues | Start `uv run python -m src.workflow.worker`; check Temporal UI (`:8080`) for stuck/failed workflows |
| `Property values can only be of primitive types or arrays thereof` in worker | unsafe metadata leaked into `PropertyGraphIndex` | Already fixed in the metadata stripper; pull latest |
| 401 on every `/v1/chat/completions` | `OPENAI_API_KEY` missing from the litellm container env | Edit `.env`, `docker compose up -d --force-recreate litellm` |
| `/api/v1/search` returns 500 with "vector dim mismatch" | Embedding model changed but the Milvus collection wasn't recreated | `scripts.wipe_db` + re-ingest |
| A document ends in `vector_only` status | Vector half indexed but the graph/merge half failed | Inspect the `GraphBuildWorkflow` run in the Temporal UI; re-ingest |
| Worker eats LLM calls forever, no completion | Translation/extraction concurrency vs upstream tier limit | Lower `INGESTION_TRANSLATION_CONCURRENCY` / tune the `LLM_POOL_*` caps |

---

## 15. Where to look

| Need | Open this |
|---|---|
| Understand the whole picture | `docs/ARCHITECTURE.md` |
| The Temporal task-queue topology + concurrency | `docs/QUEUES.md` |
| Trace one query end-to-end | `docs/SEARCH.md` (architecture) · `docs/runbook/search-usage.md` (usage) |
| Swap the LLM / embedding model | `docs/MODELS.md` |
| Inspect workflows / retries | Temporal Web UI at `http://localhost:8080` |
| Diagnose KG extraction | `scripts/diag_kg.py`, `scripts/diag_kg_medical.py` |
| Inspect ingest pipeline state | `docker exec kb-llamaindex-postgres-1 psql -U postgres -d kb_llamaindex -c "SELECT id, status, error FROM documents ORDER BY created_at DESC LIMIT 10;"` |
| Inspect Neo4j entities | Neo4j Browser at `http://localhost:7474` (bolt `:7687`) |
| Inspect Milvus chunks | `pymilvus` REPL: `MilvusClient('http://localhost:19530').query(collection_name='kb_llamaindex', filter='doc_id=="..."', output_fields=["*"])` |

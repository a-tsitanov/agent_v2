# Deployment & env redesign — design

**Date:** 2026-06-16
**Status:** approved (brainstorming)
**Author:** a.tzitanov + Claude

## Problem

Deployment is "inconvenient and unclear." Root cause: **no single source of
truth for env.** 98 env vars across 15 settings classes are declared/defaulted
in up to 6 places (`config.py`, `.env.example`, `.env.prod.example`,
`docker-compose.yml`, `docker-compose.prod.yml`, `scripts/make_env.py`) that
drift. Dev (app on host) and prod (app in containers) are structurally
different, so host-only bootstrap scripts don't fit prod. Concrete failures
found in audit:

- `config.py` dim/model defaults disagreed with all templates (dim-drift).
  *(partially addressed — canon to be locked here.)*
- Prod compose **never runs `setup_db`** → fresh prod has no tables/bucket/
  search-attrs → first ingest fails.
- `setup_wikibase` needs the host docker socket (`docker exec`) → can't run
  from the api container.
- No fail-fast preflight: misconfig surfaces as a deep stack trace mid-request,
  not a clear startup error. (The dim check lives only in `make_env`, which the
  documented `cp .env.example .env` path skips.)
- Prod-wrong defaults with no override hook (API_ENV=development, CORS=`*`,
  log_json=false, MINIO_SECURE=false, WIKIBASE_DB_ROOT_PASSWORD).
- amd64 images on arm64 (emulation); BGE reranker (~1GB) lazily pulled at first
  request.
- Stale README (rabbitmq/taskiq).

(Dead/deprecated env vars already removed in a prior commit; `litellm.llm_model`
+ `effective_base` and `REDIS_*`/`LLM_CACHE_ENABLED` intentionally kept.)

## Assumptions (confirmed)

- **A.** Target = a single server via `docker-compose.prod.yml` (NOT k8s/managed).
- **B.** Canonical LLM profile = OpenAI `text-embedding-3-small` / **1536**;
  Ollama (`nomic-embed-text` / 768) is a documented opt-in override.

## Goal

Deployment that is convenient and clear:
1. ONE source of truth for env (config.py) — no drift.
2. Fail-fast preflight at boot with actionable messages.
3. One command to bring up + initialize (dev and prod).
4. Prod self-initializes (no forgotten manual steps).

## Design

### Component 1 — config.py as the single source of truth
- Lock the canonical dim/model: `MILVUS_DIM` and the embedding model default to
  the **1536 / text-embedding-3-small** profile across `config.py` AND all
  templates/compose. Document the Ollama/768 override as a commented block.
- Invert `scripts/make_env.py`: it reads `config.py` (env_prefix + fields +
  defaults + `SecretStr` markers) as the authority and **generates**
  `.env.example`. Add a `--check` mode (CI-friendly) that fails when
  `.env.example` / the compose `x-app-env` anchor drift from the field set:
  - **coverage check** — every non-deprecated field appears in `.env.example`;
  - **orphan check** — every env var in templates/anchor maps to a real field
    (allow an explicit `_FORWARD_DECL` allowlist for REDIS_*/LLM_CACHE_ENABLED).
- Keep the existing secret-generation + cross-field `validate()` logic.

### Component 2 — fail-fast preflight at boot
- Add `Settings.preflight() -> list[str]` (or `scripts/preflight.py`) returning
  actionable problems. Checks:
  - secrets not left at placeholder defaults when `API_ENV=production`
    (API_KEYS, NEO4J/POSTGRES/MINIO passwords, WIKIBASE_BOT_PASSWORD if wiki on);
  - `TEMPORAL_LLM/MERGE_ACTIVITY_CONCURRENCY >= LLM_POOL_N`;
  - if `WIKI_ENABLED`/`WIKIBASE_ENABLED`: bot creds present + ≥8;
  - LiteLLM base URL reachable (best-effort, warn).
- Call it at API startup (`src/api/main.py`) and worker startup
  (`src/workflow/worker.py`): in `API_ENV=production` a hard problem **exits
  non-zero with a clear message**; in dev it logs warnings. (Mirror the existing
  litellm-model validator pattern.)

### Component 3 — auto-initialization (no manual host scripts)
- **`init` one-shot service** in both compose files: same app image,
  `command: ["python","-m","scripts.setup_db"]`, `restart: "no"`,
  `depends_on` postgres/milvus/minio `service_healthy`. Make `api`/`worker`
  `depends_on: init: { condition: service_completed_successfully }`. → prod
  initializes itself; the dev path can also use it.
- **`setup_wikibase` without the host docker socket:** replace the
  `docker exec <container> createAndPromote` bot step with a compose **one-shot
  service** that runs the maintenance script inside the wikibase service's own
  network/image (profile `wikibase`), or an in-cluster MediaWiki API call. The
  schema-bootstrap part already works over the API. Result: runnable from any
  container; the api-container `docker`-absent failure disappears.

### Component 4 — one command
- `Makefile` (or `scripts/up.sh`):
  - `make up` — `make_env --check` (or generate) → `docker compose up -d` → wait
    for health → init runs automatically → print smoke-test hints.
  - `make up-prod` — same against `docker-compose.prod.yml` (+ `--profile`
    handling for optional wiki).
  - `make down`, `make logs`, `make wiki-setup` (runs the wikibase one-shot).

### Component 5 — platform + model prefetch
- arm64: document/setting for `DOCKER_DEFAULT_PLATFORM=linux/amd64` + Rosetta;
  optionally pin `platform:` on the amd64-only services (wikibase/wdqs) with a
  comment.
- Prefetch the BGE reranker (and optional GLiNER) in the Dockerfile build (or
  the init service) so first `/search` doesn't stall ~minutes pulling ~1GB.

### Component 6 — prod-correct defaults exposed
- Surface in `.env.prod.example` + anchor: `API_ENV=production`,
  `API_CORS_ORIGINS=<concrete>`, `API_LOG_JSON=true`, `MINIO_SECURE`,
  `WIKIBASE_DB_ROOT_PASSWORD`, `GRAFANA_ADMIN_USER`.

### Component 7 — docs
- Rewrite `docs/QUICKSTART.md` + `docs/DEPLOYMENT.md` to the one-command flow.
- Fix the stale `README.md` run section (rabbitmq/taskiq → Temporal + `make up`).

## Batched execution

- **Batch 1 — correctness (highest value):** lock canon dim/model (Component 1
  partial); `init` one-shot service for `setup_db` (Component 3a); preflight
  validator (Component 2); `setup_wikibase` container-friendly (Component 3b).
- **Batch 2 — single source of truth:** `make_env` reads config.py + `--check`
  drift/coverage/orphan (Component 1 full); prod-correct defaults (Component 6).
- **Batch 3 — DX/ops:** `make up`/`Makefile` (Component 4); platform + model
  prefetch (Component 5); docs (Component 7).

Each batch: spec-grounded plan → subagent-driven TDD → review → commit; live-
verify on the running stack where possible (init service, preflight, wikibase
one-shot). No push without explicit go.

## Out of scope (YAGNI / deferred)

- k8s / Helm / external secrets manager (assumption A: single-server compose).
- Removing `litellm.llm_model` + `effective_base` (legacy no-role `build_llm`
  path; separate task — live `.env` still sets it).
- `REDIS_*` / `LLM_CACHE_ENABLED` (forward-declared for redis-llm-cache).
- Real search filters (#18) — unrelated.

## Risks

- Changing the canonical dim to 1536 means a fresh Milvus collection at 1536; an
  existing 768 collection (local Ollama test) must be recreated — call this out
  in docs, don't auto-wipe.
- `service_completed_successfully` requires compose spec v2.20+ (Docker Compose
  v2) — already used here. Verify on the target host.
- Wikibase one-shot bot creation must be idempotent (`--force`) and gated on the
  `wikibase` profile so the core stack isn't blocked when wiki is off.

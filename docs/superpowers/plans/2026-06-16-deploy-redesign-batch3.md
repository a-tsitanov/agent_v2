# Deploy redesign — Batch 3 (DX) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

> **Project git gate:** commits allowed (allow-rule set); do NOT `git push`.

**Goal:** One command to bring up + initialize the stack, model prefetch to avoid first-request stalls, and docs that match reality.

**Architecture:** A `Makefile` wraps the verified flow (env-check → compose up --wait → init). `make models` prefetches HF models via the existing `scripts/download_models.py`. Docs rewritten to the `make up` flow; stale README (rabbitmq/taskiq) fixed; arm64 note added.

**Tech Stack:** GNU Make, Docker Compose v2 (`--wait`, profiles), existing `scripts/`.

Spec: `docs/superpowers/specs/2026-06-16-deploy-env-redesign-design.md` (Components 4, 5, 7).

---

## File Structure
- `Makefile` — NEW: the one-command entrypoints.
- `scripts/start.sh` — refresh stale echo text (qwen3 → canon; setup_db → `make init`).
- `docs/QUICKSTART.md` — rewrite to the `make up` flow + arm64/prefetch notes.
- `README.md` — fix stale rabbitmq/taskiq run section.

---

## Task 1: Makefile — one-command bring-up

**Files:** Create `Makefile`.

- [ ] **Step 1: Create `Makefile`** with these targets (tabs, not spaces, for recipes):

```makefile
# kb-llamaindex — one-command dev/prod operations.
COMPOSE      ?= docker compose
PROD_COMPOSE ?= docker compose -f docker-compose.prod.yml
PY           ?= uv run python

.DEFAULT_GOAL := help

.PHONY: help env-check up init models down logs ps up-prod wiki-setup

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

env-check:  ## Validate env: .env.reference current (drift guard)
	$(PY) -m scripts.make_env --check

up: env-check  ## Dev: bring up backends (healthy) + init schemas
	$(COMPOSE) up -d --wait
	$(COMPOSE) --profile init up init
	@echo "Backends up + schemas initialised. Start the app on the host:"
	@echo "  $(PY) -m src.workflow.worker &"
	@echo "  uv run uvicorn src.api.main:app --port 8000 &"

init:  ## Run setup_db (schemas + bucket + Temporal search-attrs)
	$(COMPOSE) --profile init up init

models:  ## Prefetch HF models (BGE reranker + GLiNER) to avoid first-request stalls
	$(PY) -m scripts.download_models

wiki-setup:  ## Bootstrap Wikibase: create bot (in running container) + schema over API
	$(COMPOSE) --profile wikibase exec wikibase \
	  php /var/www/html/maintenance/run.php createAndPromote --bot --force \
	  "$${WIKIBASE_BOT_USER:-KbBot}" "$${WIKIBASE_BOT_PASSWORD:?set WIKIBASE_BOT_PASSWORD}"
	$(PY) -m scripts.setup_wikibase

up-prod: env-check  ## Prod: build + up the full app stack (init runs automatically)
	$(PROD_COMPOSE) up -d --build --wait

down:  ## Stop the dev stack
	$(COMPOSE) down

logs:  ## Tail dev stack logs
	$(COMPOSE) logs -f --tail=50

ps:  ## Show dev stack status
	$(COMPOSE) ps
```

- [ ] **Step 2: Verify `make help` + `make env-check`**

Run: `make help` — lists the targets with descriptions.
Run: `make env-check` — runs `make_env --check`, exits 0 (reference current; may print an [INFO] coverage line).
Expected: both work. (If `make` recipes error on spaces-vs-tabs, fix to tabs.)

- [ ] **Step 3: Verify `make init` works against the running stack (live)**

Run: `make init`
Expected: the init one-shot runs `setup_db` and exits 0 ("setup_db all done"). (Stack is already up from earlier; if not, `make up` first.)

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "feat(deploy): Makefile — one-command up/init/models/wiki-setup/down"
```

---

## Task 2: Refresh start.sh + fix stale README + QUICKSTART rewrite

**Files:** `scripts/start.sh`, `README.md`, `docs/QUICKSTART.md`.

- [ ] **Step 1: Fix `scripts/start.sh` stale echo**

Read it. In the `up)` branch, replace the stale instructions:
- `ollama pull qwen3:8b nomic-embed-text` → note that the canonical profile is OpenAI (text-embedding-3-small); Ollama is opt-in (`gemma4:e4b` + `nomic-embed-text`, see `.env.example`).
- `uv run python -m scripts.setup_db` → `make init` (or `make up`).
- Update the Neo4j/MinIO/Postgres credential hints to say "see your .env" rather than hardcoded `minioadmin`/`changeme` (those are dev defaults; don't imply they're prod).
Keep the script's structure (up/down/logs/ps).

- [ ] **Step 2: Fix stale README run section**

In `README.md`, find the stale lines:
- line ~37: `rabbitmq / litellm / ...` → remove `rabbitmq` (the stack uses Temporal, not rabbitmq). List the real services (etcd/minio/milvus/neo4j/postgres/temporal/litellm).
- lines ~69-71: the `# Serve API + worker` block with `uv run taskiq worker src.ingestion.tasks:broker` → replace with the real flow:
```bash
# One command (backends + schema init):
make up
# Then start the app on the host:
uv run python -m src.workflow.worker &
uv run uvicorn src.api.main:app --port 8000 &
```
Grep after: `grep -niE "rabbitmq|taskiq" README.md` → should be ZERO.

- [ ] **Step 3: Rewrite `docs/QUICKSTART.md` to the `make up` flow**

Read it. Replace the manual TL;DR (cp .env.example, docker compose up, setup_db, worker, api) with:
```bash
cp .env.example .env        # 1. config (edit secrets; see §2)
make up                     # 2. backends + schema init (one command)
make models                 # 3. (optional) prefetch reranker model
uv run python -m src.workflow.worker &   # 4. worker (host)
uv run uvicorn src.api.main:app --port 8000 &  # 5. API (host)
```
Keep the env-params table + feature-enable matrix. Update the wiki section to use `make wiki-setup`. Add a short "Apple Silicon / arm64" note: some images (wikibase, wdqs) are amd64-only — enable Docker Desktop's Rosetta emulation; give Docker enough RAM (Milvus standalone wants ≥4–6 GB). Add a note: `make models` avoids a ~1GB reranker download on the first /search.

- [ ] **Step 4: Verify docs coherence**

Run: `grep -niE "rabbitmq|taskiq" README.md docs/QUICKSTART.md` → 0.
Run: `grep -nE "make up|make init|make models|make wiki-setup" docs/QUICKSTART.md README.md` → present.

- [ ] **Step 5: Commit**

```bash
git add scripts/start.sh README.md docs/QUICKSTART.md
git commit -m "docs: one-command (make up) flow; drop stale rabbitmq/taskiq; arm64 + prefetch notes"
```

---

## Task 3: Verification

- [ ] **Step 1: Make targets**

Run: `make help && make env-check` → both succeed.
Run (live): `make models` → downloads the BGE reranker (and GLiNER); confirm it completes (or, if network-restricted, note it; the target is correct).

- [ ] **Step 2: Docs grep clean**

Run: `grep -rniE "rabbitmq|taskiq" README.md docs/QUICKSTART.md scripts/start.sh` → 0 matches.

- [ ] **Step 3: Final commit** (if anything left)

```bash
git add -A && git commit -m "chore: deploy batch 3 cleanup"
```

---

## Self-Review notes (author)
- **Spec coverage:** Component 4 (one command) → Task 1 Makefile; Component 5 (platform + prefetch) → `make models` + the arm64 QUICKSTART note (Task 1/2) — NOTE: model prefetch is a `make` target, NOT baked into the Dockerfile (avoids ~1GB image bloat + build-time network dep); documented rationale; Component 7 (docs) → Task 2.
- **Decision:** `make up` brings up backends + runs init, but does NOT start the host worker/API (dev runs those on the host) — it prints the two commands. Prod (`up-prod`) runs everything in compose incl. the auto-init service.
- **Make tabs:** recipe lines MUST be tab-indented; the implementer must ensure tabs (a common make pitfall).

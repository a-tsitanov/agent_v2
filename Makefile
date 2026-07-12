# kb-llamaindex — one-command dev/prod operations.
COMPOSE      ?= docker compose
PROD_COMPOSE ?= docker compose -f docker-compose.prod.yml
# Full prod stack + litellm proxy + tg-ingest, in one invocation.
PROD_ALL_COMPOSE ?= docker compose -f docker-compose.prod.yml -f docker-compose.litellm.yml -f docker-compose.tg-ingest.yml
PY           ?= uv run python

.DEFAULT_GOAL := help

.PHONY: help env-check up init models down logs ps up-prod up-prod-all down-prod-all wiki-setup

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
	@WB_USER=$$(sed -n 's/^WIKIBASE_BOT_USER=//p' .env 2>/dev/null); \
	 WB_PASS=$$(sed -n 's/^WIKIBASE_BOT_PASSWORD=//p' .env 2>/dev/null); \
	 $(COMPOSE) --profile wikibase exec -T wikibase \
	   php /var/www/html/maintenance/run.php createAndPromote --bot --force \
	   "$${WB_USER:-KbBot}" "$${WB_PASS:?set WIKIBASE_BOT_PASSWORD in .env (>= 8 chars)}"
	$(PY) -m scripts.setup_wikibase

up-prod: env-check  ## Prod: build + up the full app stack (init runs automatically)
	$(PROD_COMPOSE) up -d --build --wait

up-prod-all: env-check  ## Prod EVERYTHING in one command: app+nebula + litellm + tg-ingest
	@command -v ollama >/dev/null 2>&1 && ! curl -sf http://localhost:11434/api/tags >/dev/null 2>&1 \
	  && echo "⚠ host ollama not reachable on :11434 — start it: OLLAMA_HOST=0.0.0.0 ollama serve" || true
	$(PROD_ALL_COMPOSE) up -d --build --wait

down-prod-all:  ## Stop the full prod+litellm+tg-ingest stack
	$(PROD_ALL_COMPOSE) down

down:  ## Stop the dev stack
	$(COMPOSE) down

logs:  ## Tail dev stack logs
	$(COMPOSE) logs -f --tail=50

ps:  ## Show dev stack status
	$(COMPOSE) ps

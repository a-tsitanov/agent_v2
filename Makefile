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

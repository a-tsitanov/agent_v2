#!/usr/bin/env bash
# Bring up the kb-llamaindex docker stack and print service URLs.
#
# Usage:
#   bash scripts/start.sh
#   bash scripts/start.sh down       # stop everything
#   bash scripts/start.sh logs       # tail logs

set -euo pipefail

cd "$(dirname "$0")/.."

case "${1:-up}" in
  up)
    docker compose up -d
    echo
    echo "Services:"
    echo "  Milvus      → localhost:19530 (gRPC), localhost:9091 (HTTP)"
    echo "  MinIO       → http://localhost:9001 (dev defaults — see your .env)"
    echo "  Postgres    → localhost:5432 (postgres / postgres)"
    echo "  Neo4j       → http://localhost:7474 (dev defaults — see your .env)"
    echo "  Temporal    → http://localhost:8080 (UI), localhost:7233 (gRPC)"
    echo "  LiteLLM     → http://localhost:4000"
    echo
    echo "Canonical LLM profile: OpenAI text-embedding-3-small (+ gpt-4o-mini)."
    echo "Ollama is opt-in — uncomment the Ollama block in .env.example"
    echo "  (gemma4:e4b + nomic-embed-text) and re-run 'ollama pull' as needed."
    echo
    echo "After services are healthy, initialise schemas:"
    echo "  make init        # Postgres tables + MinIO bucket + Temporal search attrs"
    echo "  — or —"
    echo "  make up          # compose up + schema init in one step"
    echo
    echo "Tip: 'make up' does compose up + schema init in one step."
    ;;
  down)
    docker compose down
    ;;
  logs)
    docker compose logs -f --tail=50
    ;;
  ps|status)
    docker compose ps
    ;;
  *)
    echo "usage: $0 {up|down|logs|ps}"
    exit 1
    ;;
esac

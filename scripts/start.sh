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
    echo "  MinIO       → http://localhost:9001 (minioadmin / minioadmin)"
    echo "  Postgres    → localhost:5432 (postgres / postgres)"
    echo "  Neo4j       → http://localhost:7474 (neo4j / changeme)"
    echo "  Temporal    → http://localhost:8080 (UI), localhost:7233 (gRPC)"
    echo "  LiteLLM     → http://localhost:4000"
    echo
    echo "Make sure Ollama is running on the host with required models:"
    echo "  ollama pull qwen3:8b nomic-embed-text"
    echo "  (optional) ollama pull llama3.1:8b   # baseline for R9 eval"
    echo
    echo "After services are healthy:"
    echo "  uv run python -m scripts.setup_db"
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

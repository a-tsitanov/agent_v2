#!/usr/bin/env bash
# End-to-end smoke test for the kb-llamaindex API.
#
# Requires:
#   * `docker compose up -d` — services healthy
#   * `python -m scripts.setup_db` — schema initialized
#   * `uvicorn src.api.main:app --port 8000` running
#   * `taskiq worker src.ingestion.tasks:broker --workers 1` running
#
# Usage:
#   bash scripts/smoke.sh                        # all sections
#   bash scripts/smoke.sh health|search|errors   # one section

set -euo pipefail

API="${API_BASE:-http://localhost:8000}"
KEY="${API_KEY:-dev-local-key}"

section() { echo; echo "=== $1 ==="; }

curl_ok() { curl -sf -H "X-API-Key: ${KEY}" "$@"; }

case "${1:-all}" in
  all|health)
    section "health"
    curl -sf "${API}/health" | jq -e '.status == "ok"' >/dev/null
    echo "ok"
    ;;
esac

case "${1:-all}" in
  all|search)
    section "search hybrid (non-agentic)"
    curl_ok -X POST "${API}/api/v1/search" \
      -H "Content-Type: application/json" \
      -d '{"query": "what is X", "agentic": false}' \
      | jq -e '.answer != null' >/dev/null
    echo "ok"

    section "search agentic"
    curl_ok -X POST "${API}/api/v1/search" \
      -H "Content-Type: application/json" \
      -d '{"query": "find contract details", "agentic": true, "agentic_max_rounds": 2}' \
      | jq -e '.agentic_round_stats != null' >/dev/null
    echo "ok"
    ;;
esac

case "${1:-all}" in
  all|errors)
    section "401 without API key"
    curl -s -o /dev/null -w "%{http_code}\n" -X POST "${API}/api/v1/search" \
      -H "Content-Type: application/json" -d '{"query":"x"}' \
      | grep -q "401" && echo "ok"

    section "403 with bad API key"
    curl -s -o /dev/null -w "%{http_code}\n" -X POST "${API}/api/v1/search" \
      -H "X-API-Key: invalid" \
      -H "Content-Type: application/json" -d '{"query":"x"}' \
      | grep -q "403" && echo "ok"
    ;;
esac

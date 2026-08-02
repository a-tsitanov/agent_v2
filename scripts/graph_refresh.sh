#!/usr/bin/env bash
# Periodic graph maintenance for the kb-llamaindex stack (agent_v2 on .31).
# Fires the two fire-and-forget admin endpoints:
#   1. community detection + summaries  (CommunityBuildWorkflow)
#   2. graph analytics materialize       (AnalyticsMaterializeWorkflow)
# Both run offline on the kb-graph-build queue. An overlap guard skips a
# trigger while a previous run of that workflow type is still Running, so a
# slow cycle never piles up. Scheduled by host crontab every 6h.
set -uo pipefail

LOG=/home/user/projects/agent_v2/logs/graph_refresh.log
mkdir -p "$(dirname "$LOG")"
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# API key stays inside the container env — fetched at runtime, never logged.
KEY=$(docker exec agent_v2-api-1 printenv API_KEYS | cut -d, -f1)

running() { # workflow_type -> count of Running executions
  docker exec agent_v2-temporal-1 temporal workflow count \
    --address temporal:7233 --namespace default \
    --query "WorkflowType=\"$1\" AND ExecutionStatus=\"Running\"" 2>/dev/null \
    | grep -oE 'Total: [0-9]+' | grep -oE '[0-9]+' | head -1
}

trigger() { # label  workflow_type  url
  local label="$1" wf="$2" url="$3" n resp
  n=$(running "$wf"); n=${n:-0}
  if [ "$n" -gt 0 ]; then
    echo "$(ts) SKIP  $label — $n still running" >>"$LOG"
    return
  fi
  resp=$(curl -sS -m 30 -X POST "$url" -H "X-API-Key: $KEY" -w ' http=%{http_code}' 2>&1)
  echo "$(ts) START $label — $resp" >>"$LOG"
}

trigger communities CommunityBuildWorkflow      http://localhost:8000/api/v1/admin/communities/rebuild
trigger analytics   AnalyticsMaterializeWorkflow http://localhost:8000/admin/graph/materialize

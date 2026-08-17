#!/usr/bin/env bash
# Periodic graph maintenance for the kb-llamaindex stack (agent_v2 on .31).
# Fires the two fire-and-forget admin endpoints:
#   1. community detection + summaries  (CommunityBuildWorkflow)
#   2. graph analytics materialize       (AnalyticsMaterializeWorkflow)
# Both run offline on the kb-graph-build queue. An overlap guard skips a
# trigger while a previous run of that workflow type is still Running, so a
# slow cycle never piles up.
#
# Usage: graph_refresh.sh [communities|analytics|all]   (default: all)
#
# 2026-08-05: the two triggers are now SEPARATE cron entries. They used to
# fire in the same second on `0 */6 * * *`, which put a 4h30m community build
# and a 40m betweenness pass (igraph, holds the GIL) on the box at once —
# every such run was followed by a global OOM storm within 0-21 min on this
# 14Gi host. Community build alone is 4h30m, so a 6h cadence also meant heavy
# graph load ~75% of the day. Now nightly and staggered:
#   0 2 * * *  communities   (~02:00-06:30)
#   0 7 * * *  analytics     (~07:00-07:40)
# To go back to twice daily, add +12h copies of both lines — keep them
# staggered and never let a community window overlap an analytics window.
set -uo pipefail

TARGET="${1:-all}"
case "$TARGET" in
  communities|analytics|stats|all) ;;
  *) echo "usage: $0 [communities|analytics|stats|all]" >&2; exit 2 ;;
esac

LOG=/home/user/projects/agent_v2/logs/graph_refresh.log
mkdir -p "$(dirname "$LOG")"
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# API key stays inside the container env — fetched at runtime, never logged.
KEY=$(docker exec agent_v2-api-1 printenv API_KEYS 2>/dev/null | cut -d, -f1)
# The stack is not always up (it gets taken down under memory pressure). Fail
# loudly instead of POSTing an empty X-API-Key and logging a bare 401.
if [ -z "$KEY" ]; then
  echo "$(ts) ABORT $TARGET — cannot read API_KEYS (is agent_v2-api-1 running?)" >>"$LOG"
  exit 1
fi

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

if [ "$TARGET" = communities ] || [ "$TARGET" = all ]; then
  trigger communities CommunityBuildWorkflow http://localhost:8000/api/v1/admin/communities/rebuild
fi
if [ "$TARGET" = analytics ] || [ "$TARGET" = all ]; then
  trigger analytics AnalyticsMaterializeWorkflow http://localhost:8000/admin/graph/materialize
fi

# Nebula's own tag/edge counts. `graph_stats` reads these instead of
# scanning — the counting scans are refused for memory on this space — so
# without a refresh it reports numbers frozen at the last job. Nothing ran
# one until 2026-08-16, and `SHOW STATS` had never had anything to serve.
# ~1s, in storaged, no memory spike, so no overlap guard.
#
# It runs under EVERY target, and `stats` exists so it can be run ALONE.
# Without that target the only way to exercise this was to also fire a
# 40-minute betweenness pass — which is exactly what happened the first
# time it was tested, on a host with 2.4 GB free.
if [ "$TARGET" = stats ] || [ "$TARGET" = communities ] || \
   [ "$TARGET" = analytics ] || [ "$TARGET" = all ]; then
  # NOTE: graph_admin is mounted WITHOUT the /api/v1 prefix (see
  # src/api/main.py) — same as the materialize URL above, unlike the
  # communities one.
  resp=$(curl -sS -m 30 -X POST http://localhost:8000/admin/graph/refresh-stats \
    -H "X-API-Key: $KEY" -w ' http=%{http_code}' 2>&1)
  echo "$(ts) START nebula-stats — $resp" >>"$LOG"
fi

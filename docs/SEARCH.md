# Search subsystem

> Status: refactor complete through R7b (Plan #2, agentic-search). The
> monolithic `src/workflow/search_workflow.py` has been DECOMPOSED into
> the `src/workflow/search/` package — a thin orchestrator + per-mode
> child workflows (sub-query retrieval, global search) + an offline
> community builder — and the legacy monolith has been REMOVED. The new
> package is now the SOLE search path.
>
> **BREAKING (R7b cutover)**: the legacy ReAct endpoints
> `/api/v1/search`, `/api/v1/agent`, `/api/v1/selfrag` and the judge-based
> `/api/v1/legacy/agent` baseline were removed along with the
> `SearchWorkflow` workflow and its exclusive activities
> (`agent_reasoning_step`, `tool_execution`, `distill_observation`).
> Clients must move to `/api/v1/search/{local,global,drift,auto}`.

## Workflows
- `SearchOrchestratorWorkflow` — plan → fan-out → coverage → rerank → synthesize
- `SubQueryRetrievalWorkflow` — plan-execute retrieval for one sub-question
- `GlobalSearchWorkflow` — GraphRAG community map-reduce
- `DriftSearchWorkflow` / `AutoSearchWorkflow` — drift + routed dispatch
- `CommunityBuildWorkflow` — offline GDS Leiden + batch community summaries

## Endpoints (the sole search surface)
- `POST /api/v1/search/local`, `/search/global`, `/search/drift`, `/search/auto`
- `POST /api/v1/admin/communities/rebuild`

(See `docs/superpowers/plans/2026-05-25-agentic-search.md`.)

## Local plan-execute flow (R2, shipped)

`POST /api/v1/search/local` runs `SearchOrchestratorWorkflow` — a thin
plan-execute-synthesize pipeline that **replaces the open-ended ReAct
loop** for local search. No "LLM picks next tool" step: the only LLM
calls are the up-front planner and the final synthesizer.

```
question
  │
  ▼  plan_subquestions  (small "plan" model)
  ├─ "sub A"   ─▶ SubQueryRetrievalWorkflow (child)  ─┐
  ├─ "sub B"   ─▶ SubQueryRetrievalWorkflow (child)  ─┤  asyncio.gather
  └─ …(≤ AGENT_MAX_SUBQUERIES, default 5)            ─┘  (parallel)
                                                          │
                          merge + dedup by chunk_id  ◀────┘
                                                          │
                          coverage gate (R4, bounded)
                                                          │
                          rerank_sources  (R5, unified graph+vector pool)
                                                          │
                          synthesize_answer  (large "synthesis" model,
                                              kb-search-large queue)
                                                          │
                                                          ▼  SearchOutcome
```

- **`query_planner.decompose(question, llm)`** splits a compound
  question into atomic sub-questions; returns `[question]` for atomic
  questions and on ANY planner failure (fail-safe). Robust parsing:
  numbered / bulleted / JSON-array, capped at `AGENT_MAX_SUBQUERIES`.
- **`SubQueryRetrievalWorkflow`** — for ONE sub-question, runs a
  DETERMINISTIC retrieve pipeline (`retrieve_subquestion`: hybrid
  `vector_search` + `graph_search` via `atomic_tools.dispatch`), dedups
  sources by chunk_id. No `agent_reasoning_step`.
- **`SearchOrchestratorWorkflow`** — plan → one child per sub-question
  in parallel → merge+dedup sources by chunk_id → `synthesize_answer`
  on the large tier (`use_synthesis_llm=True`) → returns the
  `SearchOutcome` shape (formerly shared with the now-removed legacy
  `SearchWorkflow`), mapped onto `SearchResponse` by the route handler.

The orchestrator runs on the `kb-search-small` task queue (see
`docs/QUEUES.md`). The R7b cutover removed the legacy ReAct
`SearchWorkflow` that previously shared this queue — the orchestrator is
now the sole local path.

### Multi-hop traversal: `graph_walk` (R3)

`graph_walk` is an EXPLICIT, BOUNDED multi-hop graph tool, distinct from
the default `graph_search` (which stays similarity-based, `path_depth=1`,
UNCHANGED). Use it for connection / chain questions where one hop isn't
enough — "who is connected to X transitively", "что/кто связывает X и Y
через цепочку".

- **Signature**: `graph_walk(start_entity, hops=2, rel_filter=None)`.
  `start_entity` is a real entity name (anchor via `find_entity_by_id` /
  `graph_search` first); `rel_filter` restricts to given relationship
  labels at the QUERY level (empty ⇒ all types).
- **Backend**: `GraphRetriever.awalk()` issues ONE bounded Cypher query
  via the store's `structured_query` — `MATCH (e {name:$name})-[r*1..hops]-(m)`
  with a `WHERE`-clause `rel_filter`, a server-side `LIMIT $node_cap`, and
  an APOC-free fallback path. `hops` is clamped and interpolated as a
  vetted int (Neo4j can't parametrise a var-length bound); everything else
  is a proper param.
- **Hard caps (never unbounded)**: `hops ≤ GRAPH_WALK_MAX_HOPS` (3),
  `≤ GRAPH_WALK_MAX_NODES` (50) entities, `≤ GRAPH_WALK_MAX_EDGES` (100)
  relations. Clamp + truncation are enforced BOTH in the Cypher and again
  in the tool / retriever row-mapping, so the contract holds even against a
  store that ignores the LIMIT. This is what keeps multi-hop from blowing
  up the agent's context window.
- **Returns** the same serialized shape as the other graph tools:
  `{"entities": [...], "relations": [...]}` in the observation, related
  chunks as `sources`.
- **Wiring**: registered in `atomic_tools` (`TOOL_FUNCTIONS`,
  `TOOL_DESCRIPTIONS`, `dispatch()`) and dispatchable on the R2 retrieve
  path via the same `graph_retriever` DI (see `ALLOWED_TOOLS` in
  `retrieve.py`). It is NOT in the default deterministic `_PIPELINE`
  because it needs an explicit `start_entity`. The tool description
  (`TOOL_DESCRIPTIONS["graph_walk"]`) documents WHEN an LLM should pick it
  vs `graph_search`.
- **Auto-seeding in the local retrieve path (R3b)**: rather than waiting
  for an LLM tool-pick, the `retrieve_subquestion` activity now SEEDS
  `graph_walk` deterministically. After `graph_search` runs, the activity
  parses its observation, picks the TOP entity (first non-blank
  `entity_name` — graph_search returns entities in similarity-rank order)
  via the pure helper `top_entity_name(observation) -> str | None`, then
  dispatches `graph_walk` with that `start_entity` and
  `hops=settings.agent.graph_walk_hops`. The walk's chunks are merged into
  the accumulated sources (deduped by `chunk_id`, same `seen` set as the
  pipeline). Gated by `settings.agent.graph_walk_enabled` (default `True`,
  `graph_walk_hops` default `2`). FAIL-OPEN: if `graph_search` returned no
  entities, or parsing / the walk raises for any reason, the seed step is
  skipped and the vector + graph_search results are returned unchanged
  (the activity never raises on the walk). Because parsing is
  non-deterministic, this logic lives in the ACTIVITY, never in a
  `@workflow.run` body.

### Coverage gate (R4)

After the orchestrator merges all sub-question sources (and before the
single `synthesize_answer`), it runs ONE small-tier `coverage_check`
(a SHARED activity retained through the R7b cutover — the legacy ReAct
`SearchWorkflow` that originally introduced it has been removed, but the
activity stays as the orchestrator's gate) asking whether the gathered
evidence FULLY covers the
question. On a verdict of `complete=False` with a non-empty `missing`
gap, the orchestrator issues that gap as ONE extra
`SubQueryRetrievalWorkflow` (deterministic child id `…-cov-1`), merges
its sources back into the pool (dedup by chunk_id), records an extra
step-stat, then synthesizes. This adds the gap-detection the plain
fan-out otherwise lacks for multi-part questions.

```
          merge + dedup by chunk_id
                    │
                    ▼  coverage_check  (small "coverage" model)
            complete? ──yes──▶ synthesize
                    │ no + named gap (and round budget left)
                    ▼
            SubQueryRetrievalWorkflow(gap)  (child, id …-cov-N)
                    │
            re-merge + dedup  ─▶ (re-check, bounded) ─▶ synthesize
```

- **Bounded**: at most `AgentSettings.max_coverage_rounds` (default 1)
  extra sub-questions — no infinite loop even if a gap persists.
- **FAIL-OPEN**: ANY error in the coverage check OR the extra retrieval
  round → proceed straight to synthesis. A flaky completeness call can
  never block the answer. (The `coverage_check` activity is itself
  fail-open, returning `complete=True` on its own internal errors.)
- **Decision logic** lives in pure, Temporal-free helpers
  (`src/workflow/search/_coverage.py`: `should_run_coverage_round`,
  `build_evidence`) so the gap/complete/bound branching is unit-tested
  without a live Temporal env.
- **R7b note**: the legacy ReAct `SearchWorkflow` had a SEPARATE coverage
  mechanism (gap fed back into the reasoning history, bounded by
  `max_coverage_checks`). That workflow — and its `max_coverage_checks`
  knob — were removed in the cutover; only the orchestrator's
  "gap → extra sub-question" gate remains.

### Unified rerank + large-tier synthesis (R5)

After the coverage gate produces the final merged pool — and BEFORE the
single `synthesize_answer` — the orchestrator co-ranks the graph-derived
and vector chunks in ONE unified rerank pass (`rerank_sources`), then
schedules synthesis on a dedicated large-tier queue.

```
            merged graph+vector pool (deduped by chunk_id)
                    │
                    ▼  rerank_sources  (bge cross-encoder, ONE pass)
            reranked top-N  (kb-search-small queue)
                    │
                    ▼  synthesize_answer  (large "synthesis" model)
            FINAL ANSWER     (kb-search-large queue, low concurrency)
```

- **Unified pool**: `rerank_sources` reranks the COMBINED graph+vector
  pool in a single cross-encoder call, so the two retrieval modalities
  are scored against each other (not concatenated by retriever order).
  A chunk surfacing from both modalities is deduped (first wins) before
  reranking — the pure `prepare_rerank_pool` helper
  (`src/workflow/search/activities/rerank.py`) so it's unit-tested
  without loading the ~1 GB bge model. REUSES the existing
  `src/retrieval/reranker.py` (`BAAI/bge-reranker-v2-m3`), process-cached
  via `_search_deps.get_reranker`; `top_n` from
  `TEMPORAL_RERANK_TOP_N` (default 5).
- **FAIL-OPEN**: any rerank error → fall back to the merged pool CAPPED
  to `rerank_top_n` via the pure `cap_synth_sources` helper (never block
  the answer, but never feed synthesis an unbounded pool either — an
  uncapped fallback could blow past `synthesize_answer`'s start_to_close
  timeout). The displayed `SearchOutcome.sources` stays the FULL merged
  pool (citations unchanged); only the synthesis context is trimmed.
- **Large-tier queue**: synthesis is pinned to `kb-search-large` via
  `execute_activity("synthesize_answer", …, task_queue=large_task_queue)`
  with `use_synthesis_llm=True` (large `build_synthesis_llm`). A separate
  low-concurrency `Worker` pool in the same worker process polls that
  queue (see `docs/QUEUES.md`). The call spec is built by the pure
  `build_synthesize_call` helper so the queue + tier routing is
  unit-tested outside Temporal.
- **R7b note**: the small-tier ReAct synthesis path
  (`use_synthesis_llm=False`, no rerank) belonged to the removed legacy
  `SearchWorkflow`; the orchestrator always synthesizes large-tier with
  rerank.
- **Module status (post-cutover cleanup)**: `retrieval/reranker.py` is
  ACTIVE (above). `retrieval/hybrid.py` is NOT wired — a BM25+dense
  experiment candidate (benchmark via `tests/eval/` before integrating).
  Removed as dead post-R7b: `retrieval/{agent,judge,react_agent,
  query_engine,reflective_synth}.py`, the DI `ApiProvider`, and the
  legacy request/telemetry models in `models/search.py`. The reflective
  (Self-RAG) synthesis path was unreachable and removed; to bring it
  back, re-add as an opt-in `OrchestratorParams.reflective` flag and
  benchmark before defaulting on.

### Config knobs (R2)
- `AGENT_MAX_SUBQUERIES` (`AgentSettings.max_subqueries`, default 5) —
  caps the parallel sub-query fan-out.
- `AGENT_COVERAGE_CHECK_ENABLED` (`AgentSettings.coverage_check_enabled`,
  default true) — gates the orchestrator coverage check (R4).
- `AGENT_MAX_COVERAGE_ROUNDS` (`AgentSettings.max_coverage_rounds`,
  default 1) — caps the orchestrator's extra coverage sub-question
  rounds (R4).
- `TEMPORAL_SEARCH_TASK_QUEUE` (default `kb-search-small`) — queue
  hosting the orchestrator +
  sub-query child.

### Config knobs (R5)
- `TEMPORAL_LARGE_TASK_QUEUE` (`TemporalSettings.large_task_queue`,
  default `kb-search-large`) — dedicated queue for the final large-tier
  `synthesize_answer`.
- `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY`
  (`TemporalSettings.large_activity_concurrency`, default 2) — low cap so
  the heavyweight synthesis model isn't dogpiled by parallel sessions.
- `TEMPORAL_RERANK_TOP_N` (`TemporalSettings.rerank_top_n`, default 5) —
  bge cross-encoder top-N for the unified graph+vector rerank pass.

### Offline community build (R6)

**Fully decoupled / offline** — community detection + summarisation runs
on its own `kb-graph-build` queue, NEVER on the query hot path. It exists
to pre-compute the building blocks for a future **global search** mode
(answers over the whole corpus, not just retrieved chunks).

Pipeline (`CommunityBuildWorkflow`, `src/workflow/search/community_wf.py`):

1. **detect** (`detect_communities_activity` → `src/graph/communities.py`)
   — projects the `__Entity__` sub-graph into an in-memory GDS graph
   (Cypher projection, handles the KG extractor's arbitrary relationship
   types), runs **GDS Leiden** (`gds.leiden.stream`), groups members by
   `communityId`, drops communities below `min_size`, and idempotently
   MERGEs `:Community {id, level, member_count}` nodes with
   `(:__Entity__)-[:IN_COMMUNITY]->(:Community)` links. Drops the
   transient projection on the way out. **Fail-safe**: any GDS/Cypher
   error (or no store) → `[]`, never raised through the activity.
2. **summarize** (`summarize_community_activity`) — for each community,
   reads its members' descriptions + inter-member relations and produces a
   short Russian summary via the **small-tier** LLM (`build_llm("retrieve")`
   — never the large synthesis model), persisted on `:Community.summary`
   (idempotent MERGE). Fan-out is bounded by
   `community_summary_parallelism`.

**`:Community` schema** (additive — no existing label/property touched):
- Label `:Community`; props `id` (Leiden communityId), `level` (0 for the
  single-level R6 pass), `member_count`, `summary`, `updated`,
  `summarized_at`.
- Relationship `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`.
- Unique constraint `community_key` on `(id, level)` backs the MERGE.

**Trigger**: admin endpoint `POST /api/v1/admin/communities/rebuild`
(requires `X-API-Key`) starts the workflow and returns its id. A Temporal
Schedule/cron can call the same workflow — the repo has no Schedule wired
yet (see `docs/QUEUES.md`). The query path (orchestrator / local search)
is **unchanged**; nothing in the query path reads `:Community` yet.

> **GDS note**: the exact `gds.graph.project` / `gds.leiden.stream` /
> `gds.graph.drop` Cypher is written per the Neo4j GDS 2.x API but is
> **unverified against a live GDS install** — there is no Neo4j/GDS in the
> dev sandbox, so all tests mock the store + GDS rows. Validate the Cypher
> against the live GDS version before relying on it in production. All
> GDS/Cypher strings are isolated as constants at the top of
> `src/graph/communities.py` to make that fix a one-file change.

### Config knobs (R6)
- `TEMPORAL_GRAPH_BUILD_TASK_QUEUE`
  (`TemporalSettings.graph_build_task_queue`, default `kb-graph-build`) —
  dedicated offline queue for the community build.
- `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY`
  (`TemporalSettings.graph_build_activity_concurrency`, default 2) — low
  cap so a rebuild's summary burst doesn't flood the small-tier proxy.
- `TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM`
  (`TemporalSettings.community_summary_parallelism`, default 4) — bounded
  per-community summarize fan-out inside the workflow.
- `TEMPORAL_COMMUNITY_MIN_SIZE`
  (`TemporalSettings.community_min_size`, default 3) — communities smaller
  than this are ignored (noise / disconnected pairs).

## Query routing + GraphRAG global search (R7a, shipped)

R7a adds query **routing** and a GraphRAG **global** search that
map-reduces over the R6 community summaries, plus per-type endpoints.
(Originally shipped additively alongside the legacy ReAct routes; those
legacy routes + `SearchWorkflow` were removed in the R7b cutover, leaving
the routed `/api/v1/search/{local,global,drift,auto}` surface as the
sole search path.)

### Routing (`route_query`)

`route_query` (`src/workflow/search/activities/route.py`, small `route`
model) classifies a question into one of three modes:

- **local** — specific / factual ("who is X", "where does Y work") →
  the R2–R5 plan-execute flow (`SearchOrchestratorWorkflow`).
- **global** — corpus-level / thematic / aggregate ("what are the main
  themes", "overall trends") → `GlobalSearchWorkflow`.
- **drift** — complex / mixed (needs both specific facts AND a broad
  overview) → `DriftSearchWorkflow`.

**Fail-safe**: any LLM error or unparseable reply → `"local"` (the
cheapest, safest mode). The parse/classify mapping is the pure
`classify_route` helper (unit-tested without Temporal/LLM).

### Global search (`GlobalSearchWorkflow`)

GraphRAG **map-reduce** over the stored `:Community.summary` texts —
answers corpus-level questions without retrieving individual chunks:

```
question
  │
  ▼  map_communities      (read :Community.summary for level, ranked
  │                         by query overlap, capped at max_communities)
  ├─ community 1  ─▶ map_community_partial (small tier)  ─┐
  ├─ community 2  ─▶ map_community_partial (small tier)  ─┤  asyncio.gather
  └─ …(≤ AGENT_GLOBAL_MAX_COMMUNITIES)                   ─┘  (bounded by
                                                              map_parallelism)
        off-topic communities self-drop ('НЕТ' → score 0)
                                                          │
        partials → synthesis context (one node per       │
        surviving community, chunk_id = "community:<id>") │
                                                          ▼
        REDUCE: synthesize_answer  ── pinned to kb-search-large,
                                       use_synthesis_llm=True (R5 pattern)
```

- **MAP** runs on `kb-search-small` (small `retrieve`-tier model, bounded
  parallelism). **REDUCE** is the existing `synthesize_answer` pinned to
  `large_task_queue` with `use_synthesis_llm=True` — exactly the local
  orchestrator's large-tier synthesis path.
- Map-spec / reduce-context / reduce-call assembly are pure helpers
  (`build_map_specs`, `partials_to_sources`, `build_reduce_call`) so the
  map-reduce wiring is unit-tested without a live Temporal env.
- Fail-safe: store/LLM errors yield empty results (no-evidence answer)
  rather than raising.

### Drift search (`DriftSearchWorkflow`)

A **bounded** local-then-global mechanism (no open-ended loop): run the
local plan-execute orchestrator FIRST (one pass — concrete chunk
evidence), then run `GlobalSearchWorkflow` with `drift_mode=True` SEEDED
with the local sources. In drift mode the global REDUCE merges the local
sources AHEAD of the community partials (dedup by chunk_id) so local
evidence leads and the community context broadens it; the outcome is
labelled `"drift"`. Exactly one local pass + one global pass.

### Auto search (`AutoSearchWorkflow`)

`route_query` classifies the question, then dispatches to the matching
flow as a child workflow (`dispatch_for_route` — pure, fallback `local`).

### Endpoints

| Endpoint | Workflow | Orchestration queue | Synthesis |
| --- | --- | --- | --- |
| `POST /api/v1/search/local` | `SearchOrchestratorWorkflow` | `kb-search-small` | `kb-search-large` |
| `POST /api/v1/search/global` | `GlobalSearchWorkflow` | `kb-search-small` | `kb-search-large` (REDUCE) |
| `POST /api/v1/search/drift` | `DriftSearchWorkflow` | `kb-search-small` | `kb-search-large` (global REDUCE) |
| `POST /api/v1/search/auto` | `AutoSearchWorkflow` | `kb-search-small` | per chosen flow |
| `POST /api/v1/admin/communities/rebuild` | `CommunityBuildWorkflow` | `kb-graph-build` | n/a (offline build) |

All require `X-API-Key` and reuse the `SearchRequest` / `SearchResponse`
shapes (`SearchResponse.mode` carries `local`/`global`/`drift`). These
four endpoints (plus the admin rebuild trigger) are the COMPLETE search
surface after the R7b cutover.

### Config knobs (R7a)
- `AGENT_GLOBAL_MAX_COMMUNITIES` (`AgentSettings.global_max_communities`,
  default 20) — caps how many community summaries enter the global MAP
  step (bounds fan-out + LLM load on a large corpus).
- `AGENT_GLOBAL_MAP_PARALLELISM` (`AgentSettings.global_map_parallelism`,
  default 4) — bounded per-community MAP concurrency inside
  `GlobalSearchWorkflow`.
- The `route` role tier is configurable via `LITELLM_ROLE_TIERS`
  (defaults to `small`, per `_DEFAULT_ROLE_TIERS`).

## Legacy cutover (R7b, shipped — BREAKING)

The legacy ReAct `SearchWorkflow`, the `/api/v1/search`, `/agent`,
`/selfrag` endpoints, the judge-based `/api/v1/legacy/agent` baseline,
and the legacy-only activities (`agent_reasoning_step`, `tool_execution`,
`distill_observation`) + their exclusive contracts (`SearchParams`,
`ReasoningParams`, `AgentDecision`, `ToolCall*`, `Distill*`, `ToolSpec`,
`SerializedMessage`/`SerializedToolCall`, the `Relevance` alias) were
REMOVED. The MCP-1 `kb_search` tool now submits
`SearchOrchestratorWorkflow` (local plan-execute) instead.

SHARED activities/contracts the new path depends on were KEPT:
`synthesize_answer`, `coverage_check`, `SerializedNode`,
`SynthesizeParams`/`SynthesizeResult`, `CoverageParams`/`CoverageResult`,
`AgenticStepStatDict`, `SearchOutcome`, and the `node_to_serialized` /
`serialized_to_node` serde helpers.

> **REQUIRED before merge to main**: live-Temporal parity verification of
> `/api/v1/search/{local,global,drift,auto}` against a real environment.
> The unit/integration suites here run with Temporal mocked / skip-gated.

# Search subsystem

> Status: refactor in progress (Plan #2, agentic-search). The monolithic
> `src/workflow/search_workflow.py` is being decomposed into a
> `src/workflow/search/` package: a thin orchestrator + per-mode child
> workflows (sub-query retrieval, global search) + an offline community
> builder. Legacy search remains the default until cutover.

## Workflows (target)
- `SearchOrchestratorWorkflow` — route → plan → fan-out → rerank → coverage → synthesize
- `SubQueryRetrievalWorkflow` — plan-execute retrieval for one sub-question
- `GlobalSearchWorkflow` — GraphRAG community map-reduce
- `CommunityBuildWorkflow` — offline GDS Leiden + batch community summaries

## Endpoints (target)
- `POST /api/v1/search/local`, `/search/global`, `/search/drift`, `/search/auto`
- `POST /api/v1/admin/communities/rebuild`

(Populated phase by phase; see `docs/superpowers/plans/2026-05-25-agentic-search.md`.)

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
  on the large tier (`use_synthesis_llm=True`) → returns the same
  `SearchOutcome` shape as the legacy `SearchWorkflow`, mapped onto
  `SearchResponse` by the route handler.

The legacy ReAct `SearchWorkflow` + `/api/v1/search` stay UNTOUCHED and
remain the default until cutover (parity window). Both flows share the
`kb-search-small` task queue (see `docs/QUEUES.md`).

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
  because it needs an explicit `start_entity` — that belongs to a future
  connection-aware planner / LLM tool-pick step. The tool description
  (`TOOL_DESCRIPTIONS["graph_walk"]`) documents WHEN an LLM should pick it
  vs `graph_search`.

### Coverage gate (R4)

After the orchestrator merges all sub-question sources (and before the
single `synthesize_answer`), it runs ONE small-tier `coverage_check`
(the SAME activity the legacy ReAct `SearchWorkflow` uses — reused, not
re-implemented) asking whether the gathered evidence FULLY covers the
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
- **Legacy path unchanged**: the ReAct `SearchWorkflow` keeps its own
  coverage gate (gap fed back into the reasoning history, bounded by
  `max_coverage_checks`) — a SEPARATE mechanism from the orchestrator's
  "gap → extra sub-question".

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
- **FAIL-OPEN**: any rerank error → fall back to the unranked merged
  pool (never block the answer). The displayed `SearchOutcome.sources`
  stays the FULL merged pool (citations unchanged); only the synthesis
  context is trimmed to the reranked top-N.
- **Large-tier queue**: synthesis is pinned to `kb-search-large` via
  `execute_activity("synthesize_answer", …, task_queue=large_task_queue)`
  with `use_synthesis_llm=True` (large `build_synthesis_llm`). A separate
  low-concurrency `Worker` pool in the same worker process polls that
  queue (see `docs/QUEUES.md`). The call spec is built by the pure
  `build_synthesize_call` helper so the queue + tier routing is
  unit-tested outside Temporal.
- **Legacy path unchanged**: the ReAct `SearchWorkflow` still synthesizes
  on the small tier (`use_synthesis_llm=False`) on `kb-search-small`,
  with no rerank step.

### Config knobs (R2)
- `AGENT_MAX_SUBQUERIES` (`AgentSettings.max_subqueries`, default 5) —
  caps the parallel sub-query fan-out.
- `AGENT_COVERAGE_CHECK_ENABLED` (`AgentSettings.coverage_check_enabled`,
  default true) — gates the orchestrator coverage check (R4); REUSED
  from the legacy ReAct knob.
- `AGENT_MAX_COVERAGE_ROUNDS` (`AgentSettings.max_coverage_rounds`,
  default 1) — caps the orchestrator's extra coverage sub-question
  rounds (R4); distinct from the ReAct `max_coverage_checks`.
- `TEMPORAL_SEARCH_TASK_QUEUE` (default `kb-search-small`) — queue
  hosting both the legacy ReAct workflow and the new orchestrator +
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

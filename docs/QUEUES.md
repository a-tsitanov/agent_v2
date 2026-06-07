# Temporal task queues

The worker process (`uv run python -m src.workflow.worker`) hosts
several Worker pools against the same Temporal client, each polling its
own task queue so GPU / LLM pressure can be capped independently.

| Queue (config field) | Default | Hosts | Concurrency cap |
| --- | --- | --- | --- |
| `task_queue` | `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding activities | `TEMPORAL_ACTIVITY_CONCURRENCY` (4) |
| `llm_task_queue` | `kb-ingest-llm` | `extract_kg` ONLY (the extract lane) | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` (18) |
| `merge_task_queue` | `kb-ingest-merge` | `GraphBuildWorkflow` + `merge_and_resolve` / `build_property_graph` (the merge lane) | `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` (14) |
| `search_task_queue` | `kb-search-small` | `SearchOrchestratorWorkflow` + `SubQueryRetrievalWorkflow` + `GlobalSearchWorkflow` + `DriftSearchWorkflow` + `AutoSearchWorkflow` + their activities (plan / retrieve / coverage_check / rerank_sources / route / map_communities / documents_for_communities) | `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) |
| `large_task_queue` | `kb-search-large` | `synthesize_answer` ONLY (final large-tier synthesis, Search R5) — activities-only Worker, no workflows | `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (2) |
| `graph_build_task_queue` | `kb-graph-build` | `CommunityBuildWorkflow` + `detect_communities_activity` / `summarize_community_activity` (OFFLINE GDS-Leiden community build, Search R6) | `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY` (2) |
| `wiki.task_queue` | `kb-wiki` | `WikiSweepWorkflow` + `select_dirty_entities` / `write_entity_article` (continuous per-entity MediaWiki article editor) | `WIKI_ACTIVITY_CONCURRENCY` (4) |

## Queue caps vs the LLMPool — who actually owns concurrency

Two independent limiters apply to every LLM-bound activity:

1. **Temporal per-queue `max_concurrent_activities`** (the table above) —
   how many activities of that queue a worker will run at once. This is an
   **isolation** boundary: it keeps one workload (e.g. an `extract_kg`
   burst) from occupying every slot a sibling lane (merge) needs.
2. **The per-process `LLMPool`** (`src/retrieval/llm_pool.py`,
   `LLM_POOL_*`) — the real GPU/upstream concurrency arbiter, shared
   across ingest AND search in the same process. It enforces a
   hierarchical limit: a small-tier global total
   (`LLM_POOL_TIER_SMALL_TOTAL`, default 25) and a large-tier total
   (`LLM_POOL_TIER_LARGE_TOTAL`, default 8), combined with per-role lane
   ceilings (`LLM_POOL_LANE_CAPS`: extraction 18, judge 14, search 14,
   plan 4, route 2, retrieve 4, synthesis 8). Small-tier lanes
   deliberately over-subscribe (sum of ceilings > tier total) so one role
   can fill the GPU while none monopolizes it; `LLM_POOL_JUDGE_FLOOR`
   (default 7) reserves capacity so merge/judge never starves under an
   extraction flood (sizing rule: extraction ceiling ≤
   `tier_small_total − judge_floor`).

**The Temporal caps must be ≥ the matching pool lane ceiling** so the pool
binds first — otherwise Temporal throttles before the pool can arbitrate.
That is exactly why the `kb-ingest-llm` / `kb-ingest-merge` caps were
raised to 18 / 14 (matching the extraction / judge lane ceilings) rather
than left at the old concurrency-1.

## Dedicated merge queue: `kb-ingest-merge`

`extract_kg` and the merge stage (`GraphBuildWorkflow` →
`merge_and_resolve` + `build_property_graph`) used to share the single
`kb-ingest-llm` queue at concurrency 1. When many documents ingest at
once, a burst of `extract_kg` tasks fills that FIFO queue and a
document's merge — enqueued *behind* all the pending extracts — starves
(head-of-line blocking). The vector half completes fast but the graph
half waits out the whole extract backlog.

**Fix**: merge gets its own queue + Worker pool (`kb-ingest-merge`). The
parent `DocumentIngestWorkflow` starts the `GraphBuildWorkflow` child on
`merge_task_queue`; its `merge_and_resolve` / `build_property_graph`
activities carry NO `task_queue` override, so they inherit the child's
queue and ride the merge lane automatically. `extract_kg` stays pinned
to `kb-ingest-llm`. Now extract and merge poll independent queues and
interleave instead of serialising through one FIFO.

**LLM concurrency is now owned by the per-process LLMPool** (`src/retrieval/llm_pool.py`), not by Temporal caps alone. The Temporal `llm`/`merge` caps were raised to 18/14 so the pool binds first — they must be ≥ the pool's per-role lane ceilings or Temporal would throttle before the pool gets a chance to arbitrate. The pool enforces a hierarchical limit: a small-tier global total (default 25, `LLM_POOL_TIER_SMALL_TOTAL`) combined with per-role lane ceilings (`LLM_POOL_LANE_CAPS`), so extract and merge interleave dynamically and the GPU stays utilized without either role monopolizing capacity. `build_property_graph` remains registered in `MAIN_ACTIVITIES` too (Neo4j-write, not GPU-bound) so single-pool deployments still work.

**Operator action on upgrade**: set `TEMPORAL_MERGE_TASK_QUEUE` /
`TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` if non-default (keep them ≥ the
corresponding pool lane ceiling), and restart the worker so it polls the
new `kb-ingest-merge` queue.

## Offline graph-community build queue: `kb-graph-build` (Search R6)

Fully **decoupled / offline** — this queue is NEVER touched on the query
hot path. The worker process hosts a dedicated `Worker` pool on
`kb-graph-build` (same process, same Temporal client) running
`CommunityBuildWorkflow` and its two activities:

- `detect_communities_activity` — runs Neo4j **GDS Leiden** over the
  `__Entity__` sub-graph (Cypher projection → `gds.leiden.stream`),
  groups members by `communityId`, drops communities below
  `TEMPORAL_COMMUNITY_MIN_SIZE` (3), and idempotently MERGEs
  `:Community {id, level, member_count}` nodes linked to their members via
  `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`.
- `summarize_community_activity` — for one community, summarises its
  members (+ inter-member relations) via the **small-tier** LLM
  (`build_llm("retrieve")`) and persists the result on
  `:Community.summary` (idempotent MERGE). Batchable; the workflow fans
  out one call per community with bounded parallelism
  (`TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM`, default 4).

**Triggers** (no query path):
- Admin endpoint `POST /api/v1/admin/communities/rebuild` (primary) —
  starts the workflow on `kb-graph-build`, returns the workflow id.
- Optional **Temporal Schedule** — the repo does not yet configure any
  Temporal Schedule, so this is currently a manual/admin-triggered build.
  To run it on a cron, create a Schedule (e.g. via `tctl schedule create`
  or `client.create_schedule`) that starts `CommunityBuildWorkflow` on
  `kb-graph-build` with a `DetectCommunitiesParams(min_size=…)` input.

**Idempotent / incremental**: re-running refreshes summaries and
membership on the existing `:Community` nodes (MERGE keyed on
`(id, level)`) — it never duplicates communities. The query path is
unchanged; these summaries are written for a future global-search phase.

**Concurrency**: kept low (`TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY`,
default 2) so a rebuild's summary burst doesn't flood the small-tier LLM
proxy. **Operator action on upgrade**: restart the worker so it polls the
new `kb-graph-build` queue.

## Model tier ↔ queue mapping

| Tier | Model | Queue | Why |
| --- | --- | --- | --- |
| small | search-role LLM (`build_search_llm`) | `kb-search-small` | planner, sub-query retrieval, coverage check, unified rerank — cheap, parallel-friendly |
| large | synthesis LLM (`build_synthesis_llm`) | `kb-search-large` | one heavyweight final synthesis per session — capped LOW so it never serves many parallel sessions |

## Large-tier synthesis queue: `kb-search-large` (Search R5)

`SearchOrchestratorWorkflow` itself still lives on `kb-search-small`, but
it pins the final `synthesize_answer` to `kb-search-large` via
`workflow.execute_activity("synthesize_answer", …, task_queue=settings.temporal.large_task_queue)`.
The worker process hosts a **separate `Worker` pool** on
`kb-search-large` (same process, same Temporal client) registering ONLY
the `synthesize_answer` activity, capped at
`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (default 2). This isolates the
expensive synthesis model on its own low-concurrency pool so concurrent
search sessions don't dogpile it, while the cheap small-tier work
(plan / retrieve / coverage / rerank) keeps its own higher concurrency
on `kb-search-small`.

The pre-synthesis **unified rerank** (`rerank_sources`) runs on
`kb-search-small` — the bge cross-encoder is cheap relative to the large
synthesis LLM, so it doesn't warrant the low-concurrency queue.

**Operator action on upgrade**: set `TEMPORAL_LARGE_TASK_QUEUE` /
`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` if non-default, and restart the
worker so it polls the new queue. (The legacy ReAct `SearchWorkflow` that
once synthesized on the small tier was removed in the R7b cutover; the
plan-execute orchestrator always synthesizes large-tier on
`kb-search-large`.)

## Search queue rename: `kb-search-llm` → `kb-search-small` (Search R2)

The search queue default was renamed from `kb-search-llm` to
`kb-search-small`. The queue now hosts the small-tier plan-execute flow
(planner + parallel sub-query retrieval) in addition to the legacy ReAct
workflow, so the name reflects the dominant model **tier** rather than
"any LLM". The large-tier final synthesis still happens *inside* a
`synthesize_answer` activity on this same queue (no separate
`kb-search-large` queue yet — that arrives in a later phase).

**Operator action on upgrade**: update `TEMPORAL_SEARCH_TASK_QUEUE` if
it was pinned to the old value, and restart the worker so it polls the
new queue name. In-flight workflows on the old queue drain on the old
worker; new submissions go to `kb-search-small`.

### Activities registered on `kb-search-small`
- Shared (`SEARCH_ACTIVITIES`): `coverage_check`, `synthesize_answer`.
  (The legacy ReAct activities `agent_reasoning_step`, `tool_execution`,
  `distill_observation` were removed in the R7b cutover.)
- Search-v2 (`SEARCH_V2_ACTIVITIES`): `plan_subquestions`,
  `retrieve_subquestion`, `rerank_sources`, `route_query`,
  `map_communities`, `map_community_partial`, `documents_for_communities`.

The orchestrator reuses `synthesize_answer` for the final answer, so no
synthesis activity is duplicated.

## Continuous wiki editor queue: kb-wiki

The worker hosts a `kb-wiki` Worker pool running `WikiSweepWorkflow` and its two activities: `select_dirty_entities` (queries Neo4j for entities flagged `wiki_dirty=true`) and `write_entity_article` (generates and writes the per-entity MediaWiki article section). Ingest marks touched entities `wiki_dirty` via a best-effort hook immediately after graph writes; a Temporal Schedule (`scripts/setup_wiki_schedule.py`) or the admin route `POST /admin/wiki/rebuild` starts the sweep, which regenerates each dirty entity's bot-managed MediaWiki article section from the graph (grounded and cited, drift-free) and skips unchanged entities via a subgraph hash. The feature is opt-in via `WIKI_ENABLED`. Unlike every other queue here, the wiki queue's config lives on `WikiSettings` (env prefix `WIKI_`), NOT `TemporalSettings` — the queue name is `WIKI_TASK_QUEUE` (default `kb-wiki`) and concurrency is capped via `WIKI_ACTIVITY_CONCURRENCY` (default 4). Article generation rides the LLMPool synthesis lane so it shares the same hierarchical GPU budget as search synthesis. **Operator action on upgrade**: restart the worker so it polls the new `kb-wiki` queue.

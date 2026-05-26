# Temporal task queues

The worker process (`uv run python -m src.workflow.worker`) hosts
several Worker pools against the same Temporal client, each polling its
own task queue so GPU / LLM pressure can be capped independently.

| Queue (config field) | Default | Hosts | Concurrency cap |
| --- | --- | --- | --- |
| `task_queue` | `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding activities | `TEMPORAL_ACTIVITY_CONCURRENCY` (4) |
| `llm_task_queue` | `kb-ingest-llm` | `extract_kg` ONLY (the extract lane) | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` (1) |
| `merge_task_queue` | `kb-ingest-merge` | `GraphBuildWorkflow` + `merge_and_resolve` / `build_property_graph` (the merge lane) | `TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` (1) |
| `search_task_queue` | `kb-search-small` | `SearchWorkflow` (legacy ReAct) **and** `SearchOrchestratorWorkflow` + `SubQueryRetrievalWorkflow` (R2 plan-execute) + their activities (plan / retrieve / coverage_check / **rerank_sources**) | `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) |
| `large_task_queue` | `kb-search-large` | `synthesize_answer` ONLY (final large-tier synthesis, Search R5) — activities-only Worker, no workflows | `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (2) |
| `graph_build_task_queue` | `kb-graph-build` | `CommunityBuildWorkflow` + `detect_communities_activity` / `summarize_community_activity` (OFFLINE GDS-Leiden community build, Search R6) | `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY` (2) |

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

**Tradeoff — ~2 concurrent LLM tasks**: with both lanes capped at
concurrency 1, up to two LLM tasks can be in flight at once (one extract
+ one merge). This was confirmed acceptable — the GPU/proxy is sized for
~2 concurrent LLM calls. Operators on a tighter budget should keep both
caps at 1 (the default); raising either multiplies the in-flight LLM
load. `build_property_graph` remains registered in `MAIN_ACTIVITIES`
too (Neo4j-write, not GPU-bound) so single-pool deployments still work.

**Operator action on upgrade**: set `TEMPORAL_MERGE_TASK_QUEUE` /
`TEMPORAL_MERGE_ACTIVITY_CONCURRENCY` if non-default, and restart the
worker so it polls the new `kb-ingest-merge` queue.

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
worker so it polls the new queue. The legacy ReAct `SearchWorkflow`
synthesis path is UNCHANGED — it still synthesizes on the small tier on
`kb-search-small` (`use_synthesis_llm=False`).

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
- Legacy (`SEARCH_ACTIVITIES`): `agent_reasoning_step`, `tool_execution`,
  `distill_observation`, `coverage_check`, `synthesize_answer`.
- R2 (`SEARCH_V2_ACTIVITIES`): `plan_subquestions`, `retrieve_subquestion`.

The orchestrator reuses `synthesize_answer` for the final answer, so no
synthesis activity is duplicated.

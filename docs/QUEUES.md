# Temporal task queues

The worker process (`uv run python -m src.workflow.worker`) hosts
several Worker pools against the same Temporal client, each polling its
own task queue so GPU / LLM pressure can be capped independently.

| Queue (config field) | Default | Hosts | Concurrency cap |
| --- | --- | --- | --- |
| `task_queue` | `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding activities | `TEMPORAL_ACTIVITY_CONCURRENCY` (4) |
| `llm_task_queue` | `kb-ingest-llm` | `GraphBuildWorkflow` + `extract_kg` / `merge_and_resolve` / `build_property_graph` | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` (1) |
| `search_task_queue` | `kb-search-small` | `SearchWorkflow` (legacy ReAct) **and** `SearchOrchestratorWorkflow` + `SubQueryRetrievalWorkflow` (R2 plan-execute) + their activities (plan / retrieve / coverage_check / **rerank_sources**) | `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) |
| `large_task_queue` | `kb-search-large` | `synthesize_answer` ONLY (final large-tier synthesis, Search R5) — activities-only Worker, no workflows | `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` (2) |

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

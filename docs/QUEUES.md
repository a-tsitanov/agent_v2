# Temporal task queues

The worker process (`uv run python -m src.workflow.worker`) hosts
several Worker pools against the same Temporal client, each polling its
own task queue so GPU / LLM pressure can be capped independently.

| Queue (config field) | Default | Hosts | Concurrency cap |
| --- | --- | --- | --- |
| `task_queue` | `kb-ingest` | `DocumentIngestWorkflow` + IO/embedding activities | `TEMPORAL_ACTIVITY_CONCURRENCY` (4) |
| `llm_task_queue` | `kb-ingest-llm` | `GraphBuildWorkflow` + `extract_kg` / `merge_and_resolve` / `build_property_graph` | `TEMPORAL_LLM_ACTIVITY_CONCURRENCY` (1) |
| `search_task_queue` | `kb-search-small` | `SearchWorkflow` (legacy ReAct) **and** `SearchOrchestratorWorkflow` + `SubQueryRetrievalWorkflow` (R2 plan-execute) + their activities | `TEMPORAL_SEARCH_ACTIVITY_CONCURRENCY` (4) |

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

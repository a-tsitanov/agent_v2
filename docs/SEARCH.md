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
                          synthesize_answer  (large "synthesis" model)
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

### Config knobs (R2)
- `AGENT_MAX_SUBQUERIES` (`AgentSettings.max_subqueries`, default 5) —
  caps the parallel sub-query fan-out.
- `TEMPORAL_SEARCH_TASK_QUEUE` (default `kb-search-small`) — queue
  hosting both the legacy ReAct workflow and the new orchestrator +
  sub-query child.

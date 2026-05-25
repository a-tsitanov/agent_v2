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

### Config knobs (R2)
- `AGENT_MAX_SUBQUERIES` (`AgentSettings.max_subqueries`, default 5) —
  caps the parallel sub-query fan-out.
- `TEMPORAL_SEARCH_TASK_QUEUE` (default `kb-search-small`) — queue
  hosting both the legacy ReAct workflow and the new orchestrator +
  sub-query child.

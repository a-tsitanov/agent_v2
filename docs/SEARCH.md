# Search subsystem — deep reference

> The DEEP narrative reference for how a query becomes an answer. For the
> at-a-glance flow diagrams (Mermaid + rendered D2) see
> [`SEARCH-FLOW.md`](SEARCH-FLOW.md); for the one-paragraph feature
> summaries and env-var quick-reference see
> [`FEATURES.md`](FEATURES.md#2-search). Usage/runbook:
> [`runbook/search-usage.md`](runbook/search-usage.md). Queue topology:
> [`QUEUES.md`](QUEUES.md).

Search is a set of **durable Temporal workflows** submitted from
`src/api/routes/search_v2.py`. Every mode returns the same internal
`SearchOutcome`, projected onto the public `SearchResponse` by the route
handler (`_outcome_to_response`). There is no open-ended ReAct "LLM picks
the next tool" loop anywhere in the live path — every retrieval pipeline
is deterministic; the only LLM calls are the router, planner, the
per-community/sub-question small-tier steps, and the final synthesizer.

## Entry surface

`src/api/routes/search_v2.py` is the **sole** HTTP search surface. All
routes require `X-API-Key` and consume the shared `SearchRequest`
(`src/models/search.py`: `query`, `top_k`, `history`, plus
backward-compatible filter fields). The mode is chosen by the *endpoint*,
not a request field.

| Endpoint | Workflow | Orchestration queue | Synthesis tier |
| --- | --- | --- | --- |
| `POST /api/v1/search/local` | `SearchOrchestratorWorkflow` | `kb-search-small` | `kb-search-large` |
| `POST /api/v1/search/global` | `GlobalSearchWorkflow` | `kb-search-small` | `kb-search-large` (REDUCE) |
| `POST /api/v1/search/drift` | `DriftSearchWorkflow` | `kb-search-small` | `kb-search-large` (global REDUCE) |
| `POST /api/v1/search/auto` | `AutoSearchWorkflow` | `kb-search-small` | per chosen flow |
| `POST /api/v1/admin/communities/rebuild` | `CommunityBuildWorkflow` | `kb-graph-build` | n/a (offline build) |

Workflows are started on `settings.temporal.search_task_queue`
(`kb-search-small`) with `ALLOW_DUPLICATE` id-reuse; the route awaits
`handle.result()` and maps the outcome. `SearchResponse.mode` carries the
effective mode (`local` / `global` / `drift`).

> Two MCP surfaces also reach search: MCP-1's `kb_search` submits
> `SearchOrchestratorWorkflow` (local plan-execute); MCP-2
> (`src/mcp/tools_server.py`) exposes the atomic retrieval tools
> in-process with **per-call** overrides — see
> [Per-call depth/hops](#per-call-depthhops-mcp-layer).

---

## Local — plan-execute (`SearchOrchestratorWorkflow`)

`src/workflow/search/orchestrator.py`. A thin plan → fan-out → coverage →
rerank → synthesize coordinator. The only LLM calls are the up-front
planner and the final synthesizer; everything between is deterministic
retrieval.

```
question (+ optional history)
  │
  ▼  0. contextualize_query        (only if history present + enabled)
  │     follow-up → standalone question  (model_copy(query=…))
  ▼  1. plan_subquestions          (small "plan" model)
  ├─ "sub A" ─▶ SubQueryRetrievalWorkflow (child) ─┐
  ├─ "sub B" ─▶ SubQueryRetrievalWorkflow (child) ─┤  asyncio.gather
  └─ …(≤ max_subqueries, default 5)               ─┘  (parallel)
                                                      │
                  2. merge + dedup by chunk_id  ◀─────┘
                                                      │
                  3. coverage gate (bounded loop)
                                                      │
                  4. rerank_sources (bge cross-encoder, ONE pass)
                                                      │
                  5. synthesize_answer (large tier, kb-search-large)
                                                      ▼  SearchOutcome (mode="local")
```

### 0. Contextualisation (conversation history)

When `params.history` is non-empty **and** `params.contextualize_enabled`
(resolved at submit time from `AGENT_CONVERSATION_HISTORY_ENABLED`), the
orchestrator runs the `contextualize_query` activity
(`activities/contextualize.py`) FIRST. It rewrites the follow-up into a
standalone question using the recent turns (small `route`-tier model,
`/no_think`), bounded by `history_max_turns` / `history_max_chars`, then
`params.model_copy(query=…)` makes the *entire* downstream pipeline use
the standalone query with no other edits.

- **Replay-safe**: the enable decision is captured on the params at submit
  time (`contextualize_enabled`), not read from config inside the
  workflow, so a config change can't break a replaying workflow.
- **Fail-open**: empty history, no usable turns, or any LLM error returns
  the original query unchanged. Inert (skipped) when history is empty, so
  single-shot callers are unaffected.
- Client-managed history (no server sessions) keeps search stateless.

### 1. Plan — `plan_subquestions`

The small "plan" model decomposes a compound question into atomic
sub-questions (numbered / bulleted / JSON-array parsing, capped at
`max_subqueries`). Atomic questions and **any** planner failure return
`[query]` (fail-safe — search never blocks on the planner).

### 2. Fan-out — `SubQueryRetrievalWorkflow`

One child workflow per sub-question, run in parallel via `asyncio.gather`
over `execute_child_workflow` (deterministic child ids `…-sub-{i}`). Each
child (`subquery_wf.py`) invokes the single `retrieve_subquestion`
activity and dedups its sources by `chunk_id`. No agent / tool-selection
LLM call — the plan fixed the tools up front. See
[The deterministic retrieve pipeline](#the-deterministic-retrieve-pipeline).

The orchestrator unions all children's sources and dedups by `chunk_id`
(`merge_subquery_sources`). One step-stat is recorded per child for
telemetry (reuses the `AgenticStepStatDict` shape so the response model
maps unchanged).

### 3. Coverage gate (bounded loop)

After the merge, while `coverage_check_enabled` and there is round budget
left (`max_coverage_rounds`, default 1), the orchestrator runs ONE
small-tier `coverage_check` over the gathered evidence
(`build_evidence(merged)`). The pure helpers
`should_run_coverage_round` / `build_evidence`
(`src/workflow/search/_coverage.py`) decide the branch:

- **complete** (no gap) → break to rerank.
- **named gap + budget left** → issue that gap as ONE extra
  `SubQueryRetrievalWorkflow` (child id `…-cov-{n}`), re-merge its sources
  (dedup by `chunk_id`), decrement the budget, append a coverage
  step-stat, and re-check (still bounded).

This adds gap-detection the plain fan-out lacks for multi-part questions.

- **Bounded**: at most `max_coverage_rounds` extra rounds — no infinite
  loop even if a gap persists.
- **Fail-open**: any error in the check OR the extra retrieval round →
  break straight to synthesis. The `coverage_check` activity is itself
  fail-open (returns `complete=True` on its own internal errors), so a
  flaky completeness call can never block the answer.

### 4. Unified rerank — `rerank_sources`

Before synthesis the orchestrator co-ranks the **combined** graph-derived
and vector chunks in ONE bge cross-encoder pass
(`BAAI/bge-reranker-v2-m3`, reused from `src/retrieval/reranker.py`,
process-cached) so the two modalities are scored against each other
rather than concatenated by retriever order. A chunk surfacing from both
modalities is deduped (first wins) by the pure `prepare_rerank_pool`
helper (`activities/rerank.py`) before reranking, so the model load is
unit-testable without the ~1 GB checkpoint. `top_n` from
`TEMPORAL_RERANK_TOP_N` (default 5). Runs on the small search queue.

- **Fail-open**: any rerank error → fall back to the merged pool **capped**
  to `rerank_top_n` (pure `cap_synth_sources`). The cap matters: an
  uncapped fallback could blow past `synthesize_answer`'s start_to_close
  timeout. The displayed `SearchOutcome.sources` stays the FULL merged
  pool (citations unchanged); only the synthesis *context* is trimmed.

### 5. Synthesis — large tier

`synthesize_answer` is pinned to `TEMPORAL_LARGE_TASK_QUEUE`
(`kb-search-large`) with `use_synthesis_llm=True` (the large
`build_synthesis_llm`). A separate low-concurrency `Worker` pool
(`TEMPORAL_LARGE_ACTIVITY_CONCURRENCY`, default 2) polls that queue so the
heavyweight synthesis model isn't dogpiled by parallel sessions. The call
spec (queue + tier) is built by the pure `build_synthesize_call` helper,
unit-testable outside Temporal.

The `SearchOutcome` returns the answer, the full merged source pool,
distinct `doc_ids` (`distinct_doc_ids`, skipping community partials),
per-step stats, citations, uncertainties, and latency.

### The deterministic retrieve pipeline

`retrieve_subquestion` (`activities/retrieve.py`) runs a FIXED tool
sequence for one sub-question by reusing `atomic_tools.dispatch` (the same
code path as the MCP server). A failure in one tool is logged and does
**not** sink the activity — whatever the other tools found is still
returned.

`_PIPELINE = (vector_search, graph_search, find_entity_by_name)`, then a
seeded `graph_walk`:

| Tool | Backend | Returns | Notes |
| --- | --- | --- | --- |
| `vector_search` | Milvus (HNSW) | top-k chunks by embedding similarity | the dense baseline; always available |
| `graph_search` | Neo4j native vector kNN over entity embeddings + `LLMSynonymRetriever` | matched entities + neighbours (`depth` triplet-hops) + related chunks | entity matching is an **indexed** native vector kNN (scales to a large graph) plus one small-LLM synonym call; entities are ER-canonicalised at ingest |
| `find_entity_by_name` | Neo4j fulltext index on `__Entity__.name` | entities by (partial) name | catches typos / partial names `graph_search` may rank out |
| `graph_walk` | Neo4j var-length `(e)-[*1..hops]-` | bounded neighbourhood (≤50 nodes / ≤100 edges) | explicit N-hop traversal; **dual-seeded** (below) |

**`graph_search` neighbour depth.** `graph_search` passes
`depth = settings.agent.graph_search_path_depth` (`AGENT_GRAPH_SEARCH_PATH_DEPTH`,
default 1, clamp 1–3) through to the retriever's `path_depth`. Depth 1 =
matched nodes + immediate relations; raise to widen graph context without
a code change. (Historically this was hard-pinned to 1; it is now an
operator knob, and also a per-call MCP override.) Candidate breadth is
`graph_similarity_top_k` (`AGENT_GRAPH_SIMILARITY_TOP_K`, default 20) so a
named entity isn't ranked out on a large graph.

**Dual walk-seed.** After `graph_search` (and `find_entity_by_name`) run,
the activity deterministically seeds the bounded `graph_walk` — no LLM
tool-pick. The pure `top_entity_name` helper picks the top entity from a
`graph_search` observation (entities arrive in similarity-rank order), and
`_walk_seeds` decides the seeds:

- **single-seed** (`graph_walk_dual_seed=False`): graph_search's top
  entity, else the fulltext top entity.
- **dual-seed** (default, `AGENT_GRAPH_WALK_DUAL_SEED=True`): the union of
  *both* (deduped, graph_search first), so a fulltext-matched entity
  (partial name / typo) still contributes its neighbourhood even when
  `graph_search` already returned something.

Each seed dispatches `graph_walk(start_entity, hops=graph_walk_hops)`;
walk chunks merge into the accumulated sources (same `seen` chunk_id set).
Gated by `AGENT_GRAPH_WALK_ENABLED` (default on). **Fail-open per seed**:
parse failure, a missing seed, or a store error skips that walk and
returns the rest unchanged — the activity never raises on the walk.
Because this parsing is non-deterministic it lives in the ACTIVITY, never
in a `@workflow.run` body.

`graph_walk` is **hard-bounded** (never unbounded), enforced both in the
Cypher and in the row-mapping: `hops ≤ GRAPH_WALK_MAX_HOPS` (3),
`≤ GRAPH_WALK_MAX_NODES` (50), `≤ GRAPH_WALK_MAX_EDGES` (100). One bounded
Cypher (`MATCH (e {name:$name})-[r*1..hops]-(m)` with a server-side
`LIMIT`, APOC-free fallback) keeps multi-hop from blowing up the synthesis
context. It is dispatchable but NOT in the fixed `_PIPELINE` (it needs an
explicit `start_entity`); the seeding above is what activates it.

---

## Global — GraphRAG map-reduce (`GlobalSearchWorkflow`)

`src/workflow/search/global_wf.py`. Answers corpus-level / thematic
questions by MAP-REDUCE over the offline community **reports**, without
retrieving individual chunks.

```
question (+ optional history)
  │
  ▼  0. contextualize_query        (if history + enabled; skipped under drift)
  ▼  1. map_communities — SELECT which communities to map over
  │       strategy: lexical | semantic | descent  (capped at max_communities)
  ├─ community 1 ─▶ map_community_partial (small tier) ─┐
  ├─ community 2 ─▶ map_community_partial (small tier) ─┤  gather, sem=map_parallelism
  └─ …                                                 ─┘
        off-topic communities self-drop ('НЕТ' → score 0)
                                                         │
        surviving partials → SerializedNode             │
        (chunk_id = "community:<id>")                    │
                                                         ▼
  2. documents_for_communities (doc_ids behind surviving communities)
  3. REDUCE: synthesize_answer ONCE  (large tier, kb-search-large)
                                                         ▼  SearchOutcome (mode="global")
```

- **MAP** (`map_community_partial`, `activities/global_search.py`) runs on
  `kb-search-small` (small `retrieve`-tier model), bounded by
  `map_parallelism` (`AGENT_GLOBAL_MAP_PARALLELISM`, default 4). Each
  off-topic community self-reports the literal `НЕТ` → score 0 and is
  dropped (`is_relevant_partial`).
- **REDUCE** is the existing `synthesize_answer`, pinned to the large
  queue exactly like the local flow (`build_reduce_call`).
- Map-spec / reduce-context / reduce-call assembly are pure helpers
  (`build_map_specs`, `partials_to_sources`, `build_reduce_call`) —
  unit-tested without a live Temporal env.
- `documents_for_communities` resolves the `doc_ids` behind the surviving
  communities for the response's document links.
- **Fail-safe**: store / LLM errors yield empty results (a no-evidence
  answer) rather than raising; `_coerce_global_params` defends against a
  data-converter handing back a plain dict.

### Community selection strategies

`map_communities` switches on `params.community_selection`
(`AGENT_COMMUNITY_DYNAMIC_SELECTION`, default `lexical`):

- **`lexical`** (`_map_communities_lexical` + pure `rank_summaries`) —
  read all `:Community.summary` for the level, rank by query word-overlap,
  cap at `limit`. Deterministic, LLM-free, no vector dependency. Ties fall
  back to the Cypher order (largest community first).
- **`semantic`** (`select_communities_semantic`) — embed the query, then
  kNN over the `community_report_vec` index
  (`db.index.vector.queryNodes`), nearest-first, capped at `limit`.
- **`descent`** (`select_communities_descent`) — GraphRAG dynamic
  selection: embed the query, start at the **coarsest** level (0), rank
  the frontier by cosine of query vs `report_vec`, descend `PARENT_OF`
  into the relevant children, and collect the **finest** relevant
  communities up to `budget`. Cycle-guarded; if nothing reaches a leaf
  (e.g. only level 0 exists) it falls back to the top-budget roots by
  cosine.

Both vector strategies **fail open to lexical** on an empty result or any
error, so flipping the selection knob can never harden into a failure.

### Communities are built offline

Communities + reports are produced by `CommunityBuildWorkflow`
(below) on `kb-graph-build`, fully decoupled from the query hot path.

---

## Drift — local then global (`DriftSearchWorkflow`)

`src/workflow/search/router_wf.py`. A **bounded** local-then-global
mechanism — exactly one local pass + one global pass, no open-ended loop.

```
question (+ optional history)
  │
  ▼  0. contextualize_query ONCE  (then children get history CLEARED)
  ▼  1. SearchOrchestratorWorkflow (child, id …-local)   → concrete chunk evidence
  ▼  2. GlobalSearchWorkflow (child, id …-global, drift_mode=True)
  │       seeded with the local sources
  │            └─ REDUCE merges local sources AHEAD of community partials
  │               (dedup by chunk_id) so local evidence leads
  │
  ├─ global fails / times out ─▶ _drift_local_fallback(local)
  │                               degrade to the local answer
  ▼  merge local + global doc_ids
     SearchOutcome (mode="drift" either way)
```

- **Contextualise once**: drift rewrites the follow-up itself, then passes
  the standalone query to BOTH children with `history=[]` so neither child
  re-runs contextualisation. Empty history ⇒ skipped (children behave as
  before).
- **Drift seed**: the global child is dispatched with `drift_mode=True`
  and the local `outcome.sources` as `drift_seed`. In drift mode the
  REDUCE context is `merge_subquery_sources([drift_seed, partials])` —
  local sources lead, community partials broaden, deduped by chunk_id. The
  outcome is labelled `"drift"`.
- **Graceful fallback**: if the global child raises (ChildWorkflowError /
  timeout / activity failure), `_drift_local_fallback` returns the local
  outcome with `mode` kept as `"drift"` — the request degrades to the
  local answer instead of failing. This is the drift safety net.
- On success, the final outcome's `documents` are the order-preserving
  union of local + global doc_ids (`merge_doc_ids`).

---

## Auto — routed dispatch (`AutoSearchWorkflow`)

`src/workflow/search/router_wf.py`. Picks the mode, then dispatches the
matching workflow as a child.

1. `route_query` (`activities/route.py`, small `route`-tier model)
   classifies the question into `local` / `global` / `drift`. The prompt
   asks for a single word; the pure `classify_route` helper recognises the
   FIRST known label in the (possibly wrapped) reply.
2. `dispatch_for_route` (pure) maps the label → workflow handle
   (`local` → orchestrator, `global` → global, `drift` → drift), defaulting
   to **local** for any unknown label.
3. Dispatch as a child workflow (`…-local` / `…-global` / `…-drift`).
   `get_state` exposes the chosen route for observability.

**Fail-safe routing**: `route_query` returns `route="local"` on ANY error
or unparseable reply, and `dispatch_for_route` also defaults to local — so
a flaky router degrades to the safe, cheapest, always-chunk-grounded flow.

---

## Offline community build (`CommunityBuildWorkflow`)

`src/workflow/search/community_wf.py` +
`src/workflow/search/activities/community.py`. **Fully decoupled /
offline** — runs on its own `kb-graph-build` queue, NEVER on the query hot
path. Triggered by `POST /api/v1/admin/communities/rebuild` (fire-and-
forget, returns the workflow id); a Temporal Schedule could call the same
workflow (none wired in-repo yet).

```
1. detect_communities_activity   (GDS Leiden over __Entity__)
     max_levels == 1 → single-level (detect_communities)
     max_levels  > 1 → full dendrogram hierarchy (detect_hierarchy)
   → MERGE :Community {id, level, member_count} + (:__Entity__)-[:IN_COMMUNITY]->
   → coarse→finer (:Community)-[:PARENT_OF]->(:Community) for hierarchy
   → ensure the community_report_vec index ONCE (fail-open → degrades to lexical)
2. summarize fan-out, FINEST-level-first (bounded by community_summary_parallelism)
   per community: structured report {title, summary, findings:[{statement, importance}]}
     level 0  → from member entities/relations
     level>0  → from CHILD reports (composed bottom-up; falls back to members)
   → embed (title + summary) → MERGE report / title / summary / report_vec on :Community
```

- **Detection** projects the `__Entity__` sub-graph into an in-memory GDS
  graph and runs **GDS Leiden** (`gds.leiden.stream`). Communities below
  `community_min_size` are dropped. **Fail-safe**: any GDS/Cypher error
  (or no store) → `[]`, never raised through the activity.
- **Summaries** are produced by the **small-tier** LLM
  (`build_llm("retrieve")` — never the large synthesis model), tolerant
  JSON-parsed (`_parse_report` always yields *something* to persist).
- **Level ordering**: the workflow groups specs by level and processes the
  finest first (`group_specs_by_level`), so a coarse parent's child
  reports already exist when it composes its own
  (`_CHILD_REPORTS_CYPHER`). A level barrier means "this level finished",
  not "all children present"; partial failures degrade and idempotent
  re-runs heal.
- **Incremental**: a community carried over unchanged from a prior build
  (`needs_report=False`) is skipped — its report is already persisted.
  Re-running is idempotent (MERGE keys on `(id, level)`), refreshing
  summaries / membership without duplicating nodes.

**`:Community` schema** (additive — no existing label/property touched):
- Label `:Community`; props `id` (Leiden communityId), `level` (0 =
  coarsest), `member_count`, `title`, `summary`, `report` (JSON structured
  report), `report_vec` (native embedding, may be unset on embed failure),
  `summarized_at`.
- Relationships `(:__Entity__)-[:IN_COMMUNITY]->(:Community)` and the
  coarse→finer `(:Community)-[:PARENT_OF]->(:Community)` dendrogram.
- Unique constraint `community_key` on `(id, level)` backs the MERGE;
  range indexes on `Community.level` and `Chunk.doc_id` back the global
  read / community→document traversal (`ensure_community_indexes`).

> **GDS note**: the `gds.graph.project` / `gds.leiden.stream` /
> `gds.graph.drop` Cypher targets the Neo4j GDS 2.x API but is **unverified
> against a live GDS install** (no Neo4j/GDS in the dev sandbox — tests
> mock the store + GDS rows). All GDS/Cypher strings are isolated as
> constants at the top of `src/graph/communities.py` to make a version fix
> a one-file change. Validate before relying on it in production.

---

## Per-call depth/hops (MCP layer)

The HTTP `/api/v1/search/*` endpoints have NO per-request depth/hops field
— `SearchRequest` exposes only `query`, `top_k`, `history`, and the local
pipeline reads depth/hops from `AgentSettings`.

The **per-call** overrides live on the MCP-2 atomic tools
(`src/mcp/tools_server.py`), where a caller drives the retrievers
directly:

- `graph_search(query, depth=1)` — `depth` (1–3, clamped) sets neighbour
  expansion per call (maps to the retriever's `path_depth`).
- `find_neighbours(entity_name, hops=1)` — `hops` (1–3, clamped) sets
  neighbour depth per call.
- `graph_walk(start_entity, hops=2, rel_filter=None)` — `hops` per call,
  hard-capped at `GRAPH_WALK_MAX_HOPS`.

These wrap the same `atomic_tools` the deterministic pipeline uses, so the
config defaults (`AGENT_GRAPH_SEARCH_PATH_DEPTH`, `AGENT_GRAPH_WALK_HOPS`)
and the per-call MCP args share one backend.

---

## What degrades vs hard-fails

The live path is heavily fail-open so a flaky dependency never blocks an
answer; the few hard-fail points are the route handlers themselves.

| Component | On failure |
| --- | --- |
| `contextualize_query` | use the raw query (degrade) |
| `plan_subquestions` | `[query]` — single sub-question (degrade) |
| one retrieve tool | logged, other tools' results still returned (degrade) |
| `graph_walk` seed | skip that walk, keep the rest (degrade per seed) |
| `coverage_check` / extra round | break straight to synthesis (degrade) |
| `rerank_sources` | merged pool capped to `rerank_top_n` (degrade) |
| `route_query` | default `local` (degrade) |
| drift global pass | `_drift_local_fallback` → local answer, mode stays `drift` (degrade) |
| `map_communities` semantic/descent | fall back to lexical (degrade) |
| global MAP / community store | empty → no-evidence answer (degrade) |
| community detect / summarize | `[]` / non-persisted, next build reconciles (degrade) |
| `synthesize_answer` | propagates → route returns HTTP 500 (**hard-fail**) |
| Temporal start/await in the route | route returns HTTP 500 (**hard-fail**) |

---

## Config knobs

Search knobs live on `AgentSettings` (`AGENT_` prefix, `src/config.py`)
and `TemporalSettings` (`TEMPORAL_` prefix). See
[`FEATURES.md`](FEATURES.md#config-quick-reference-new-feature-env-vars)
for the new-feature subset.

| Env var | Setting | Default | Effect |
| --- | --- | --- | --- |
| `AGENT_TOP_K` | `top_k` | 10 | default chunk top-k (request `top_k` overrides) |
| `AGENT_MAX_SUBQUERIES` | `max_subqueries` | 5 | caps the parallel sub-query fan-out (and planner cost) |
| `AGENT_COVERAGE_CHECK_ENABLED` | `coverage_check_enabled` | true | gates the orchestrator coverage gate |
| `AGENT_MAX_COVERAGE_ROUNDS` | `max_coverage_rounds` | 1 | caps extra coverage sub-question rounds (0–3) |
| `AGENT_CONVERSATION_HISTORY_ENABLED` | `conversation_history_enabled` | true | contextualise follow-ups (inert without history) |
| `AGENT_HISTORY_MAX_TURNS` | `history_max_turns` | 6 | recent turns fed to contextualisation |
| `AGENT_HISTORY_MAX_CHARS` | `history_max_chars` | 4000 | char budget for the history window |
| `AGENT_GRAPH_WALK_ENABLED` | `graph_walk_enabled` | true | enable the deterministic `graph_walk` seeding |
| `AGENT_GRAPH_WALK_HOPS` | `graph_walk_hops` | 2 | requested walk hops (clamped to `GRAPH_WALK_MAX_HOPS`=3) |
| `AGENT_GRAPH_WALK_DUAL_SEED` | `graph_walk_dual_seed` | true | seed walk from graph_search + fulltext entity |
| `AGENT_GRAPH_SEARCH_PATH_DEPTH` | `graph_search_path_depth` | 1 | `graph_search` neighbour depth (1–3) |
| `AGENT_GRAPH_SIMILARITY_TOP_K` | `graph_similarity_top_k` | 20 | graph retriever candidate count |
| `AGENT_GLOBAL_MAX_COMMUNITIES` | `global_max_communities` | 20 | caps communities entering the global MAP |
| `AGENT_GLOBAL_MAP_PARALLELISM` | `global_map_parallelism` | 4 | per-community MAP concurrency |
| `AGENT_COMMUNITY_MAX_LEVELS` | `community_max_levels` | 1 | Leiden hierarchy depth to materialise (1 = single-level) |
| `AGENT_COMMUNITY_DYNAMIC_SELECTION` | `community_dynamic_selection` | lexical | global/drift selection: `lexical`\|`semantic`\|`descent` |
| `TEMPORAL_SEARCH_TASK_QUEUE` | `search_task_queue` | `kb-search-small` | queue hosting the orchestrator + sub-query children |
| `TEMPORAL_LARGE_TASK_QUEUE` | `large_task_queue` | `kb-search-large` | dedicated queue for large-tier `synthesize_answer` |
| `TEMPORAL_LARGE_ACTIVITY_CONCURRENCY` | `large_activity_concurrency` | 2 | low cap so synthesis isn't dogpiled |
| `TEMPORAL_RERANK_TOP_N` | `rerank_top_n` | 5 | bge cross-encoder top-N into synthesis |
| `TEMPORAL_GRAPH_BUILD_TASK_QUEUE` | `graph_build_task_queue` | `kb-graph-build` | offline community-build queue |
| `TEMPORAL_GRAPH_BUILD_ACTIVITY_CONCURRENCY` | `graph_build_activity_concurrency` | 2 | low cap so a rebuild's summary burst doesn't flood the proxy |
| `TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM` | `community_summary_parallelism` | 4 | per-community summarize fan-out |
| `TEMPORAL_COMMUNITY_MIN_SIZE` | `community_min_size` | 3 | communities smaller than this are ignored |

The `route` / `plan` / `retrieve` / synthesis model tiers are configurable
via `LITELLM_ROLE_TIERS` (defaults from `_DEFAULT_ROLE_TIERS`); `route`,
`plan`, `retrieve` and the community/contextualise roles map to the small
tier, synthesis to the large tier.

---

## Module status

- **Active**: `src/retrieval/reranker.py` (`BAAI/bge-reranker-v2-m3`,
  unified rerank), `src/retrieval/atomic_tools.py` (the tool backend),
  `src/retrieval/llm.py` (`build_llm` role→tier).
- **Not wired**: `src/retrieval/hybrid.py` — a BM25+dense experiment
  candidate; benchmark via `tests/eval/` before integrating.

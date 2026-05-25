# Changelog

All notable changes to **kb-llamaindex** are recorded here. The project
is built incrementally per the plan in
`~/.claude/plans/hashed-rolling-llama.md` — one commit per stage, each
with a section in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
loosely (Added / Changed / Fixed / Notes per stage).

## [Search R7a] — 2026-05-26 — Query routing + GraphRAG global search (additive)

### Added
- `src/workflow/search/activities/route.py` — `route_query` activity
  (small `route` model) classifies a question into `local` (specific /
  factual), `global` (corpus-level / thematic / aggregate) or `drift`
  (complex / mixed). Fail-safe → `local` on any LLM/parse error. Pure
  `classify_route` helper for Temporal-free unit testing.
- `src/workflow/search/global_wf.py` — `GlobalSearchWorkflow`: GraphRAG
  **map-reduce** over the R6 `:Community.summary` texts. `map_communities`
  reads + ranks summaries (capped by `global_max_communities`); MAP fans
  out one per-community partial (small tier, bounded by
  `global_map_parallelism`, off-topic communities self-drop); REDUCE reuses
  `synthesize_answer` pinned to `large_task_queue` with
  `use_synthesis_llm=True` (the R5 large-tier pattern). Pure helpers
  `build_map_specs` / `partials_to_sources` / `build_reduce_call`.
- `src/workflow/search/activities/global_search.py` — `map_communities`
  + `map_community_partial` activities (fail-safe; pure `rank_summaries` /
  `is_relevant_partial` helpers).
- `src/workflow/search/router_wf.py` — `DriftSearchWorkflow` (local pass
  → global community expansion seeded with the local sources, `drift_mode`)
  and `AutoSearchWorkflow` (`route_query` → dispatch to local/global/drift
  as a child workflow). Pure `dispatch_for_route` helper.
- Endpoints in `src/api/routes/search_v2.py`: `POST /api/v1/search/global`
  (→ `GlobalSearchWorkflow`), `/search/drift` (→ `DriftSearchWorkflow`),
  `/search/auto` (→ `AutoSearchWorkflow`). All on `kb-search-small`
  orchestration; synthesis pinned large in the chosen flow. Reuse the
  legacy `SearchRequest`/`SearchResponse` shapes.
- `AgentSettings`: `global_max_communities` (20), `global_map_parallelism`
  (4). New contracts: `RouteParams/Result`, `CommunitySummaryRef`,
  `MapCommunitiesParams/Result`, `MapPartialParams/Result`,
  `GlobalSearchParams`. `SearchMode` extended with `global`/`drift`.
- Worker (`src/workflow/worker.py`): registers `GlobalSearchWorkflow`,
  `DriftSearchWorkflow`, `AutoSearchWorkflow` + `route_query` /
  `map_communities` / `map_community_partial` on the `kb-search-small`
  queue (REDUCE still pins synthesize to `kb-search-large`).

### Notes
- **Additive only — legacy intentionally RETAINED.** The legacy
  `SearchWorkflow` (`src/workflow/search_workflow.py`) and the
  `/search`,`/agent`,`/selfrag` routes are NOT modified; the local R2–R5
  flow is unchanged. Defaults are unchanged. The legacy cutover/deletion is
  a SEPARATE phase pending live-environment parity verification.
- GraphRAG global search reads the `:Community.summary` nodes built offline
  in R6; run `POST /api/v1/admin/communities/rebuild` first or global/drift
  answers have no community evidence to map over.

## [Search R6] — 2026-05-26 — Offline community build (GDS Leiden + summaries)

### Added
- `src/graph/communities.py` — `detect_communities(store, *, min_size, level)`
  runs Neo4j **GDS Leiden** over the `__Entity__` sub-graph (Cypher
  projection → `gds.leiden.stream` → group by `communityId`), drops
  communities below `min_size`, and idempotently MERGEs
  `:Community {id, level, member_count}` nodes linked to members via
  `(:__Entity__)-[:IN_COMMUNITY]->(:Community)`. All GDS/Cypher isolated as
  module constants for easy fix-against-live. Fail-safe: any GDS/store
  error → `[]` (logged, never raised).
- `src/workflow/search/activities/community.py` —
  `detect_communities_activity` (wraps detection) and
  `summarize_community_activity` (small-tier `build_llm("retrieve")`
  summary per community, persisted on `:Community.summary` via idempotent
  MERGE; fail-safe per community).
- `src/workflow/search/community_wf.py` — `CommunityBuildWorkflow`:
  detect → bounded-parallel summarize fan-out → done. Pure
  `build_summarize_specs` helper for Temporal-free unit testing.
- Dedicated **`kb-graph-build`** task queue + a separate `Worker` pool in
  `src/workflow/worker.py` hosting the workflow + its activities — fully
  DECOUPLED from the query hot path.
- Admin endpoint `POST /api/v1/admin/communities/rebuild`
  (`src/api/routes/search_v2.py`) — starts `CommunityBuildWorkflow` on
  `kb-graph-build`, returns the workflow id. Optional Temporal
  Schedule/cron documented in `docs/QUEUES.md` (none wired yet).
- `TemporalSettings`: `graph_build_task_queue` (default `kb-graph-build`),
  `graph_build_activity_concurrency` (2), `community_summary_parallelism`
  (4), `community_min_size` (3). New contracts: `CommunityRef`,
  `DetectCommunitiesParams/Result`, `SummarizeCommunityParams/Result`,
  `CommunityBuildResult`.

### Notes
- Additive / offline only — the query path (orchestrator + local search)
  is UNCHANGED; nothing on the query path reads `:Community` yet. Summaries
  are written for a future global-search phase.
- The GDS Cypher (`gds.graph.project` / `gds.leiden.stream` /
  `gds.graph.drop`) is written per the Neo4j GDS 2.x API but is
  **UNVERIFIED against a live GDS install** — no Neo4j/GDS in the dev
  sandbox, so all tests mock the store + GDS rows. Validate against the
  live GDS version before production use.

## [Search R5] — 2026-05-26 — Large-tier synthesis queue + unified rerank

### Added
- Dedicated `kb-search-large` task queue
  (`TemporalSettings.large_task_queue`, `large_activity_concurrency`
  default 2) + a separate low-concurrency `Worker` pool in the same
  worker process (`src/workflow/worker.py`) that polls it, hosting ONLY
  the `synthesize_answer` activity so the heavyweight synthesis model is
  never dogpiled by parallel search sessions.
- `rerank_sources` activity
  (`src/workflow/search/activities/rerank.py`,
  `RerankParams`/`RerankResult` in `contracts.py`): co-ranks the merged
  graph-derived + vector pool in ONE bge cross-encoder pass before
  synthesis. REUSES `src/retrieval/reranker.py`
  (`BAAI/bge-reranker-v2-m3`), process-cached via
  `_search_deps.get_reranker`. Pure `prepare_rerank_pool`
  (dedup-before-rerank) + `build_synthesize_call` (large-queue call
  spec) helpers — unit-tested without a live Temporal env or the bge
  model.
- `TEMPORAL_RERANK_TOP_N` (`TemporalSettings.rerank_top_n`, default 5)
  knob for the unified rerank top-N.
- Queue/tier docs: `kb-search-large` row + small/large tier↔queue
  mapping table in `docs/QUEUES.md`; rerank-before-synthesis step in
  `docs/SEARCH.md`.

### Changed
- `SearchOrchestratorWorkflow` (`src/workflow/search/orchestrator.py`):
  after the coverage gate / final merge it runs `rerank_sources` (on the
  small queue), THEN pins `synthesize_answer` to `kb-search-large` via
  `execute_activity(task_queue=large_task_queue)` with
  `use_synthesis_llm=True` (large `build_synthesis_llm`). The displayed
  `SearchOutcome.sources` stays the FULL merged pool (citations
  unchanged); only the synthesis context is trimmed to the reranked
  top-N.

### Notes
- FAIL-OPEN: any rerank error → fall back to the unranked merged pool
  (never blocks the answer). Empty pool → reranker model is never loaded.
- The orchestrator workflow itself still lives on `kb-search-small`; only
  the final synthesis activity runs on `kb-search-large`. plan / retrieve
  / coverage_check / rerank all stay on the small queue.
- Legacy ReAct `SearchWorkflow` synthesis path UNCHANGED — still small
  tier (`use_synthesis_llm=False`), default `kb-search-small` queue, no
  rerank step.

## [Search R4] — 2026-05-26 — Coverage gate on orchestrator

### Added
- Bounded coverage round on `SearchOrchestratorWorkflow`
  (`src/workflow/search/orchestrator.py`): after merging all
  sub-question sources (and before the single `synthesize_answer`), the
  orchestrator runs ONE `coverage_check` — REUSING the existing
  small-tier activity (`src/workflow/activities/coverage_check.py`,
  already registered via `SEARCH_ACTIVITIES` on the search worker), not
  a re-implementation. On `complete=False` with a named `missing` gap it
  issues that gap as ONE extra `SubQueryRetrievalWorkflow` (child id
  `…-cov-N`), re-merges its sources (dedup by chunk_id), records an
  extra step-stat, then synthesizes.
- Pure, Temporal-free gate helpers (`src/workflow/search/_coverage.py`):
  `should_run_coverage_round(result, rounds_left) -> str | None` and
  `build_evidence(sources, max_chars)` — unit-tested for the gap /
  complete / empty-gap / bound branches without a live Temporal env.
- `AgentSettings.max_coverage_rounds` (`AGENT_MAX_COVERAGE_ROUNDS`,
  default 1) capping the orchestrator's extra rounds; the existing
  `coverage_check_enabled` knob is REUSED to gate the check.
  `OrchestratorParams` carries both (resolved at submit time in
  `search_v2.py` → replay-safe).
- Coverage-gate section in `docs/SEARCH.md`.

### Notes
- FAIL-OPEN: any error in the coverage check OR the extra retrieval
  round → proceed straight to synthesis (never blocks the answer).
- Bounded by `max_coverage_rounds` (default 1) — at most one extra
  sub-question even if a gap persists, so the loop always terminates.
- The legacy ReAct `SearchWorkflow` coverage path (gap fed back into the
  reasoning history, bounded by `max_coverage_checks`) is UNCHANGED — a
  separate mechanism from the orchestrator's "gap → extra sub-question".

## [Search R3] — 2026-05-26 — Multi-hop graph_walk tool

### Added
- `graph_walk(start_entity, hops=2, rel_filter=None)` atomic tool
  (`src/retrieval/atomic_tools.py`) — EXPLICIT, BOUNDED multi-hop graph
  traversal. Registered in `TOOL_FUNCTIONS`, `TOOL_DESCRIPTIONS`, and
  `dispatch()` exactly like the sibling graph tools; returns the same
  serialized `{"entities", "relations"}` observation + chunk `sources`.
- `GraphRetriever.awalk()` (`src/graph/retriever.py`) — N-hop backend:
  one bounded Cypher query (`MATCH (e {name:$name})-[r*1..hops]-(m)` with
  a `rel_filter` `WHERE` clause + `LIMIT $node_cap`) via the store's
  `structured_query`, with an APOC-free fallback. `hops` clamped and
  interpolated as a vetted int; row mapping re-applies the caps.
- Hard caps `GRAPH_WALK_MAX_HOPS=3`, `GRAPH_WALK_MAX_NODES=50`,
  `GRAPH_WALK_MAX_EDGES=100` (mirrored tool-side + retriever-side) so a
  multi-hop walk can never blow up the agent's context window.
- `ALLOWED_TOOLS` on the R2 retrieve path
  (`src/workflow/search/activities/retrieve.py`) now lists `graph_walk`
  as dispatchable via the same `graph_retriever` DI.
- `graph_walk` section in `docs/SEARCH.md` (purpose, caps, when used).

### Notes
- Default `graph_search` (similarity, `path_depth=1`) behaviour and tests
  are UNCHANGED; `awalk` is a separate method, not a change to
  `aretrieve`.
- `graph_walk` is NOT in the default deterministic `_PIPELINE` — it needs
  an explicit `start_entity` (a real entity name), which belongs to a
  future connection-aware planner / LLM tool-pick step. Dispatch wiring +
  caps are in place; the tool description documents WHEN to pick it.

## [Search R2] — 2026-05-26 — Plan-execute orchestrator + /search/local

### Added
- `src/retrieval/query_planner.py:decompose()` — splits a compound
  question into atomic sub-questions via the small `plan` model;
  returns `[question]` for atomic questions and on any planner failure
  (fail-safe). Robust parsing (numbered / bulleted / JSON-array).
- `plan_subquestions` activity (`src/workflow/search/activities/plan.py`)
  wrapping `decompose` (`build_llm("plan")`).
- `retrieve_subquestion` activity
  (`src/workflow/search/activities/retrieve.py`) — deterministic hybrid
  `vector_search` + `graph_search` for one sub-question via
  `atomic_tools.dispatch`, sources deduped by chunk_id.
- `SubQueryRetrievalWorkflow` (`src/workflow/search/subquery_wf.py`) —
  deterministic per-sub-question retrieval; NO `agent_reasoning_step`.
- `SearchOrchestratorWorkflow` (`src/workflow/search/orchestrator.py`) —
  plan → parallel `SubQueryRetrievalWorkflow` children (`asyncio.gather`
  over `execute_child_workflow`) → merge+dedup sources by chunk_id →
  single `synthesize_answer` on the large tier. Same `SearchOutcome`
  shape as legacy `SearchWorkflow`.
- `POST /api/v1/search/local` (`src/api/routes/search_v2.py`) starting
  `SearchOrchestratorWorkflow` on `kb-search-small`; reuses
  `SearchRequest` / `SearchResponse`.
- `AgentSettings.max_subqueries` (env `AGENT_MAX_SUBQUERIES`, default 5)
  — caps the parallel sub-query fan-out.
- `SynthesizeParams.use_synthesis_llm` flag + `get_synthesis_llm()` /
  `get_synthesis_synthesizer()` in `_search_deps` — large-tier final
  synthesis for the plan-execute flow.
- `docs/QUEUES.md` (new) + plan-execute section in `docs/SEARCH.md`.

### Changed
- Search task queue renamed `kb-search-llm` → `kb-search-small`
  (`TemporalSettings.search_task_queue` default, `.env.example`). The
  queue now also hosts the small-tier plan-execute flow.
- Worker registers `SearchOrchestratorWorkflow` +
  `SubQueryRetrievalWorkflow` and `SEARCH_V2_ACTIVITIES` on the search
  queue alongside the legacy `SearchWorkflow`.

### Notes
- Legacy ReAct `SearchWorkflow` + `/api/v1/search` (and `/agent`,
  `/selfrag`) are UNCHANGED and remain the default behind the parity
  window until cutover.
- Core merge/dedup is extracted into pure helpers
  (`src/workflow/search/_merge.py`) so it is unit-testable without a
  live Temporal env; full workflow tests follow the repo's
  skip-on-no-Temporal pattern.

## [Search R1] — 2026-05-25 — Two-tier model architecture

### Added
- `LiteLLMSettings.model_small` (default `gemma4:e4b`) and
  `model_large` (default `gpt-4o-mini`) — the two physical model
  tiers operators manage.
- `LiteLLMSettings.role_tiers` (env `LITELLM_ROLE_TIERS`, JSON) —
  declarative role→tier map, merged onto `_DEFAULT_ROLE_TIERS` so a
  partial override (e.g. `{"plan":"large"}`) escalates one role
  without re-declaring the rest.
- `LiteLLMSettings.tier_for(role)` and `effective_base` property.
- `LLMTier = Literal["small","large"]`; `LLMRole` extended with
  `route`, `plan`, `retrieve`, `distill`, `coverage`, `synthesis`.
- `src/retrieval/llm.py:build_synthesis_llm()` — final synthesis on
  the large tier.

### Changed
- `LiteLLMSettings.model_for` now resolves `role → tier → one of the
  two physical models` instead of reading per-role model fields.
- **Behavior change**: default extraction/judge/search model is now
  `gemma4:e4b` (small tier) instead of `qwen3:8b`.  Every role maps
  to the small tier except `synthesis` → large (`gpt-4o-mini`).
- `build_llm()` no-role path uses `effective_base` (small tier, or the
  deprecated `llm_model` alias when explicitly set).
- `src/observability/litellm_models.py` validates the two physical
  models (small/large) rather than per-role names.
- `src/api/routes/ingest.py` analytics `Model` snapshot now uses
  `effective_base`.
- `.env.example`, `docker/litellm_config.yaml`, `docs/MODELS.md`
  rewritten for the two-tier model.

### Removed
- Per-role `extraction_model` / `judge_model` / `search_model` fields
  on `LiteLLMSettings` and their `LITELLM_*_MODEL` env vars.

### Notes
- `LITELLM_LLM_MODEL` / `LiteLLMSettings.llm_model` kept as a
  deprecated alias (defaults to `""`) so the legacy no-role
  `build_llm()` path keeps working.  Remove once all callers pass a
  role.

## [Search R0] — 2026-05-25 — Search package scaffold

### Added
- `src/workflow/search/` package (+ `activities/` subpackage) — skeleton
  for decomposing the monolithic `search_workflow.py` into an
  orchestrator + per-mode child workflows (Plan #2, agentic-search).
- `docs/SEARCH.md` — search subsystem overview + target workflows/endpoints.

### Notes
- No behavior change: the legacy `SearchWorkflow` and its endpoints are
  untouched. New workflows land behind flags/new endpoints in later phases.

## [R1] — 2026-05-11 — Model migration to qwen3:8b

### Changed
- Default LLM model in `.env`, `.env.example`,
  `src/config.py:LiteLLMSettings.llm_model`,
  `docker/litellm_config.yaml` → `qwen3:8b`.
  Rationale: qwen3:8b has reliable Hermes-style tool calling +
  structured output, required by R7 (ReAct agent) and R8
  (reflective synthesis).
- `LITELLM_TIMEOUT_S` raised 600 → 900 (qwen3:8b is slightly
  slower per token).
- `docker/litellm_config.yaml` — keeps `llama3.1:8b` as
  baseline (commented escalation targets `qwen3:14b`/`32b`
  for `docs/MODELS.md` from R6).
- `scripts/start.sh` — startup banner reminds the operator to
  pull `qwen3:8b` and `nomic-embed-text` into Ollama before
  running the stack.
- `README.md` — rewritten: recommended model + R1-R10 refactor
  status + 3-endpoint architecture preview + multi-domain
  context (reports / emails / transcripts).

### Added
- `AgentSettings.max_iterations` and `max_refinements` env
  knobs (`AGENT_MAX_ITERATIONS`, `AGENT_MAX_REFINEMENTS`).
  Reserved for R7 (ReAct iteration cap) and R8 (reflective
  redraft cap); not yet used in code.

### Cleanup (refactoring mandate)
- `.env.example` — removed stage-numbered comments
  ("used from Stage 8 onward" etc.); subsystem headers now
  describe purpose only.
- `src/config.py:AgentSettings` docstring no longer references
  Stage 4 by name.

### Verified
- `pytest -q` — 107/107 green.
- Live smoke against running compose:
  - `llm.acomplete("2+2?")` → "четыре" (qwen3:8b reachable via
    LiteLLM proxy).
  - `embed_model.aget_text_embedding(...)` → 768-dim vector.
- `scripts/diag_kg.py` on Russian contract excerpt with
  qwen3:8b → **16 entities + 8 relations** (target was ≥10
  entities + ≥5 relations).  llama3.1:8b previously produced
  14 + 7 on the same text — qwen3:8b is marginally better.

## [post-Stage-9 fixes] — 2026-05-11 — Live KG extraction wired

### Fixed
- **KG extraction never ran at runtime.** Worker's
  `process_document` did `inject_canonical_entities` but never
  called the `SchemaLLMPathExtractor`/`PropertyGraphIndex` → 0
  relations in Neo4j.  Now invokes
  `build_property_graph_index(nodes=...)` after canonical
  injection so LLM-extracted triples land alongside the
  deterministic canon-nodes.
- **`graph_retriever` provider returned None.** Wrapped Neo4j
  PropertyGraphIndex construction; falls back to None on
  Neo4j outage.  Agent search now sees real entities + relations
  per round when graph is online.
- **SchemaLLMPathExtractor incompatible with llama3.1:8b.** Its
  Pydantic validator catches only `KeyError/ValueError`; small
  models emit malformed triplet JSON that raises `TypeError`,
  killing the whole structured_predict.  Even when using
  function-calling mode (`is_function_calling_model=True`),
  llama3.1:8b often skips the tool call.
  → Default extractor switched to `SimpleLLMPathExtractor`
  (regex-based, tolerant).  Stricter `SchemaLLMPathExtractor` is
  still available via `build_kg_extractor(mode="schema",
  strict=True)` for GPT-4-class or 70B+ backends.
- **Russian-tuned extract prompt.** Stock LlamaIndex prompt
  contains English Alice/Bob/Philz examples that small models
  literally echo as "Subject/Predicate/Object" placeholders.
  Replaced with a Russian B2B example
  (`ООО Альфа → договор № 17-К → ИП Иванов`).  Empirical result
  on llama3.1:8b: 18 entities + 9 typed relations from a
  5-line contract excerpt (vs 0 with the stock prompt).
- **`build_kg_extractor` API extended** with `mode: ExtractorMode
  = "simple" | "schema"` for future swaps.

### Added
- `scripts/diag_kg.py` — diagnostic that runs the extractor on a
  hard-coded chunk and prints entities + relations.  Helpful when
  swapping LLMs to verify they still produce structured output.

### Notes
- `SimpleLLMPathExtractor` returns entities with generic label
  `entity` and relations with the LLM-emitted verb as label.  We
  rely on Stage 7's canonical-identifier injection (which
  upserts EntityNodes with **typed** labels via PropertyGraphStore
  directly) for phone/INN/OGRN/etc.  Free-form Person /
  Organization / Event nodes from the LLM remain untyped — this is
  the tradeoff vs strict SchemaLLM mode.
- Suite total: 107 tests green (unchanged).

## [Stage 9] — 2026-05-09 — Eval gate + ops scripts

### Added
- `tests/eval/identifier_recall.py` — **ported** from
  enterprise-kb.  CLI: `--strict`, `--json-out`, `--golden`.
  Same acceptance thresholds: phone/email/INN/OGRN/BIC/date
  ≥0.95 recall, contract/amount ≥0.85, address ≥0.75,
  precision ≥0.90.
- `tests/eval/test_identifier_recall_thresholds.py` — pytest
  gate over the eval (CI integration).
- `tests/eval/golden_identifiers/*.json` — 7 golden cases
  ported (contract supply, banking details, sole proprietor,
  dates / phones / amounts variety, negative case).
- `scripts/merge_identifier_duplicates.py` — adapted for
  LlamaIndex.  Pure helpers (`canonicalize_for_type`,
  `group_by_canonical`) port verbatim; the merge-write path
  uses Cypher (`structured_query`) directly because LlamaIndex's
  graph-store interface lacks a single-shot
  `amerge_entities` equivalent.  `--dry-run` default; per-group
  failures logged + counted, run continues.
- `tests/test_scripts/test_merge_identifier_duplicates.py` — 7
  tests covering canonicalisation, grouping, dry-run no-op,
  real-run cypher invocation, error tolerance.
- `scripts/check_ingestion.py` — diagnostic over Postgres
  (row counts by status), Milvus (collection stats), Neo4j
  (node + relationship counts).

### Notes
- Eval result on the ported golden set: every type 100%
  recall + 100% precision (matches enterprise-kb baseline).
- Merge cypher is intentionally cautious: MERGEs the target,
  redirects all in/out relations to `RELATED_TO` then
  DETACH-DELETEs sources. Future work can preserve original
  relation types when porting becomes critical.
- Suite total: 107 tests green.

## [Stage 8] — 2026-05-09 — FastAPI + Taskiq worker

### Added
- `docker-compose.yml` — RabbitMQ 3.13 added (5672/15672 +
  management UI, healthcheck via `rabbitmq-diagnostics ping`).
- `src/storage/postgres.py` — `AsyncPostgres` client with
  `insert_pending`, `update_status`, `get` ops over the
  `documents` table.
- `src/api/auth.py` — `require_api_key` dep returning 401 on
  missing header / 403 on invalid key.
- `src/api/routes/health.py` — `GET /health` (public).
- `src/api/routes/ingest.py` — `POST /api/v1/ingest`
  (multipart upload + 202 with `job_id`),
  `GET /api/v1/ingest/{job_id}` (PG-backed status poll).
- `src/api/routes/search.py` — `POST /api/v1/search`.  Dispatches
  to `agentic_search` when `agentic=true`, otherwise single-round
  retrieve + synthesize.  All collaborators come from dishka.
- `src/api/main.py` — FastAPI app, lifespan-managed dishka
  container, CORS middleware, route registration.
- `src/di/providers.py` — `CommonProvider` (PG, LLM, embeddings),
  `ApiProvider` (retriever, judge, synthesizer,
  graph_retriever=None).  `build_api_container()` and
  `build_worker_container()` factories.
- `src/ingestion/tasks.py` — Taskiq broker
  (``AioPikaBroker(settings.rabbitmq.url)``) and
  `process_document(doc_id, path)` task wiring the full
  ingestion chain (parse → chunk → canon transform → vector
  index → optional graph injection → PG status update).
- `scripts/smoke.sh` — health / search / errors scenarios via
  curl + jq.
- `tests/test_api/test_health.py` — `/health` smoke.
- `tests/test_api/test_auth.py` — auth dep matrix.

### Notes
- `graph_retriever` provider currently returns ``None`` — wiring
  the live PropertyGraphIndex retriever needs a populated Neo4j,
  which the eval gate (Stage 9) will exercise.  Search route
  handles `None` gracefully.
- Worker task ingestion path mirrors enterprise-kb's
  `AsyncDocumentWorker.process_document` flow but composed from
  LlamaIndex pieces.  No taskiq enqueue call from the API route
  yet — the prototype's worker can be invoked manually via
  `uv run taskiq worker src.ingestion.tasks:broker` after a
  prior CLI ingest, or extended in a follow-up.
- Suite total: 92 tests green.

## [Stage 7] — 2026-05-09 — Identifier canonicalization

### Added
- `src/ingestion/identifiers.py` — **ported verbatim** from
  `enterprise-kb/src/ingestion/identifiers.py`.  Same per-type
  detectors (PhoneNumber → E.164, Email lowercase, INN/OGRN with
  checksum, BIC, ContractNumber upper, DocumentDate ISO,
  Amount with `тыс/млн/млрд` multipliers, PostalAddress via
  libpostal-or-rules), same dataclass `NormalizedIdentifier`,
  same Stage-C helpers (`dedupe_by_canonical`,
  `build_custom_kg_payload`, `build_augment_block`).
- `src/ingestion/identifier_transform.py`:
  - `IdentifierCanonicalizationTransform` —
    `TransformComponent` for `IngestionPipeline`.  Stores
    `canonical_identifiers` in node.metadata; appends the
    "Канонические идентификаторы:" block to node.text.
  - `inject_canonical_entities(graph_store, nodes)` —
    dedup-merges by (entity_type, canonical) and upserts
    `EntityNode` objects into the property-graph store.
- `tests/test_ingestion/test_identifiers.py` — **ported** —
  39 tests on per-type detectors, edge cases, integration on a
  realistic Russian contract excerpt.
- `tests/test_ingestion/test_identifier_transform.py` — 5 tests:
  metadata + augment block, no-op on plain text, graph
  injection dedup, missing metadata is safe, pluggable into the
  Stage-2 pipeline factory.

### Notes
- The deterministic identifier layer is the project's main moat
  vs RAGFlow / vanilla LlamaIndex stacks.  Porting it 1:1 keeps
  parity with enterprise-kb so the Stage-9 comparative eval is
  apples-to-apples.
- Suite total: 88 tests green.

## [Stage 6] — 2026-05-09 — Knowledge graph (PropertyGraphIndex)

### Added
- `docker-compose.yml` — Neo4j 5-enterprise added (ports 7474/7687,
  apoc plugin, sane heap defaults, healthcheck via wget on 7474).
  `neo4j_data` and `neo4j_logs` volumes added.
- `src/graph/schema.py` — `EntityType` / `RelationType` Literal
  unions covering Russian B2B identifier types + generic ones.
  `DEFAULT_VALIDATION_SCHEMA` lists 13 valid (head, relation,
  tail) triples for `SchemaLLMPathExtractor` strict-mode.
- `src/graph/store.py` — `build_neo4j_graph_store()` factory.
- `src/graph/index.py`:
  - `build_kg_extractor(llm, strict=True, num_workers=2)` —
    `SchemaLLMPathExtractor` over the typed schemas.
  - `build_property_graph_index(graph_store, embed_model,
    extractor, nodes=None)` — composes index from store + embed +
    extractor; either populates from chunks or attaches to an
    existing store.
- `src/graph/retriever.py` — `GraphRetriever` async wrapper around
  `PropertyGraphIndex.as_retriever`; classifies returned nodes
  into entities / relations / chunks with the same dict shape
  enterprise-kb's `query_graph_data` produces.
- `src/retrieval/agent.py` — extended:
  - `GraphRetrieverProtocol` (runtime-checkable Protocol).
  - `_merge_graph(accumulated, fresh_entities, fresh_relations)`
    helper — dedup entities by name, relations by
    `src+tgt+label`.
  - `_accumulated_hl_keywords(graph, limit)` — top-N entity names,
    matches enterprise-kb Stage F.
  - `agentic_search(graph_retriever=None)` — optional graph hop
    per round; entities/relations participate in dedup + early-exit
    decision; graph chunks fold into the final synthesis node set.
  - `AgenticRoundStat.new_entities` and `new_relations` populated.
- `tests/test_graph/test_schema.py` — 3 tests on Literal coverage
  and validation schema soundness.
- `tests/test_retrieval/test_agent_graph.py` — 7 tests covering
  helpers (`_merge_graph`, `_accumulated_hl_keywords`) and
  end-to-end loop with a stub `GraphRetriever`: 2-round with graph,
  early-exit when both sources and graph stable, NO early-exit
  when only graph grows, graph chunks merged into synthesis input.

### Notes
- Live Neo4j NOT exercised in unit tests — would need a running
  container.  The graph integration is verified via stubs;
  end-to-end check is the manual `python -m src.ingestion.run`
  against the live stack (Stage 8).
- `SchemaLLMPathExtractor` itself isn't unit-tested here either —
  it requires a real LLM to produce triplets.  Tests confirm the
  factory builds without error; behaviour is verified in Stage 9
  eval against enterprise-kb on identical golden questions.
- Suite total: 44 tests green.

## [Stage 5] — 2026-05-09 — Hybrid retrieval (BM25 + vector + RRF)

### Added
- `src/retrieval/hybrid.py`:
  - `build_bm25_retriever(nodes, similarity_top_k)` — pure-Python
    BM25 over an in-memory node list.
  - `build_hybrid_retriever(vector_index, bm25_nodes, ...)` —
    `QueryFusionRetriever` (mode `reciprocal_rerank`) over the
    dense + BM25 retrievers.  ``num_queries=1`` disables built-in
    query expansion (the agent loop already covers expansion).
- `src/retrieval/reranker.py` — `build_reranker(model_name, top_n)`
  factory for `SentenceTransformerRerank`.  Default:
  `BAAI/bge-reranker-v2-m3`.  Heavy (~1 GB on first run), kept
  out of the default test path.
- `tests/test_retrieval/test_hybrid.py` — 3 tests: BM25 surfaces a
  keyword match, hybrid combines both retrievers, reranker factory
  importable.

### Notes
- `QueryFusionRetriever` resolves an LLM at construction even with
  expansion disabled — the factory now takes an explicit ``llm``
  arg so callers (and tests via `MockLLM`) avoid surprise OpenAI
  dependency.
- Agent module is **not modified** — Stage 5 swaps the retriever
  the caller passes into `agentic_search`.  This is the
  composable seam the plan promised.
- Suite total: 34 tests green.

## [Stage 4] — 2026-05-09 — Agentic loop (PRIORITY)

### Added
- `src/models/search.py` — Pydantic shapes (`SearchRequest`,
  `SearchResponse`, `SourceCitation`, `AgenticRoundStat`) mirroring
  enterprise-kb wire format.
- `src/retrieval/judge.py` — `LLMJudge` with the same JSON contract
  and defensive-fallback semantics as `enterprise-kb._judge_context`.
  Strips markdown fences, swallows JSON parse errors and LLM
  exceptions → `sufficient=True` with reason carrying the error
  text.
- `src/retrieval/agent.py` — `agentic_search()` async function:
  - Stub-friendly via `RetrieverProtocol`, `JudgeProtocol`,
    `SynthesizerProtocol`.
  - Per-round node dedup by `node.node_id`, delta tracking, early
    exit on barren follow-up rounds (round > 1 and `new_sources=0`).
  - `AgenticRoundStat` recorded per executed round, including
    skipped-judge entry on early-exit (`sufficient=None`,
    `reason="no new info"`).
  - Defensive: judge exceptions don't crash the loop.
  - Anti-loop: `follow_up == current_query` → break.
  - Final synthesis via `ResponseSynthesizer` over the *enriched*
    query (original + appended unique follow-ups) and *accumulated*
    nodes — matches enterprise-kb Stage F semantics.
- `tests/test_retrieval/test_agent.py` — 15 tests covering helpers
  (dedup, enriched query), end-to-end loop (1-round, 2-round,
  max_rounds, early-exit, follow-up loop guard, dedup across
  rounds, round_stats per round, judge defensive defaults), and
  `LLMJudge` direct unit tests (plain JSON, markdown fences,
  invalid JSON, LLM exception).

### Notes
- This is the project's **agentic-first** milestone — the agent
  loop works end-to-end on stub deps, hybrid retrieval / KG /
  canonicalisation will plug in without touching `agent.py`.
- Plan called for `AgentWorkflow`/Workflow primitives; opted for a
  plain async function instead — keeps the port from
  `enterprise-kb/agent_search.py` 1:1 readable, easier to reason
  about and benchmark. Migrating to Workflow primitives later is
  mechanical if needed.
- `hl_keywords` (Stage F in enterprise-kb) is intentionally absent
  here — Stage 4 has no KG, so there are no entity names to feed
  back. Stage 6 will introduce the graph-search tool and
  re-enable `hl_keywords`-style enrichment.
- Suite total: 31 tests green.

## [Stage 3] — 2026-05-09 — Vector index + basic query engine

### Added
- `src/retrieval/vector_index.py`:
  - `build_vector_store(overwrite=False)` — Milvus
    `BasePydanticVectorStore` from settings (cosine similarity).
  - `build_vector_index(store, embed_model)` — wraps any vector
    store into `VectorStoreIndex` (tests pass `SimpleVectorStore`
    in-memory).
  - `index_nodes(index, nodes)` — inserts pre-chunked nodes,
    returns count for diagnostics.
- `src/retrieval/query_engine.py`:
  - `build_basic_query_engine(index, llm, similarity_top_k=10)` —
    dense retriever + LLM synthesis. Stage 5 swaps the retriever
    underneath; this API stays stable.
- `src/retrieval/llm.py` — `build_llm()` factory using
  `OpenAILike` against the LiteLLM proxy.
- `src/ingestion/run.py` — CLI to ingest a directory end-to-end:
  read docs → pipeline → vector index. Flags:
  `--semantic`, `--overwrite-collection`, `--recursive/--no-recursive`.
- `tests/test_retrieval/test_vector_index.py` — 3 tests using
  `SimpleVectorStore` + `MockEmbedding` + `MockLLM`.

### Notes
- Live Milvus deliberately not exercised in unit tests — tests use
  the in-memory `SimpleVectorStore` so the suite stays fast and
  hermetic. Live Milvus is verified manually via
  `python -m src.ingestion.run`.
- Suite total: 16 tests green.

## [Stage 2] — 2026-05-09 — IngestionPipeline (parsing + chunking)

### Added
- `src/ingestion/embeddings.py` — `build_embedding_model()` factory
  returning `OpenAILikeEmbedding` wired to LiteLLM proxy.
- `src/ingestion/pipeline.py`:
  - `build_ingestion_pipeline(embed_model=None, semantic=False,
    cache_dir=None, extra_transformations=None)` — composes a
    LlamaIndex `IngestionPipeline`.
  - `read_documents(input_dir)` — `SimpleDirectoryReader` wrapper
    used by tests and (later) the worker.
  - Two splitter modes: `SentenceSplitter` (default, deterministic,
    no embed dep) and `SemanticSplitterNodeParser` (opt-in with
    `semantic=True` + embed_model).
  - Persistent disk cache via `IngestionCache` + `SimpleKVStore`.
  - `extra_transformations` hook reserved for Stage 7 (canonical
    identifier injection).
- `tests/test_ingestion/fixtures/sample.txt` — minimal multi-paragraph
  fixture.
- `tests/test_ingestion/test_pipeline.py` — 5 tests covering reader,
  sentence-splitter pipeline, semantic-splitter with `MockEmbedding`,
  cache persistence, and the `extra_transformations` hook.

### Notes
- Plan called for SemanticSplitter as default; downgraded to
  SentenceSplitter to avoid embedding round-trip per test run.
  Semantic mode is opt-in via the factory and exercised in tests
  with a `MockEmbedding`.
- Embedding *transformation* (i.e. attaching embeddings to nodes) is
  intentionally NOT in this pipeline yet — it gets bolted on in
  Stage 3 alongside the vector store, matching LlamaIndex's typical
  ingestion → vector-index split.
- Suite total: 13 tests green.

## [Stage 1] — 2026-05-09 — Minimal infra (Milvus + Postgres + LiteLLM)

### Added
- `docker-compose.yml` — etcd, minio, milvus 2.4.17, postgres 16,
  litellm proxy.  Neo4j + RabbitMQ deferred (Stage 6 / 8).
- `docker/litellm_config.yaml` — sample proxy config wiring LiteLLM
  to Ollama on the host (`host.docker.internal:11434`).  Replace
  with your upstream (OpenAI, Anthropic, Azure) for production.
- `scripts/start.sh` — `up`/`down`/`logs`/`ps` wrapper, prints
  service URLs after start.
- `scripts/setup_db.py` — idempotent Postgres `documents` table
  bootstrap + Milvus connectivity ping (collection lifecycle owned
  by `MilvusVectorStore` from Stage 3).
- `tests/test_scripts/test_setup_db.py` — DDL invariants, idempotency
  contract, callable signatures.

### Notes
- `documents` schema mirrors enterprise-kb:
  `id (UUID PK), path, department, doc_type, status, error, summary,
  created_at, updated_at`.  Status FSM:
  pending → processing → completed | failed.
- Milvus collection deliberately not provisioned in setup_db —
  LlamaIndex's `MilvusVectorStore` handles schema creation and is
  the source of truth for fields/dim.
- Suite total: 8 tests green.

## [Stage 0] — 2026-05-09 — Bootstrap

### Added
- `pyproject.toml` with uv-managed deps (Python 3.12, llama-index-core
  0.13.x, milvus/neo4j/openai-like/bm25/sbert-rerank connectors,
  FastAPI, Taskiq, dishka, pydantic-settings, loguru, phonenumbers,
  dateparser).  `postal` kept as `[postal]` extra — requires
  system libpostal, falls back to rule-based normalisation.
- Directory tree mirroring `enterprise-kb`:
  `src/{api/routes,di,ingestion,retrieval,graph,models,storage,utils}`,
  `tests/{test_api,test_ingestion,test_retrieval,test_scripts,test_storage,eval/golden_identifiers}`,
  `docs/{plans,specs}`, `scripts/`, `docker/`.
  `src/graph/` is the LlamaIndex-specific addition (PropertyGraphIndex
  in Stage 6).
- `src/config.py` — nested pydantic-settings (Api, Milvus, Neo4j,
  Postgres, LiteLLM, RabbitMQ, Ingestion, Agent).  Composed
  `Settings` with cached_property accessors.
- `src/utils/logging.py` — loguru bootstrap, JSON or human format.
- `tests/test_config.py` — 5 smoke tests covering imports, settings
  defaults, CSV parsing for API keys, Postgres DSN assembly, logging
  configuration.
- `.env.example`, `.gitignore`, `.python-version` (3.12), `README.md`
  with quickstart and stage status.

### Notes
- Initial `llama-index-vector-stores-milvus<0.7` constraint conflicted
  with `llama-index-core>=0.13` (milvus 0.5/0.6 require core 0.12).
  Relaxed sub-package upper bounds — uv resolved into core 0.13.6 +
  current connector versions.
- Verified key imports: `Workflow`, `IngestionPipeline`,
  `MilvusVectorStore`, `Neo4jPropertyGraphStore`, `OpenAILike`,
  `BM25Retriever`.
- `pytest` — 5/5 passing.

### Decision: directory tree
- Mirrors `enterprise-kb` 1:1 with one addition: `src/graph/` for
  PropertyGraphIndex code (LightRAG buried KG inside its own engine,
  LlamaIndex exposes graph as a separate index family — separate
  module is cleaner).
- `src/di/` reserved for dishka providers (Stage 8).
- `src/storage/` reserved for low-level Milvus/Neo4j/PG client
  wrappers if needed (Stage 1+).

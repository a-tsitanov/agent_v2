# Agentic Search Implementation Plan (Plan #2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve search from a single monolithic ReAct workflow into a set of logically separated, independently-scalable workflows that support query decomposition (plan-execute-synthesize), multi-hop graph traversal, corpus-level GraphRAG global search, and unified graph+vector reranking — producing detailed analytical answers over a large corpus.

**Architecture:** Decompose the monolithic `SearchWorkflow` into a thin `SearchOrchestratorWorkflow` that routes + plans + fans out to child workflows (`SubQueryRetrievalWorkflow`, `GlobalSearchWorkflow`) and a final synthesis step. Community detection/summaries are built by a fully decoupled offline `CommunityBuildWorkflow`. Models collapse to **two physical tiers** — `small` (local, high-volume) and `large` (final synthesis only) — with roles mapped declaratively to tiers, and Temporal task queues mapped to tiers so small/large work scales independently. Each search type gets its own API endpoint.

**Locked design decisions:**
- **A** — full replacement of the open ReAct loop with plan-execute-synthesize.
- **B** — multi-hop via a dedicated `graph_walk` tool (not a global `path_depth` bump).
- **C1** — community detection via Neo4j GDS (Leiden).
- **C2** — community summaries built offline in batch.
- **D** — large model used only for final synthesis.
- **Models:** `MODEL_SMALL=gemma4:e4b`, `MODEL_LARGE=gpt-4o-mini`.
- **Role→tier:** everything `small` except `synthesis` = `large`.
- **Queues:** `kb-search-small` (rename of `kb-search-llm`), `kb-search-large` (new), `kb-graph-build` (new, offline). Ingest queues unchanged.
- **Endpoints (one per search type):** `/api/v1/search/local`, `/search/global`, `/search/drift`, `/search/auto`, `/admin/communities/rebuild`.
- **Docs kept current per phase:** `docs/MODELS.md`, `docs/SEARCH.md` (new), `docs/QUEUES.md` (new).

**Tech Stack:** Python 3.12, LlamaIndex, Temporal, Neo4j (+ GDS plugin), Milvus, LiteLLM proxy (OpenAILike), FastAPI, pytest. Interpreter: `.venv/bin/python` (uv-managed venv; no bare `python`/`pytest`).

**Phasing rationale:** R0 creates the new package skeleton and R1 lands the 2-tier model config — both are foundational, low-risk, and fully specified below with code. R2–R7 build features on top of the R0 structure; they are specified as concrete task blocks (files, ordered steps, acceptance criteria, doc updates). Because R2+ code edits target files reshaped by R0, the exact in-method diffs for those phases are finalized by reading the then-current files at execution time — each task lists the precise files and the behavior contract to implement and test against. The legacy `SearchWorkflow` and its endpoints stay working behind a flag until a feature reaches parity, then are removed in the final phase.

---

## File Structure (target)

```
src/workflow/search/
  __init__.py
  orchestrator.py        # SearchOrchestratorWorkflow (thin: route → plan → fan-out → rerank → coverage → synthesize)
  subquery_wf.py         # SubQueryRetrievalWorkflow (plan-execute retrieval for one sub-question)
  global_wf.py           # GlobalSearchWorkflow (community map-reduce)
  community_wf.py        # CommunityBuildWorkflow (offline GDS Leiden + batch summaries)
  activities/
    __init__.py
    route.py             # classify local | global | drift
    plan.py              # decompose question → sub-questions
    retrieve.py          # one retrieval action: hybrid | graph | graph_walk
    rerank.py            # unified graph+vector rerank
    synthesize.py        # final synthesis (role 'synthesis' → large)
    community.py         # GDS Leiden detect + per-community summary
src/api/routes/search_v2.py   # per-type endpoints (local/global/drift/auto) + admin rebuild
src/config.py            # 2-tier model settings + new queues (modified)
src/retrieval/llm.py     # tier-aware builders (modified)
docs/MODELS.md           # rewritten for 2 tiers (modified)
docs/SEARCH.md           # new — endpoints + workflow map
docs/QUEUES.md           # new — queues ↔ tiers ↔ workers
```

The existing `src/workflow/search_workflow.py` and `src/api/routes/{search,agent,selfrag}.py` remain until the final cutover phase.

---

## Phase R0: Scaffold the `search/` package (no behavior change)

**Files:**
- Create: `src/workflow/search/__init__.py`
- Create: `src/workflow/search/activities/__init__.py`
- Create: `tests/test_workflow/test_search_pkg.py`

- [ ] **Step 1: Write a failing import test for the new package**

```python
# tests/test_workflow/test_search_pkg.py
def test_search_package_importable():
    import src.workflow.search as s
    import src.workflow.search.activities as a
    assert s is not None and a is not None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_search_pkg.py -v`
Expected: FAIL `ModuleNotFoundError: No module named 'src.workflow.search'`

- [ ] **Step 3: Create the package init files**

`src/workflow/search/__init__.py`:
```python
"""Search subsystem: orchestrator + per-mode child workflows.

Replaces the monolithic ``src/workflow/search_workflow.py`` incrementally.
Until cutover (final phase) the legacy workflow stays the default; new
workflows here are wired behind feature flags / new endpoints.
"""
```

`src/workflow/search/activities/__init__.py`:
```python
"""Temporal activities for the search subsystem (route/plan/retrieve/rerank/synthesize/community)."""
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_search_pkg.py -v`
Expected: PASS

- [ ] **Step 5: Update docs (note the restructuring is underway)**

Append a "Search subsystem refactor (in progress)" note to `docs/SEARCH.md` (create the file with a stub heading if absent).

- [ ] **Step 6: Commit**

```bash
git add src/workflow/search/ tests/test_workflow/test_search_pkg.py docs/SEARCH.md
git commit -m "feat(search): scaffold search/ package for workflow decomposition"
```

---

## Phase R1: Two-tier model configuration

**Files:**
- Modify: `src/config.py` (LiteLLMSettings + LLMRole/LLMTier)
- Modify: `src/retrieval/llm.py` (tier-aware builders)
- Create: `tests/test_config/test_model_tiers.py`
- Modify: `docs/MODELS.md`

- [ ] **Step 1: Write the failing test for tier resolution**

```python
# tests/test_config/test_model_tiers.py
from src.config import LiteLLMSettings


def test_roles_resolve_to_two_physical_models():
    cfg = LiteLLMSettings(model_small="gemma4:e4b", model_large="gpt-4o-mini")
    # everything small except synthesis
    assert cfg.model_for("extraction") == "gemma4:e4b"
    assert cfg.model_for("judge") == "gemma4:e4b"
    assert cfg.model_for("search") == "gemma4:e4b"
    assert cfg.model_for("plan") == "gemma4:e4b"
    assert cfg.model_for("synthesis") == "gpt-4o-mini"


def test_role_tier_override_changes_resolution():
    cfg = LiteLLMSettings(
        model_small="gemma4:e4b", model_large="gpt-4o-mini",
        role_tiers={"plan": "large"},  # operator escalates planning
    )
    assert cfg.model_for("plan") == "gpt-4o-mini"
    assert cfg.model_for("synthesis") == "gpt-4o-mini"  # default kept
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_config/test_model_tiers.py -v`
Expected: FAIL (model_small/model_large/role_tiers not defined; `model_for` only knows 3 roles)

- [ ] **Step 3: Implement the 2-tier model config in `src/config.py`**

Replace the `LLMRole` literal and the model fields/`model_for` in `LiteLLMSettings` with:

```python
LLMTier = Literal["small", "large"]
LLMRole = Literal[
    "extraction", "judge", "search",        # legacy roles (kept)
    "route", "plan", "retrieve", "distill", "coverage", "synthesis",  # search roles
]

_DEFAULT_ROLE_TIERS: dict[str, LLMTier] = {
    "extraction": "small",
    "judge": "small",
    "search": "small",
    "route": "small",
    "plan": "small",
    "retrieve": "small",
    "distill": "small",
    "coverage": "small",
    "synthesis": "large",
}
```

In `LiteLLMSettings`, replace `llm_model` + per-role override fields with the two-tier fields (keep `llm_model` as a deprecated alias that defaults `model_small` for any un-migrated caller):

```python
    model_small: str = "gemma4:e4b"
    """High-volume / latency-sensitive tier (extraction, judge, route, plan, retrieve, distill, coverage)."""
    model_large: str = "gpt-4o-mini"
    """Reserved tier for final synthesis (role 'synthesis') and heavy reasoning."""
    embedding_model: str = "nomic-embed-text"
    role_tiers: dict[str, LLMTier] = Field(default_factory=lambda: dict(_DEFAULT_ROLE_TIERS))
    """Role → tier map; override via env to escalate a role to 'large'."""

    # Deprecated: kept so un-migrated callers using build_llm() (no role) still work.
    llm_model: str = ""

    def tier_for(self, role: LLMRole) -> LLMTier:
        return self.role_tiers.get(role, "small")

    def model_for(self, role: LLMRole) -> str:
        return self.model_large if self.tier_for(role) == "large" else self.model_small
```

If `llm_model` is referenced elsewhere as a non-empty default, resolve it lazily: a `@property def effective_base(self): return self.llm_model or self.model_small`. Grep `grep -rn "llm_model" src/` and point any reader at `model_small` (or `effective_base`).

- [ ] **Step 4: Make `src/retrieval/llm.py` tier-aware**

```python
def build_llm(role: LLMRole | None = None) -> LLM:
    cfg = settings.litellm
    model = cfg.model_for(role) if role else cfg.model_small
    return _build(model)

def build_extraction_llm() -> LLM:   # -> small
    return build_llm("extraction")

def build_judge_llm() -> LLM:        # -> small
    return build_llm("judge")

def build_search_llm() -> LLM:       # -> small
    return build_llm("search")

def build_synthesis_llm() -> LLM:    # -> large
    return build_llm("synthesis")
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_config/test_model_tiers.py -v`
Expected: PASS

- [ ] **Step 6: No-regression across config consumers**

Run: `.venv/bin/python -c "import src.config, src.retrieval.llm"` and `.venv/bin/python -m pytest tests/test_config tests/test_retrieval -q`
Expected: PASS. If a test asserted on `llm_model`/`extraction_model`, update it to the tier fields.

- [ ] **Step 7: Rewrite `docs/MODELS.md` for the two tiers**

Document: the two physical models, the role→tier table, how to escalate a role via `LITELLM_ROLE_TIERS`, and the `MODEL_SMALL`/`MODEL_LARGE` env vars.

- [ ] **Step 8: Commit**

```bash
git add src/config.py src/retrieval/llm.py tests/test_config/test_model_tiers.py docs/MODELS.md
git commit -m "feat(config): two-tier model architecture (small/large) with role→tier mapping"
```

---

## Phase R2: Orchestrator + SubQueryRetrievalWorkflow (replace ReAct) + `/search/local`

**Goal:** Introduce plan-execute-synthesize. The orchestrator decomposes a question into sub-questions, runs one `SubQueryRetrievalWorkflow` per sub-question in parallel, aggregates sources (dedup by `chunk_id`), then synthesizes. No open-ended ReAct tool-picking loop.

**Files:** Create `src/workflow/search/activities/plan.py`, `src/retrieval/query_planner.py`, `src/workflow/search/subquery_wf.py`, `src/workflow/search/orchestrator.py`, `src/workflow/search/activities/retrieve.py`, `src/api/routes/search_v2.py`; modify worker registration (grep `register Workflow`/`activities=` in the search worker) and `src/api/main.py` (mount `search_v2.router`); modify `src/config.py` (flags + `kb-search-small` rename).

**Contract / acceptance:**
- `query_planner.decompose(question) -> list[str]` returns `[question]` for atomic questions and N≥2 sub-questions for compound ones (unit-tested with a mock LLM via role `plan` → small).
- `SubQueryRetrievalWorkflow(sub_question)` runs a deterministic retrieval pipeline (hybrid vector + graph) reusing the existing `atomic_tools` dispatch, applies distillation, returns deduped `NodeWithScore` sources — NO `agent_reasoning_step`.
- `SearchOrchestratorWorkflow(question, mode="local")` fans out child workflows with `asyncio`/`workflow.start_child_workflow`, gathers, dedups by `chunk_id`, calls `synthesize` (role `synthesis` → large).
- `POST /api/v1/search/local` starts the orchestrator on `kb-search-small`; returns answer + citations in the same response schema as the legacy `/search`.
- Legacy `SearchWorkflow` + `/search` untouched (parity flag).

**Steps (each TDD: failing test → impl → green → commit; doc update on SEARCH.md):**
- [ ] Planner `decompose` + unit test (atomic → 1, compound → N).
- [ ] `plan` activity wrapping the planner.
- [ ] `retrieve` activity reusing `atomic_tools.dispatch` (hybrid+graph).
- [ ] `SubQueryRetrievalWorkflow` + test (mock activities; asserts deduped sources, no reasoning step).
- [ ] `SearchOrchestratorWorkflow` + test (mock children; asserts parallel fan-out, chunk_id dedup, synthesize called once with role synthesis).
- [ ] `search_v2.py` `/search/local` endpoint + route test (mock workflow client).
- [ ] Register new workflows/activities on `kb-search-small`; rename queue constant (`search_task_queue = "kb-search-small"`); keep legacy registered too.
- [ ] Update `docs/SEARCH.md` (local flow + endpoint).

---

## Phase R3: Multi-hop `graph_walk` tool

**Goal:** Controlled multi-hop traversal as an explicit tool, leaving default `graph_search` at `path_depth=1`.

**Files:** modify `src/retrieval/atomic_tools.py` (add `graph_walk(start_entity, hops, rel_filter)` + register in dispatcher), `src/graph/retriever.py` (bounded N-hop query with node/edge caps), `src/workflow/search/activities/retrieve.py` (expose graph_walk), the agent/sub-query prompt; modify `docs/SEARCH.md`.

**Contract / acceptance:**
- `graph_walk(start_entity, hops=2, rel_filter=None)` returns entities + relations within `hops`, capped (e.g. ≤ 50 nodes), never expands unbounded.
- Default `graph_search` behavior unchanged (`path_depth=1`).
- Unit test with a fake graph store asserts hop bound + cap enforcement.

**Steps:** failing test for bounded walk → implement retriever query + tool + dispatcher entry → green → register tool in `retrieve` activity → doc update → commit.

---

## Phase R4: Coverage gate on the orchestrator

**Goal:** Move the pre-submit completeness check from the old loop to the orchestrator: after gathering sources, if evidence is incomplete and a gap is named, spawn additional sub-questions (bounded by `max_coverage_rounds`).

**Files:** move/adapt `src/workflow/activities/coverage_check.py` → `src/workflow/search/activities/` (or import it), modify `orchestrator.py`; `src/config.py` (`max_coverage_rounds`).

**Contract / acceptance:** orchestrator runs coverage check once (default), and on `complete=no` + named gap, issues the gap as an extra sub-question, re-gathers, then synthesizes; fail-open (any error → proceed to synthesize). Tested with mock coverage activity (gap path + fail-open path).

**Steps:** failing orchestrator test (gap → extra sub-question) → wire coverage activity → green → fail-open test → doc update → commit.

---

## Phase R5: `kb-search-large` queue + `synthesis` role + unified rerank

**Goal:** Final synthesis runs the large model on a dedicated low-concurrency queue; before synthesis, graph-derived and vector chunks are co-ranked in one rerank pass.

**Files:** `src/config.py` (`large_task_queue="kb-search-large"`, `large_activity_concurrency=2`); `src/workflow/search/activities/rerank.py` (unified pool rerank via bge-reranker); `src/workflow/search/activities/synthesize.py` (role `synthesis` → large, runs on large queue); `orchestrator.py` (call rerank before synthesize, schedule synthesize on large queue); the search worker setup (start a worker polling `kb-search-large`); `docs/QUEUES.md`.

**Contract / acceptance:**
- `rerank(query, nodes)` returns top-N over the *merged* graph+vector candidate pool (existing `reranker.py` reused).
- `synthesize` activity is scheduled on `kb-search-large` and builds its LLM via `build_synthesis_llm()`.
- A worker config polls both `kb-search-small` (high concurrency) and `kb-search-large` (concurrency 2).
- Unit tests: rerank merges+dedups+orders; synthesize uses the large model (assert `model_for("synthesis")`).

**Steps:** rerank test+impl → synthesize-on-large test+impl → orchestrator wiring → worker registration for `kb-search-large` → `docs/QUEUES.md` → commit.

---

## Phase R6: Offline `CommunityBuildWorkflow` (GDS Leiden + batch summaries) + `kb-graph-build`

**Goal:** Decoupled offline build of graph communities and their summaries; never on the query hot path.

**Files:** `src/graph/communities.py` (GDS Leiden projection + write `:Community {level, members}`); `src/workflow/search/activities/community.py` (detect + per-community summary via small model); `src/workflow/search/community_wf.py` (`CommunityBuildWorkflow`); `src/config.py` (`graph_build_task_queue="kb-graph-build"`); `src/api/routes/search_v2.py` (`POST /admin/communities/rebuild`); a Temporal schedule/cron registration; `docs/QUEUES.md` + `docs/SEARCH.md`.

**Contract / acceptance:**
- Leiden runs via Neo4j GDS (`gds.leiden.write` or stream→write), producing `:Community` nodes with member entities and a `level`.
- Each community gets a summary (small model) stored on `:Community.summary`.
- The workflow runs on `kb-graph-build`, is idempotent/incremental, and is triggerable by the admin endpoint and a schedule.
- Tests use a fake graph store / mock GDS calls (no live Neo4j in CI) asserting the detect→summarize→write sequence and idempotency.

**Steps:** community detect wrapper test+impl (mock GDS) → summary activity test+impl (mock LLM) → `CommunityBuildWorkflow` test → admin endpoint + schedule → docs → commit.

---

## Phase R7: `route` + `/search/global` + `/search/drift` (+ `/search/auto`) and legacy cutover

**Goal:** Routing decides local vs global vs drift; global runs community map-reduce; drift starts local then expands to communities. Then remove the legacy workflow/endpoints.

**Files:** `src/workflow/search/activities/route.py` (classify, small model); `src/workflow/search/global_wf.py` (`GlobalSearchWorkflow`: map over relevant `:Community.summary` → reduce with large model); `orchestrator.py` (route → dispatch local/global/drift); `src/api/routes/search_v2.py` (`/search/global`, `/search/drift`, `/search/auto`); finally delete `src/workflow/search_workflow.py` and `src/api/routes/{search,agent,selfrag,legacy_agent}.py` + their registrations once parity is confirmed; `docs/SEARCH.md`.

**Contract / acceptance:**
- `route(question) -> "local"|"global"|"drift"` (unit-tested with mock LLM on representative questions).
- `GlobalSearchWorkflow` map step answers per relevant community (small), reduce step synthesizes (large); tested with fake communities.
- `/search/global` and `/search/drift` start the right workflow on the right queues; `/search/auto` routes.
- Legacy removal is its own commit after parity tests pass; update all import sites.

**Steps:** route test+impl → global_wf test+impl → drift path → endpoints + route tests → parity verification vs legacy → delete legacy + update imports → docs → commit.

---

## Cross-cutting
- **Feature flags:** every new endpoint/workflow gated; defaults preserve current behavior until R7 cutover.
- **Tests:** extend `tests/eval/golden_qa` + `tests/eval/answer_quality.py` with multi-hop, thematic (global), and aggregate questions; unit-test planner/router/map-reduce with mock (small) LLMs; no live Neo4j/Milvus/proxy in CI (mock the stores/clients as existing search tests do).
- **Docs cadence:** `docs/MODELS.md` (R1), `docs/SEARCH.md` (R0, R2, R3, R6, R7), `docs/QUEUES.md` (R5, R6) — each touched in its phase's final step.
- **Sequencing:** R0→R1 are pure foundation (no behavior change). R2 unlocks the new flow behind `/search/local`. R3/R4/R5 layer on. R6 is an independent offline epic. R7 adds global/drift and removes legacy.

## Self-Review notes
- **Decisions coverage:** A → R2 (plan-execute replaces ReAct); B → R3 (`graph_walk` tool); C1 → R6 (GDS Leiden); C2 → R6 (offline batch summaries); D → R5 (`synthesis`=large on `kb-search-large`); 2 models → R1; role→tier → R1; 3 queues → R2 (small rename) + R5 (large) + R6 (graph-build); per-type endpoints → R2/R7; docs → every phase.
- **Granularity:** R0/R1 fully bite-sized with code; R2–R7 are concrete task blocks (files + contract + ordered steps) finalized to in-method diffs at execution against the R0-reshaped tree — appropriate for a multi-phase refactor where later phases depend on earlier structure existing.
- **No silent behavior change:** legacy stays until R7; flags everywhere.

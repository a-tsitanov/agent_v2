# Hierarchical communities + dynamic community selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replicate GraphRAG's community system — multi-level hierarchy + structured reports (incremental) + dynamic selection (semantic kNN + hierarchy descent) — replacing today's flat level-0 + O(N) lexical global ranking. Spec: `docs/superpowers/specs/2026-06-07-hierarchical-communities-dynamic-selection.md`.

**Architecture:** One `gds.leiden.stream(..., {includeIntermediateCommunities:true})` yields a dendrogram → materialise `:Community` per level + `PARENT_OF` edges. Reports (title/summary/findings) generated bottom-up, carried over when a community's `members_hash` is unchanged, embedded into a `community_report_vec` index. Global/drift select communities by semantic kNN (v1) and top-down descent (v2); lexical path kept as fail-open fallback. Opt-in via config; default off → today's behaviour byte-for-byte.

**Tech Stack:** Neo4j 5-enterprise + GDS 2.x (Leiden, vector index) + APOC, Temporal workflows, LiteLLM small/large tiers, pytest. Reuses the `er_vec` native-vector-index pattern shipped earlier.

**Decisions (spec review):** full hierarchy (all dendrogram levels, `community_max_levels` is only a safety ceiling); both v1+v2 selection (config enum `lexical|semantic|descent`); higher-level reports from child reports; keep lexical fallback.

---

## Phase 0 — SPIKE: verify the dendrogram shape (BLOCKING, decides hierarchy source)

GDS `includeIntermediateCommunities` returns the dendrogram of ONE Leiden run: the **last** column is the final (coarsest) partition = today's `communityId`; earlier columns are **finer**. Open question this phase answers empirically: does the coarsest column give a usefully small "top" for cheap descent, or is it already ~10–20k communities (then v2 descent must start at a higher/coarser synthesised level or we accept descent from a mid level)?

- [ ] **Step 1: Probe the dev graph (local Neo4j, read-only)**

Run (against the dev stack, never prod):
```cypher
CALL gds.graph.project('spike', '__Entity__',
  {ALL: {type:'*', orientation:'UNDIRECTED'}}) YIELD graphName;
CALL gds.leiden.stream('spike', {randomSeed:19, includeIntermediateCommunities:true})
YIELD nodeId, communityId, intermediateCommunityIds
RETURN size(intermediateCommunityIds) AS levels,
       [i IN range(0, size(intermediateCommunityIds)-1) |
          size(collect(DISTINCT intermediateCommunityIds[i]))] AS approx_counts
LIMIT 1;
CALL gds.graph.drop('spike') YIELD graphName;
```
(or a Python equivalent via the project store). Record: number of dendrogram levels, and distinct-community count per level.

- [ ] **Step 2: Decide & document in this plan**

- If the coarsest column is small enough (≤ a few hundred) → **single-run dendrogram** hierarchy (Phase 1 as written; level 0 = coarsest = today, levels increase = finer).
- If the coarsest is still ~thousands → note it; v2 descent will start at the coarsest available level (still cheaper than flat MAP over all) and a future **recursive-coarsening** task (run Leiden on the contracted community graph for a genuine small top) goes to the backlog. Do NOT block v1 on this.

Write the finding (counts + chosen branch) into a `> SPIKE RESULT:` note here before proceeding.

---

## Phase 1 — hierarchy detection (levels + PARENT_OF + members_hash)

**Files:** `src/graph/communities.py`, `src/workflow/contracts.py`, tests `tests/test_graph/test_communities.py`

**Level convention (back-compat):** level 0 = coarsest (dendrogram last column = today's `communityId`; entities keep `(:__Entity__)-[:IN_COMMUNITY]->(:Community {level:0})` unchanged). Level k = dendrogram column `(D-1-k)` (finer). `PARENT_OF` goes coarser→finer: `(:Community {level:k})-[:PARENT_OF]->(:Community {level:k+1})`.

- [ ] **Step 1: Failing tests — dendrogram → per-level + parents + hash**

```python
def test_members_hash_order_insensitive():
    from src.graph.communities import members_hash
    assert members_hash(["B","A"]) == members_hash(["A","B"])
    assert len(members_hash(["A"])) == 64

def test_group_by_levels_maps_dendrogram_and_parents():
    from src.graph.communities import _group_by_levels
    # node → intermediateCommunityIds (finest..coarsest). 3 nodes, 2 levels.
    rows = [
        {"name":"a","ids":[10, 1]},
        {"name":"b","ids":[10, 1]},
        {"name":"c","ids":[11, 1]},
    ]
    levels = _group_by_levels(rows, min_size=1, max_levels=10)
    # level 0 = coarsest (id 1) holds a,b,c; level 1 (finer) has {a,b}=10, {c}=11
    l0 = {c.community_id: set(c.members) for c in levels if c.level == 0}
    l1 = {c.community_id: set(c.members) for c in levels if c.level == 1}
    assert l0 == {"1": {"a","b","c"}}
    assert l1 == {"10": {"a","b"}, "11": {"c"}}
    # parent of finer 10/11 is coarser 1
    assert all(c.parent_id == "1" for c in levels if c.level == 1)
```

- [ ] **Step 2: `members_hash` + `_group_by_levels`**

In `communities.py`:
```python
import hashlib

def members_hash(members: list[str]) -> str:
    return hashlib.sha256("\x1f".join(sorted(members)).encode("utf-8")).hexdigest()

def _group_by_levels(rows, *, min_size: int, max_levels: int):
    """rows: [{name, ids:[finest..coarsest]}] → list[CommunityRef] across
    levels. level 0 = coarsest (ids[-1]); level k = ids[-(k+1)]. parent_id
    of a level-k community = the level-(k-1) community its members map to."""
    if not rows:
        return []
    depth = min(len(rows[0]["ids"]), max_levels)
    out: list[CommunityRef] = []
    # node name → community id at each level (level 0..depth-1)
    node_level_cid: dict[str, list[str]] = {
        r["name"]: [str(r["ids"][-(k+1)]) for k in range(depth)] for r in rows
    }
    for k in range(depth):
        members_by_cid: dict[str, list[str]] = {}
        parent_by_cid: dict[str, str] = {}
        for name, cids in node_level_cid.items():
            cid = cids[k]
            members_by_cid.setdefault(cid, []).append(name)
            if k > 0:
                parent_by_cid[cid] = cids[k-1]   # coarser level is the parent
        for cid, members in members_by_cid.items():
            if len(members) < min_size:
                continue
            out.append(CommunityRef(
                community_id=cid, level=k, members=sorted(members),
                members_hash=members_hash(members),
                parent_id=parent_by_cid.get(cid, ""),
                needs_report=True,
            ))
    return out
```

- [ ] **Step 3: Extend `CommunityRef`** (contracts.py) with `members_hash: str = ""`, `parent_id: str = ""`, `needs_report: bool = True` (frozen pydantic → set at construction as above).

- [ ] **Step 4: `detect_hierarchy` + materialisation Cypher**

Add `includeIntermediateCommunities:true` to `_leiden_stream_cypher` (return `intermediateCommunityIds`). New `detect_hierarchy(store, *, max_levels, min_size)`:
- project (reuse `_project_cypher`), stream, `_group_by_levels`.
- before writing, read old reports (Phase 2 carry-over).
- prune ALL `:Community` (all levels) — extend `_PRUNE_LEVEL_CYPHER` to prune by level in a loop, or a prune-all.
- MERGE per community with `level, member_count, members_hash` (extend `_MERGE_COMMUNITY_CYPHER`); for level 0 keep the `IN_COMMUNITY` entity links; for level k>0 link members differently — **PARENT_OF**: `MATCH (child:Community {id:$cid, level:$level}), (parent:Community {id:$parent_id, level:$level-1}) MERGE (parent)-[:PARENT_OF]->(child)`.

Provide `_MERGE_COMMUNITY_CYPHER` (level-0, entity links) and `_MERGE_SUBCOMMUNITY_CYPHER` (level>0, PARENT_OF) as two queries. Keep `detect_communities(level=0)` working by delegating to `detect_hierarchy(max_levels=1)`.

- [ ] **Step 5: Run Phase-1 tests.** `.venv/bin/python -m pytest tests/test_graph/test_communities.py -q`

- [ ] **Step 6: Commit** — `feat(community): hierarchical Leiden detection (levels + PARENT_OF + members_hash)`

---

## Phase 2 — structured reports, bottom-up, incremental, embedded

**Files:** `src/workflow/search/activities/community.py`, `src/graph/index.py`, `src/graph/communities.py` (read-old-reports), contracts, tests.

- [ ] **Step 1: Report contracts + prompt**

Report shape `{title, summary, findings:[{statement, importance}]}`. Add a small-tier prompt `_REPORT_PROMPT` that, given member context (level 0) OR child reports (level>0), returns that JSON. Parser tolerant (fall back to `{title:"", summary:raw, findings:[]}`).

- [ ] **Step 2: Carry-over read (incremental)**

In `communities.py`, before prune:
```python
_READ_OLD_REPORTS_CYPHER = """
MATCH (c:Community)
WHERE c.members_hash IS NOT NULL AND c.report IS NOT NULL
RETURN c.level AS level, c.members_hash AS h, c.report AS report,
       c.title AS title, c.summary AS summary, c.report_vec AS report_vec
"""
async def _read_old_reports(store) -> dict[tuple[int,str], dict]:
    rows = await asyncio.to_thread(_run_query, store, _READ_OLD_REPORTS_CYPHER)
    return {(int(r["level"]), r["h"]): r for r in (rows or []) if r.get("h")}
```
Carry over: for each detected community, if `(level, members_hash)` in old map → write the old report+title+summary+report_vec in the MERGE and set `needs_report=False` (no LLM, no re-embed).

- [ ] **Step 3: Report write Cypher + report_vec**

Replace `_WRITE_SUMMARY_CYPHER` with `_WRITE_REPORT_CYPHER` setting `c.report` (JSON), `c.title`, `c.summary`, `c.report_vec` (native list), `c.summarized_at`.

- [ ] **Step 4: `ensure_community_report_vector_index`** (index.py, mirrors `ensure_er_vector_index`):
```python
COMMUNITY_REPORT_VECTOR_INDEX_CYPHER = (
    "CREATE VECTOR INDEX community_report_vec IF NOT EXISTS "
    "FOR (c:Community) ON c.report_vec "
    "OPTIONS {indexConfig: {`vector.dimensions`: $dim, "
    "`vector.similarity_function`: 'cosine'}}"
)
def ensure_community_report_vector_index(store, dim: int) -> bool: ...  # fail-open
```

- [ ] **Step 5: `summarize_community_activity` → report activity**

Rework the activity: level-0 communities build context from members (`_MEMBER_CONTEXT_CYPHER`); level>0 from child reports (new `_CHILD_REPORTS_CYPHER` reading `(c)-[:PARENT_OF]->(child)` reports). Produce report JSON, embed title+summary, write via `_WRITE_REPORT_CYPHER`. Fail-open per community.

- [ ] **Step 6: Tests** — report JSON parse + fallback; carry-over skips unchanged `(level,hash)`; report_vec written; index DDL + fail-open. Run the suite.

- [ ] **Step 7: Commit** — `feat(community): structured reports (bottom-up, incremental carry-over, report_vec index)`

---

## Phase 3 — build workflow drives hierarchy bottom-up, reports only what changed

**Files:** `src/workflow/search/community_wf.py`, contracts.

- [ ] **Step 1:** `CommunityBuildWorkflow.run` calls `detect_hierarchy` (not single-level). Summarise **bottom-up** (finest level first, so child reports exist before parents) and **only** communities with `needs_report=True`. Ensure the report vector index after writes.

- [ ] **Step 2:** `build_summarize_specs` emits specs only for `needs_report` communities, grouped/ordered by level descending (finest→coarsest). Carried-over communities are skipped.

- [ ] **Step 3: Test** — given a detect result with mixed `needs_report` + 2 levels, specs cover only changed and are ordered finest-first. Run.

- [ ] **Step 4: Commit** — `feat(community): build workflow drives hierarchy bottom-up, reports only changed`

---

## Phase 4 — dynamic selection (v1 semantic + v2 descent)

**Files:** `src/workflow/search/activities/global_search.py`, config, tests.

- [ ] **Step 1: v1 — semantic selection (kNN over report_vec)**

New `select_communities_semantic(store, query_vec, *, level, limit)`:
```cypher
CALL db.index.vector.queryNodes('community_report_vec', $limit, $vec) YIELD node, score
WHERE node.level = $level
RETURN node.id AS community_id, node.level AS level, node.report AS report,
       node.summary AS summary, coalesce(node.member_count,0) AS member_count
ORDER BY score DESC
```
Returns the same `CommunitySummaryRef`-shaped rows the existing MAP consumes (so MAP→REDUCE is untouched).

- [ ] **Step 2: v2 — hierarchy descent**

`select_communities_descent(store, query_vec, *, start_level, budget)`: start at the coarsest level (per Phase-0 finding), score communities (cosine vs query_vec via report_vec, threshold), keep relevant, descend via `PARENT_OF` into children, repeat to level 0 or until `budget` reports collected. Returns the surviving leaf/relevant reports for MAP. Pure traversal + scoring — testable with a mock store returning a fixed tree + vectors.

- [ ] **Step 3: Selection strategy switch + fallback**

In `map_communities` (global_search), branch on `settings.agent.community_dynamic_selection` enum: `descent` → v2; `semantic` → v1; `lexical` (or any failure / no index) → existing `rank_summaries`. Always fail-open to lexical.

- [ ] **Step 4: Tests** — v1 picks nearest report (mock embeddings); v2 keeps-relevant/prunes-irrelevant subtree + respects budget (mock tree); strategy switch + fallback-on-error returns lexical results. Run.

- [ ] **Step 5: Commit** — `feat(community): dynamic selection — semantic kNN (v1) + hierarchy descent (v2)`

---

## Phase 5 — config, wiring, live verification

**Files:** `src/config.py`, `src/workflow/search/global_wf.py`, `src/workflow/search/activities/__init__.py` / worker registration.

- [ ] **Step 1: Config** (`AgentSettings` / `TemporalSettings`): `community_max_levels:int=10` (safety ceiling), `community_dynamic_selection:Literal["lexical","semantic","descent"]="lexical"` (default off → today), `community_report_*` knobs, `global_descent_budget:int`. Embed model dim from `settings.milvus.dim`.

- [ ] **Step 2: Wire global_wf** to pass the strategy + embed the query (reuse the embedding model) into `map_communities`. No change when `lexical`.

- [ ] **Step 3: Full regression** — `.venv/bin/python -m pytest tests/test_graph tests/test_workflow -q` (Temporal-bound tests skip if down). Confirm default (`lexical`, `max_levels` irrelevant when build not re-run) leaves existing behaviour green.

- [ ] **Step 4: Live smoke (local dev stack, isolated/cleaned)** — build hierarchy on the dev graph: assert ≥2 levels materialise + `PARENT_OF` edges + `report_vec` populated + `community_report_vec` ONLINE; rebuild with no change → carried-over (report LLM calls ≈ 0); a `descent` global query returns selected reports. NEVER touch prod; clean up the index if the dev graph shouldn't keep it.

- [ ] **Step 5: Extend `tests/eval/scale/`** with a community-hierarchy probe (levels + per-level counts on a synthetic graph) so hierarchy size stays measurable.

- [ ] **Step 6: Commit** — `feat(community): config + wire dynamic selection into global; live-verified`

---

## Self-Review

**Spec coverage:** hierarchy (P1) ✓, structured reports + incremental + embeddings (P2) ✓, build orchestration (P3) ✓, v1+v2 selection + fallback (P4) ✓, config/wiring/verify (P5) ✓. Phase 0 spike de-risks the one real unknown (dendrogram coarseness).

**Back-compat:** `community_dynamic_selection="lexical"` (default) + `detect_communities` delegating to `detect_hierarchy(max_levels=1)` → today's flat path. New columns/edges/index are additive and fail-open.

**Type consistency:** `members_hash`→str(64); `CommunityRef` gains `members_hash/parent_id/needs_report`; selection activities return the existing `CommunitySummaryRef` shape so MAP→REDUCE is unchanged; `ensure_community_report_vector_index` mirrors `ensure_er_vector_index`.

**Open risks:** (1) Phase-0 may show the dendrogram top is too granular → descent starts mid-level + recursive-coarsening to backlog (does not block v1). (2) bottom-up report ordering must complete finer levels before parents — enforced by level-descending spec emission (P3 Step 2). (3) GDS `includeIntermediateCommunities` availability — Phase 0 confirms on the actual install.

**Effort:** L. P0 ≈ S (spike), P1/P2/P4 ≈ M each, P3/P5 ≈ S–M.

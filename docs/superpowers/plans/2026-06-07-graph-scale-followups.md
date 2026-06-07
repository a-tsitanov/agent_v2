# Graph-scale follow-ups (items 8 / 13 / 12) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship three cheap, orthogonal improvements from the 250k-entity scaling review — (8) fulltext walk-seeding, (13) drift graceful fallback, (12) community indexes — each independently testable and (where it changes behaviour) opt-in.

> **Scope note:** Item 11 (incremental community summarisation) was moved to `docs/superpowers/backlog-graph-scale.md` — it is **superseded** by the hierarchical-communities + dynamic-selection track (its own spec/plan), which reshapes how/when community reports are (re)generated. This plan now covers 8/13/12 only.

**Architecture:** All three are additive/low-risk. 8 & 13 are search-path resilience/recall; 12 is two idempotent Neo4j indexes.

**Tech Stack:** Python 3.12, LlamaIndex Neo4j PG store, Temporal workflows, Neo4j 5.x (vector/range indexes, APOC, GDS Leiden), pytest.

**Scope note discovered while planning (item 8):** `find_entity_by_name` (fulltext) is ALREADY a permanent step in the retrieve pipeline (`_PIPELINE`), and its top entity ALREADY seeds `graph_walk` — but only when `graph_search` returned NO entity (`retrieve.py:138-139` uses `graph_search OR find_name`). The real remaining gap is narrow: when `graph_search` returns *some* (possibly wrong) entity, the fulltext-matched entity is dropped and never contributes its neighbourhood chunks. Item 8 below closes exactly that.

---

## Phase 1 — Item 8: dual walk-seed (graph_search + fulltext)

**Files:**
- Modify: `src/config.py` (AgentSettings — new flag)
- Modify: `src/workflow/search/activities/retrieve.py:131-154` (walk-seed block)
- Test: `tests/test_workflow/test_retrieve_dual_seed.py` (create)

**Current seam (`retrieve.py:135-149`):** one seed, one walk —
```python
seed_obs = graph_search_obs if graph_search_obs is not None else find_name_obs
if settings.agent.graph_walk_enabled and seed_obs is not None:
    start = top_entity_name(graph_search_obs or "") \
        or top_entity_name(find_name_obs or "")
    if start:
        walk = await atomic_tools.dispatch("graph_walk", {"start_entity": start, "hops": settings.agent.graph_walk_hops}, graph_retriever=graph_retriever)
        _merge_sources(walk.sources)
```

- [ ] **Step 1: Add the config flag**

In `src/config.py`, `AgentSettings`, next to `graph_walk_enabled`:
```python
    # When on, graph_walk is seeded from BOTH the top graph_search entity
    # AND the top find_entity_by_name (fulltext) entity when they differ —
    # so a fulltext-matched entity (partial name / typo) still contributes
    # its neighbourhood even if graph_search already returned something.
    graph_walk_dual_seed: bool = True
```

- [ ] **Step 2: Write the failing test**

`tests/test_workflow/test_retrieve_dual_seed.py`:
```python
import pytest
from src.workflow.search.activities.retrieve import _walk_seeds


def test_walk_seeds_unions_distinct_graph_and_fulltext_seeds():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Beta"}]}'
    assert _walk_seeds(gs, fn, dual=True) == ["Alpha", "Beta"]


def test_walk_seeds_dedupes_when_same():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Alpha"}]}'
    assert _walk_seeds(gs, fn, dual=True) == ["Alpha"]


def test_walk_seeds_single_when_dual_off_matches_legacy():
    gs = '{"entities":[{"entity_name":"Alpha"}]}'
    fn = '{"entities":[{"entity_name":"Beta"}]}'
    assert _walk_seeds(gs, fn, dual=False) == ["Alpha"]      # graph_search wins
    assert _walk_seeds("", fn, dual=False) == ["Beta"]        # falls back to fulltext
    assert _walk_seeds("", "", dual=True) == []
```

- [ ] **Step 3: Run it — expect ImportError / fail**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_retrieve_dual_seed.py -q`
Expected: FAIL (`_walk_seeds` undefined).

- [ ] **Step 4: Extract the pure seed-selection helper**

In `retrieve.py`, above `retrieve_subquestion`, add (uses the existing `top_entity_name`):
```python
def _walk_seeds(graph_search_obs: str, find_name_obs: str, *, dual: bool) -> list[str]:
    """Seed entity name(s) for graph_walk.

    Legacy (dual=False): graph_search's top entity, else fulltext's — one
    seed.  dual=True: the union of both (deduped, order: graph_search
    first) so a fulltext-matched entity also contributes its neighbourhood
    even when graph_search returned something."""
    gs = top_entity_name(graph_search_obs or "")
    fn = top_entity_name(find_name_obs or "")
    if not dual:
        return [s for s in (gs or fn,) if s]
    out: list[str] = []
    for s in (gs, fn):
        if s and s not in out:
            out.append(s)
    return out
```

- [ ] **Step 5: Rewire the walk-seed block to loop over seeds**

Replace `retrieve.py:135-149` body with:
```python
    if settings.agent.graph_walk_enabled:
        seeds = _walk_seeds(
            graph_search_obs or "", find_name_obs or "",
            dual=settings.agent.graph_walk_dual_seed,
        )
        for start in seeds:
            try:
                walk = await atomic_tools.dispatch(
                    "graph_walk",
                    {"start_entity": start, "hops": settings.agent.graph_walk_hops},
                    graph_retriever=graph_retriever,
                )
                _merge_sources(walk.sources)
            except Exception as exc:
                activity.logger.warning(
                    "retrieve_subquestion  graph_walk skipped  start=%s  err=%s",
                    start, exc,
                )
                errors.append(f"graph_walk: {exc}")
```
(`_merge_sources` already dedupes by chunk_id, so unioning two walks is safe.)

- [ ] **Step 6: Run tests + retrieve regression**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_retrieve_dual_seed.py tests/test_workflow -q -k "retrieve or dual"`
Expected: PASS.

- [ ] **Step 7: Commit** — `feat(search): dual walk-seed (graph_search + fulltext) for entity recall`

---

## Phase 2 — Item 13: drift graceful fallback on global failure

**Files:**
- Modify: `src/workflow/search/router_wf.py:72-106` (`DriftSearchWorkflow.run`)
- Test: `tests/test_workflow/test_drift_fallback.py` (create)

**Current:** the global child raises on timeout/failure → whole drift request fails even though the local pass already produced an answer.

- [ ] **Step 1: Write the failing test for the pure fallback helper**

`tests/test_workflow/test_drift_fallback.py`:
```python
from src.workflow.contracts import SearchOutcome
from src.workflow.search.router_wf import _drift_local_fallback


def _outcome(**kw):
    base = dict(query="q", mode="local", answer="A", sources=[], documents=["d1"])
    base.update(kw)
    return SearchOutcome(**base)


def test_drift_fallback_relabels_local_as_drift():
    local = _outcome(mode="local", answer="local ans", documents=["d1"])
    out = _drift_local_fallback(local)
    assert out.mode == "drift"
    assert out.answer == "local ans"
    assert out.documents == ["d1"]
```

- [ ] **Step 2: Run it — expect fail**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_drift_fallback.py -q`
Expected: FAIL (`_drift_local_fallback` undefined).

- [ ] **Step 3: Add the helper**

In `router_wf.py`, above `DriftSearchWorkflow`:
```python
def _drift_local_fallback(local: SearchOutcome) -> SearchOutcome:
    """When the global pass of drift fails, degrade to the local answer
    but keep the ``drift`` mode label (so callers/metrics see the request
    was drift, just degraded)."""
    return local.model_copy(update={"mode": "drift"})
```

- [ ] **Step 4: Run helper test — expect pass**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_drift_fallback.py -q`
Expected: PASS.

- [ ] **Step 5: Wrap the global child in try/except**

Replace the global block (`router_wf.py:96-105`) with:
```python
        drift_global = global_params.model_copy(update={"drift_mode": True})
        try:
            outcome: SearchOutcome = await workflow.execute_child_workflow(
                GlobalSearchWorkflow.run,
                args=[drift_global, list(local.sources)],
                id=f"{workflow.info().workflow_id}-global",
                result_type=SearchOutcome,
            )
        except Exception as exc:  # ChildWorkflowError / timeout / activity failure
            log.warning("drift_search: global pass failed, degrading to local: %s", exc)
            return _drift_local_fallback(local)
        return outcome.model_copy(update={
            "documents": merge_doc_ids(list(local.documents), list(outcome.documents)),
        })
```
Note: catch broad `Exception` — in Temporal a failed/timed-out child surfaces as `ChildWorkflowError` (a subclass), and we want ANY global failure to degrade rather than fail the request. Determinism is preserved (no wall-clock branch).

- [ ] **Step 6: Run the workflow test suite (live-Temporal ones skip when down)**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_drift_fallback.py tests/test_workflow -q -k "drift or router"`
Expected: PASS (helper test passes; Temporal-bound child tests skip if Temporal is down).

- [ ] **Step 7: Commit** — `fix(search): drift degrades to local answer when global pass fails`

---

## Phase 3 — Item 12: community indexes

**Files:**
- Modify: `src/graph/index.py` (add two DDLs + ensure fn, mirroring `ensure_entity_lookup_indexes`)
- Modify: `src/graph/communities.py:206-208` (call the ensure fn in the build path)
- Test: `tests/test_graph/test_index.py` (extend)

**Backed queries today (unindexed):** `Community.level` (`global_search.py:43`, `communities.py:103`), `Chunk.doc_id` (`documents.py`). `Community.id` is already covered by the `community_key` constraint.

- [ ] **Step 1: Write the failing DDL test**

Append to `tests/test_graph/test_index.py`:
```python
def test_ensure_community_indexes_ddl_and_failopen():
    from src.graph.index import (
        COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER,
        ensure_community_indexes,
    )
    assert "FOR (c:Community) ON (c.level)" in COMMUNITY_LEVEL_INDEX_CYPHER
    assert "FOR (c:Chunk) ON (c.doc_id)" in CHUNK_DOC_ID_INDEX_CYPHER
    assert all("IF NOT EXISTS" in q for q in (COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER))

    ran = []
    class _Store:
        def structured_query(self, c, param_map=None): ran.append(c)
    assert ensure_community_indexes(_Store()) is True
    assert ran == [COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER]

    class _Boom:
        def structured_query(self, c, param_map=None): raise RuntimeError("x")
    assert ensure_community_indexes(_Boom()) is False
```

- [ ] **Step 2: Run — expect fail**

Run: `.venv/bin/python -m pytest tests/test_graph/test_index.py::test_ensure_community_indexes_ddl_and_failopen -q`
Expected: FAIL (ImportError).

- [ ] **Step 3: Add the DDLs + ensure fn**

In `src/graph/index.py`, after `ensure_er_vector_index`:
```python
COMMUNITY_LEVEL_INDEX_CYPHER = (
    "CREATE INDEX community_level IF NOT EXISTS FOR (c:Community) ON (c.level)"
)
CHUNK_DOC_ID_INDEX_CYPHER = (
    "CREATE INDEX chunk_doc_id IF NOT EXISTS FOR (c:Chunk) ON (c.doc_id)"
)


def ensure_community_indexes(store) -> bool:
    """Idempotent range indexes for the community/global read paths
    (Community.level filter, Chunk.doc_id traversal).  Fail-open."""
    ok = True
    for cypher in (COMMUNITY_LEVEL_INDEX_CYPHER, CHUNK_DOC_ID_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_community_indexes failed: {e}", e=exc)
            ok = False
    return ok
```

- [ ] **Step 4: Wire into the community build path**

In `src/graph/communities.py`, after the constraint is ensured (`line 208`, `await asyncio.to_thread(_run_query, store, _COMMUNITY_CONSTRAINT)`), add:
```python
        from src.graph.index import ensure_community_indexes
        await asyncio.to_thread(ensure_community_indexes, store)
```

- [ ] **Step 5: Run tests**

Run: `.venv/bin/python -m pytest tests/test_graph/test_index.py tests/test_graph/test_communities.py -q`
Expected: PASS.

- [ ] **Step 6: Commit** — `perf(community): add Community.level + Chunk.doc_id indexes for global reads`

---

## Self-Review

**Spec coverage:** 8 (dual seed) ✓, 13 (drift fallback) ✓, 12 (two indexes) ✓.

**Type consistency:** `_walk_seeds`/`top_entity_name` (list[str]); `_drift_local_fallback`→`SearchOutcome`; `ensure_community_indexes`/`ensure_entity_lookup_indexes` same shape.

**Effort:** 8 ≈ S, 13 ≈ S, 12 ≈ S. All independent; ship in any order.

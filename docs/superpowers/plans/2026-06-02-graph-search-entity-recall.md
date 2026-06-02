# Graph-search Entity Recall Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the main `/search/*` path reliably find an entity by (partial) name on a large graph via a Neo4j full-text index + a `find_entity_by_name` tool, and lift general recall by raising the graph retriever's `similarity_top_k`.

**Architecture:** Add a full-text index on `:__Entity__(name)` (created idempotently on ingest AND at retriever bootstrap, so existing graphs are covered). A new `GraphRetriever.afind_entities_by_name` runs a Lucene OR-of-tokens query; a `find_entity_by_name` atomic tool wraps it and joins the deterministic `retrieve_subquestion` pipeline, where its top hit can also seed the existing bounded `graph_walk`. All paths fail-open.

**Tech Stack:** Neo4j full-text (`db.index.fulltext.queryNodes`), LlamaIndex PropertyGraph, Temporal activities, Pydantic settings, pytest. Spec: `docs/superpowers/specs/2026-06-02-graph-search-entity-recall-design.md`.

---

## File Structure

- `src/graph/retriever.py` — **modify**: pure `build_fulltext_query`, `GraphRetriever.afind_entities_by_name` (+ store `similarity_top_k`).
- `src/graph/index.py` — **modify**: `ENTITY_FULLTEXT_INDEX_CYPHER` + `ensure_entity_fulltext_index`.
- `src/retrieval/atomic_tools.py` — **modify**: `find_entity_by_name` tool + protocol method + dispatch + description.
- `src/workflow/search/activities/retrieve.py` — **modify**: pipeline + seed fallback.
- `src/config.py` — **modify**: `AgentSettings.graph_similarity_top_k`.
- `src/workflow/_search_deps.py` — **modify**: pass top_k + ensure index at bootstrap.
- `src/workflow/activities/build_property_graph.py` — **modify**: ensure index on ingest.
- Tests: `tests/test_graph/test_retriever_fulltext.py` (new), `tests/test_graph/test_index.py` (extend), `tests/test_retrieval/test_find_entity_by_name.py` (new), `tests/test_workflow/test_search_retrieve.py` (extend), `tests/test_config/test_settings.py` (extend).

---

### Task 1: `build_fulltext_query` pure helper

**Files:**
- Modify: `src/graph/retriever.py`
- Test: `tests/test_graph/test_retriever_fulltext.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_retriever_fulltext.py`:
```python
"""Tests for the full-text entity-name lookup helpers."""

from __future__ import annotations


def test_build_fulltext_query_or_tokens_escaped():
    from src.graph.retriever import build_fulltext_query

    assert build_fulltext_query("Иванов Иван") == "Иванов OR Иван"
    # Lucene special chars are escaped per token.
    assert build_fulltext_query("a:b (x)") == r"a\:b OR \(x\)"
    # Blank input → empty query (caller short-circuits).
    assert build_fulltext_query("   ") == ""
    assert build_fulltext_query("") == ""
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_retriever_fulltext.py::test_build_fulltext_query_or_tokens_escaped -v`
Expected: FAIL — `ImportError: cannot import name 'build_fulltext_query'`.

- [ ] **Step 3: Implement**

In `src/graph/retriever.py`, add `import re` to the imports, and a module-level helper (after the `_WALK_CYPHER*` constants, before `class GraphRetriever`):
```python
# Lucene special chars that must be backslash-escaped inside a query term.
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}\[\]^"~*?:\\/&|])')


def build_fulltext_query(query: str) -> str:
    """Build a Lucene OR-of-tokens query for the ``entity_name_fulltext``
    index: whitespace-split the input, escape Lucene special chars in each
    token, join with ``OR``.  Returns ``""`` for blank input so the caller
    short-circuits to empty results (never issues a bare/invalid query)."""
    tokens: list[str] = []
    for raw in (query or "").split():
        esc = _LUCENE_SPECIAL.sub(r"\\\1", raw).strip()
        if esc:
            tokens.append(esc)
    return " OR ".join(tokens)
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_retriever_fulltext.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/retriever.py tests/test_graph/test_retriever_fulltext.py
git commit -m "feat(graph): build_fulltext_query helper for entity-name Lucene lookup"
```

---

### Task 2: `ensure_entity_fulltext_index`

**Files:**
- Modify: `src/graph/index.py`
- Test: `tests/test_graph/test_index.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_graph/test_index.py`:
```python
def test_ensure_entity_fulltext_index_idempotent_cypher_and_failopen():
    from src.graph.index import (
        ENTITY_FULLTEXT_INDEX_CYPHER,
        ensure_entity_fulltext_index,
    )

    # Idempotent DDL on the entity name.
    assert "CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS" in (
        ENTITY_FULLTEXT_INDEX_CYPHER
    )
    assert "ON EACH [e.name]" in ENTITY_FULLTEXT_INDEX_CYPHER

    class _Store:
        def __init__(self):
            self.ran = None

        def structured_query(self, cypher):
            self.ran = cypher

    store = _Store()
    assert ensure_entity_fulltext_index(store) is True
    assert store.ran == ENTITY_FULLTEXT_INDEX_CYPHER

    class _BoomStore:
        def structured_query(self, cypher):
            raise RuntimeError("no fulltext support")

    # Fail-open: returns False, never raises.
    assert ensure_entity_fulltext_index(_BoomStore()) is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_index.py::test_ensure_entity_fulltext_index_idempotent_cypher_and_failopen -v`
Expected: FAIL — `ImportError` (names not defined).

- [ ] **Step 3: Implement**

In `src/graph/index.py`, add near the top (after imports; the module already imports `loguru.logger` — if not, add `from loguru import logger`):
```python
# Full-text index over entity names — backs partial-name lookup
# (``GraphRetriever.afind_entities_by_name``).  Idempotent DDL, safe to
# run repeatedly / concurrently.
ENTITY_FULLTEXT_INDEX_CYPHER = (
    "CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS "
    "FOR (e:__Entity__) ON EACH [e.name]"
)


def ensure_entity_fulltext_index(store) -> bool:
    """Idempotently create the entity-name full-text index.

    Returns True on success, False (logged) on any error — never raises,
    so callers on the ingest / retriever-bootstrap paths stay fail-open
    (a store without full-text support just keeps the old behaviour)."""
    try:
        store.structured_query(ENTITY_FULLTEXT_INDEX_CYPHER)
        return True
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.warning("ensure_entity_fulltext_index failed: {e}", e=exc)
        return False
```
(If `src/graph/index.py` does not already import `logger`, add `from loguru import logger` to its imports.)

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_index.py -q`
Expected: PASS (new test + existing).

- [ ] **Step 5: Commit**

```bash
git add src/graph/index.py tests/test_graph/test_index.py
git commit -m "feat(graph): ensure_entity_fulltext_index (idempotent, fail-open)"
```

---

### Task 3: `GraphRetriever.afind_entities_by_name`

**Files:**
- Modify: `src/graph/retriever.py`
- Test: `tests/test_graph/test_retriever_fulltext.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_graph/test_retriever_fulltext.py`:
```python
import pytest


class _StubStore:
    def __init__(self, rows=None, raise_=False):
        self._rows = rows or []
        self._raise = raise_
        self.last = None

    def structured_query(self, cypher, params):
        self.last = (cypher, params)
        if self._raise:
            raise RuntimeError("no fulltext index")
        return self._rows


def _retriever(store):
    from src.graph.retriever import GraphRetriever
    r = GraphRetriever.__new__(GraphRetriever)  # bypass LlamaIndex wiring
    r._graph_store = store
    r._similarity_top_k = 10
    return r


@pytest.mark.asyncio
async def test_afind_entities_by_name_maps_rows():
    store = _StubStore(rows=[
        {"name": "Иванов Иван Иванович", "labels": ["Person"], "description": "д."},
        {"name": "", "labels": [], "description": ""},  # skipped (no name)
    ])
    data = await _retriever(store).afind_entities_by_name("Иванов", limit=5)
    assert [e["entity_name"] for e in data.entities] == ["Иванов Иван Иванович"]
    assert data.entities[0]["entity_type"] == "Person"
    # passed the Lucene query + limit
    assert store.last[1] == {"lucene": "Иванов", "limit": 5}


@pytest.mark.asyncio
async def test_afind_entities_by_name_blank_and_failopen():
    # blank query → no store call, empty result
    store = _StubStore(rows=[{"name": "X"}])
    assert (await _retriever(store).afind_entities_by_name("   ")).entities == []
    assert store.last is None
    # store error → empty (fail-open)
    boom = _StubStore(raise_=True)
    assert (await _retriever(boom).afind_entities_by_name("Иванов")).entities == []
    # no store → empty
    assert (await _retriever(None).afind_entities_by_name("Иванов")).entities == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_graph/test_retriever_fulltext.py -v`
Expected: FAIL — `AttributeError: ... has no attribute 'afind_entities_by_name'`.

- [ ] **Step 3: Implement**

In `src/graph/retriever.py`:

(a) store `similarity_top_k` in `__init__` — change the body to keep it. The current `__init__` signature is `(self, pg_index, *, similarity_top_k=10, path_depth=1, include_text=True)`. After building `self._retriever = pg_index.as_retriever(...)`, add:
```python
        self._similarity_top_k = similarity_top_k
```

(b) add the find-by-name Cypher constant near `_WALK_CYPHER`:
```python
_FIND_BY_NAME_CYPHER = """
CALL db.index.fulltext.queryNodes('entity_name_fulltext', $lucene)
YIELD node, score
WHERE node:`__Entity__`
RETURN node.name AS name,
       [l IN labels(node) WHERE l <> '__Entity__' AND l <> '__Node__'] AS labels,
       coalesce(node.description, '') AS description
ORDER BY score DESC
LIMIT $limit
"""
```

(c) add the method to `GraphRetriever` (after `awalk`):
```python
    async def afind_entities_by_name(
        self, query: str, *, limit: int | None = None,
    ) -> RoundGraphData:
        """Full-text lookup of entities by (partial) name.

        Complements ``aretrieve`` (exact-synonym + vector): catches
        "Иванов" → "Иванов Иван Иванович" on large graphs via the
        ``entity_name_fulltext`` index.  Best-effort — empty on a missing
        store / missing index / any error / blank query (never raises)."""
        if self._graph_store is None:
            return RoundGraphData()
        lucene = build_fulltext_query(query)
        if not lucene:
            return RoundGraphData()
        cap = limit if limit is not None else self._similarity_top_k
        try:
            rows = await asyncio.to_thread(
                self._graph_store.structured_query,
                _FIND_BY_NAME_CYPHER,
                {"lucene": lucene, "limit": int(cap)},
            )
        except Exception as exc:  # noqa: BLE001 — index/store missing
            logger.warning("find_entities_by_name failed: {e}", e=exc)
            return RoundGraphData()
        out = RoundGraphData()
        for row in rows or []:
            name = (row or {}).get("name")
            if not name:
                continue
            labels = list(row.get("labels") or [])
            out.entities.append({
                "entity_name": name,
                "entity_type": labels[0] if labels else "",
                "description": row.get("description") or "",
            })
        out.entities = _dedupe_entities(out.entities)
        return out
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_graph/test_retriever_fulltext.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/retriever.py tests/test_graph/test_retriever_fulltext.py
git commit -m "feat(graph): GraphRetriever.afind_entities_by_name (full-text entity lookup)"
```

---

### Task 4: `find_entity_by_name` atomic tool

**Files:**
- Modify: `src/retrieval/atomic_tools.py`
- Test: `tests/test_retrieval/test_find_entity_by_name.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_retrieval/test_find_entity_by_name.py`:
```python
"""find_entity_by_name atomic tool."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from src.retrieval.atomic_tools import find_entity_by_name


@dataclass
class _Data:
    entities: list = field(default_factory=list)


class _Retriever:
    def __init__(self, entities):
        self._entities = entities
        self.seen = None

    async def afind_entities_by_name(self, query, *, limit=None):
        self.seen = (query, limit)
        return _Data(entities=self._entities)


@pytest.mark.asyncio
async def test_find_entity_by_name_returns_entities():
    r = _Retriever([{"entity_name": "Иванов Иван Иванович", "entity_type": "Person"}])
    res = await find_entity_by_name(r, query="Иванов", limit=7)
    obs = json.loads(res.observation)
    assert obs["entities"][0]["entity_name"] == "Иванов Иван Иванович"
    assert r.seen == ("Иванов", 7)
    assert res.sources == []


@pytest.mark.asyncio
async def test_find_entity_by_name_none_retriever():
    res = await find_entity_by_name(None, query="Иванов")
    assert json.loads(res.observation) == {"entities": []}
    assert res.sources == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_retrieval/test_find_entity_by_name.py -q`
Expected: FAIL — `ImportError: cannot import name 'find_entity_by_name'`.

- [ ] **Step 3: Implement**

In `src/retrieval/atomic_tools.py`:

(a) extend `GraphRetrieverProtocol` (class at line ~43) with:
```python
    async def afind_entities_by_name(
        self, query: str, *, limit: int | None = None,
    ) -> Any: ...
```

(b) add the tool function (after `find_entity_by_id`):
```python
async def find_entity_by_name(
    graph_retriever: GraphRetrieverProtocol | None,
    *,
    query: str,
    limit: int = 10,
) -> ToolResult:
    """Find entities whose NAME matches the query via the full-text index.

    Partial-name tolerant ("Иванов" → "Иванов Иван Иванович") — complements
    the exact ``find_entity_by_id`` and the similarity ``graph_search`` on
    large graphs.  Returns entity rows only (no chunks); empty for a None
    retriever."""
    if graph_retriever is None:
        return ToolResult(sources=[], observation=json.dumps({"entities": []}))
    data = await graph_retriever.afind_entities_by_name(query, limit=limit)
    entities = getattr(data, "entities", []) or []
    return ToolResult(
        sources=[],
        observation=json.dumps({"entities": entities}, ensure_ascii=False),
    )
```

(c) register in `TOOL_FUNCTIONS` (add `"find_entity_by_name": find_entity_by_name,`).

(d) add a `dispatch` branch (alongside the other `graph_retriever` tools):
```python
    if tool_name == "find_entity_by_name":
        return await find_entity_by_name(graph_retriever, **tool_kwargs)
```

(e) add a `TOOL_DESCRIPTIONS` entry:
```python
    "find_entity_by_name": (
        "Find entities by (partial) NAME via full-text — tolerant of "
        "longer stored names («Иванов» → «Иванов Иван "
        "Иванович»). Use when you have a name/surname and exact lookup "
        "misses."
    ),
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_retrieval/test_find_entity_by_name.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/atomic_tools.py tests/test_retrieval/test_find_entity_by_name.py
git commit -m "feat(retrieval): find_entity_by_name atomic tool (full-text entity lookup)"
```

---

### Task 5: wire `find_entity_by_name` into the retrieve pipeline

**Files:**
- Modify: `src/workflow/search/activities/retrieve.py`
- Test: `tests/test_workflow/test_search_retrieve.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_workflow/test_search_retrieve.py` (it already imports the retrieve module; match its import style):
```python
def test_pipeline_includes_find_entity_by_name():
    from src.workflow.search.activities.retrieve import _PIPELINE, ALLOWED_TOOLS

    assert "find_entity_by_name" in _PIPELINE
    assert _PIPELINE.index("find_entity_by_name") > _PIPELINE.index("graph_search")
    assert "find_entity_by_name" in ALLOWED_TOOLS
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_workflow/test_search_retrieve.py::test_pipeline_includes_find_entity_by_name -q`
Expected: FAIL — `find_entity_by_name` not in `_PIPELINE`.

- [ ] **Step 3: Implement**

In `src/workflow/search/activities/retrieve.py`:

(a) extend the constants:
```python
_PIPELINE = ("vector_search", "graph_search", "find_entity_by_name")
```
```python
ALLOWED_TOOLS = (
    "vector_search", "graph_search", "find_entity_by_name", "graph_walk",
)
```

(b) capture the find-by-name observation in the pipeline loop. Next to the existing `graph_search_obs: str | None = None`, add:
```python
    find_name_obs: str | None = None
```
and inside the `for tool_name in _PIPELINE:` loop, after the existing `if tool_name == "graph_search": graph_search_obs = result.observation`, add:
```python
        if tool_name == "find_entity_by_name":
            find_name_obs = result.observation
```

(c) make the R3b graph_walk seed fall back to the name hit. Replace the seed guard so it uses the graph_search entity first, then the full-text hit:
```python
    seed_obs = graph_search_obs if graph_search_obs is not None else find_name_obs
    if settings.agent.graph_walk_enabled and seed_obs is not None:
        try:
            start = top_entity_name(graph_search_obs or "") \
                or top_entity_name(find_name_obs or "")
            if start:
                walk = await atomic_tools.dispatch(
                    "graph_walk",
                    {
                        "start_entity": start,
                        "hops": settings.agent.graph_walk_hops,
                    },
                    graph_retriever=graph_retriever,
                )
                _merge_sources(walk.sources)
        except Exception as exc:
            activity.logger.warning(
                "retrieve_subquestion  graph_walk skipped  err=%s", exc,
            )
            errors.append(f"graph_walk: {exc}")
```
(`top_entity_name("")` returns `None` because `json.loads("")` raises — handled by its existing try/except.)

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_workflow/test_search_retrieve.py -q`
Expected: PASS (new + existing, incl. the existing `top_entity_name` tests).

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/activities/retrieve.py tests/test_workflow/test_search_retrieve.py
git commit -m "feat(search): run find_entity_by_name in retrieve pipeline + seed graph_walk from name hit"
```

---

### Task 6: config `graph_similarity_top_k` + ensure-index wiring

**Files:**
- Modify: `src/config.py`, `src/workflow/_search_deps.py`, `src/workflow/activities/build_property_graph.py`
- Test: `tests/test_config/test_settings.py` (extend)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_config/test_settings.py`:
```python
def test_agent_graph_similarity_top_k_default():
    from src.config import settings
    assert settings.agent.graph_similarity_top_k == 20
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_config/test_settings.py::test_agent_graph_similarity_top_k_default -q`
Expected: FAIL — `AttributeError: ... has no attribute 'graph_similarity_top_k'`.

- [ ] **Step 3: Implement**

(a) `src/config.py` — in `AgentSettings`, add (near `graph_walk_hops`):
```python
    # Graph retriever candidate count (VectorContextRetriever top_k). Raised
    # from the LlamaIndex default so a named entity isn't ranked out of the
    # result set on a large graph.
    graph_similarity_top_k: int = Field(default=20, ge=1, le=100)
```

(b) `src/workflow/_search_deps.py` — in `_build_graph_retriever_once`, ensure the index and pass the configured top_k. Replace the `gs = build_neo4j_graph_store()` … `return GraphRetriever(pg)` block with:
```python
        from src.config import settings
        from src.graph.index import (
            build_kg_extractor, build_property_graph_index,
            ensure_entity_fulltext_index,
        )
        from src.graph.retriever import GraphRetriever
        from src.graph.store import build_neo4j_graph_store
        gs = build_neo4j_graph_store()
        pg = build_property_graph_index(
            graph_store=gs, embed_model=embed_model,
            extractor=build_kg_extractor(llm), nodes=None,
        )
        # Existing-graph coverage: create the entity-name full-text index
        # once at bootstrap (idempotent, fail-open).
        ensure_entity_fulltext_index(gs)
        return GraphRetriever(
            pg, similarity_top_k=settings.agent.graph_similarity_top_k,
        )
```
(Keep the existing `except Exception` fail-open wrapper around it unchanged.)

(c) `src/workflow/activities/build_property_graph.py` — after the relations upsert (the `graph_store.upsert_relations(relations)` + heartbeat, ~line 87), add (ingest coverage):
```python
        from src.graph.index import ensure_entity_fulltext_index
        ensure_entity_fulltext_index(graph_store)
        activity.heartbeat({"stage": "fulltext_index_ensured"})
```

- [ ] **Step 4: Run to verify it passes**

Run:
```
uv run pytest tests/test_config/test_settings.py -q
uv run python -c "import src.workflow.worker, src.api.main; print('imports ok')"
```
Expected: config test PASS; `imports ok`.

- [ ] **Step 5: Commit**

```bash
git add src/config.py src/workflow/_search_deps.py src/workflow/activities/build_property_graph.py tests/test_config/test_settings.py
git commit -m "feat(search): graph_similarity_top_k=20 + ensure entity full-text index (ingest + bootstrap)"
```

---

## Final verification

- [ ] **Run touched suites + import smoke**

```bash
uv run pytest tests/test_graph tests/test_retrieval/test_find_entity_by_name.py tests/test_workflow/test_search_retrieve.py tests/test_config -q
uv run python -c "import src.workflow.worker, src.api.main; print('imports ok')"
```
Expected: all pass; `imports ok`.

- [ ] **Manual smoke (needs live Neo4j + worker/API restart)**

After redeploy (the index is created on first graph-retriever build), search for a known surname via `/api/v1/search/local` and confirm the full entity now surfaces in the answer/sources.

> **Restart required** for the new pipeline + config; the full-text index is created idempotently at retriever bootstrap (existing graphs) and on the next ingest — no manual migration.

---

## Self-Review

**Spec coverage:** full-text index (Task 2 + Task 6 wiring) ↔ spec §1; `afind_entities_by_name` + `build_fulltext_query` (Tasks 1, 3) ↔ §2; `find_entity_by_name` tool + pipeline wiring (Tasks 4, 5) ↔ §3; `graph_similarity_top_k` (Task 6) ↔ §4; fail-open everywhere ↔ spec "Error handling"; tests per spec "Testing". Out-of-scope items (fuzzy/alias/hybrid) intentionally absent.

**Placeholder scan:** every code step is complete; the only non-literal note is the spec-sanctioned "verify against live Neo4j" smoke step. No TBDs.

**Type consistency:** `build_fulltext_query(str)->str`; `GraphRetriever.afind_entities_by_name(query, *, limit=None)->RoundGraphData` matches the protocol method and the tool's call; `find_entity_by_name(graph_retriever, *, query, limit=10)->ToolResult`; `ensure_entity_fulltext_index(store)->bool`; `settings.agent.graph_similarity_top_k`. `_retriever._similarity_top_k` (set in Task 3 `__init__`) is read by `afind_entities_by_name` and the test stubs it. Pipeline constant `_PIPELINE`/`ALLOWED_TOOLS` names match the tool registry key `find_entity_by_name`.

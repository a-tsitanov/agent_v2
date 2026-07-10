# report_vec → Milvus (semantic slice) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Community-report vectors live in Milvus (`community_report_vec`) and the semantic community-select reads from Milvus, backend-dispatched. Default neo4j semantic-select is byte-for-byte unchanged; Milvus under `nebula` or opt-in `AGENT_COMMUNITY_VECTOR_BACKEND=milvus`. `descent` mode deferred.

**Architecture:** A `CommunityReportVectorStore` seam (`knn`/`upsert`), Neo4j impl wrapping the existing `_SELECT_SEMANTIC_CYPHER` verbatim + Milvus impl (collection `community_report_vec`). `summarize_community_activity` upserts report_vec through the store; `select_communities_semantic` reads through it. Directly mirrors the merged er_vec slice (`src/graph/entity_vector_store*.py`).

**Tech Stack:** Python/FastAPI, `pymilvus.MilvusClient`, `MilvusSettings` (dim=1536, HNSW, COSINE), pytest.

## Global Constraints

- **Default neo4j semantic-select unchanged.** `GRAPH_BACKEND=neo4j` + `community_vector_backend="native"` → `select_communities_semantic` and `_WRITE_REPORT_CYPHER` behave exactly as today.
- `knn(query_vec, *, level, limit) -> list[CommunityRef]` where `CommunityRef = {community_id, level, summary}` (summary from the store; semantic-select needs no graph read for it).
- Milvus reached only under nebula or opt-in. Unit tests DB-free. Local commits only. Never stage `docs/bruno/collection.bru`. `MilvusClient(uri=settings.milvus.uri, timeout=settings.milvus.timeout_s)`; collection separate from chunk + entity collections.

## File Structure

- Create `src/graph/community_vector_store.py`, `src/graph/community_vector_store_milvus.py`.
- Modify `src/config.py` (`community_vector_backend`), `scripts/make_env.py`.
- Modify `src/workflow/search/activities/community.py` (summarize upsert), `src/workflow/search/activities/global_search.py` (select_communities_semantic).
- Create `scripts/backfill_report_vec_milvus.py`.
- Tests: `tests/test_graph/test_community_vector_store.py`, `tests/test_graph/test_community_vector_store_milvus.py`, extend the relevant search-activity test.

---

### Task 1: seam + Neo4j impl + factory + config

**Files:** Create `src/graph/community_vector_store.py`; modify `src/config.py`, `scripts/make_env.py`; test `tests/test_graph/test_community_vector_store.py`.

**Interfaces produced:**
- `CommunityRef` TypedDict `{community_id: str, level: int, summary: str}`; `CommunityReport` TypedDict `{community_id, level, summary, embedding: list[float]}`.
- `CommunityReportVectorStore` Protocol: `knn(query_vec, *, level, limit) -> list[CommunityRef]`, `upsert(reports) -> None`.
- `Neo4jCommunityReportVectorStore(graph_store)`: `knn` wraps `_SELECT_SEMANTIC_CYPHER`; `upsert` no-op.
- `build_community_report_vector_store(graph_store)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_community_vector_store.py
"""CommunityReportVectorStore: Neo4j impl query/mapping + factory dispatch."""
from __future__ import annotations

import src.graph.community_vector_store as cvs


class _FakeGraphStore:
    def __init__(self, rows):
        self._rows = rows; self.last_query = None; self.last_params = None
    def structured_query(self, query, param_map=None):
        self.last_query = query; self.last_params = param_map or {}
        return self._rows


def test_neo4j_knn_maps_rows():
    rows = [{"community_id": "c1", "level": 0, "summary": "s1"},
            {"community_id": "c2", "level": 0, "summary": "  "}]  # blank skipped
    store = cvs.Neo4jCommunityReportVectorStore(_FakeGraphStore(rows))
    out = store.knn([0.1, 0.2], level=0, limit=5)
    assert "db.index.vector.queryNodes('community_report_vec'" in store._graph_store.last_query
    assert store._graph_store.last_params == {"vec": [0.1, 0.2], "level": 0, "limit": 5}
    assert out == [{"community_id": "c1", "level": 0, "summary": "s1"}]


def test_neo4j_upsert_is_noop():
    store = cvs.Neo4jCommunityReportVectorStore(_FakeGraphStore([]))
    store.upsert([{"community_id": "c1", "level": 0, "summary": "s", "embedding": [0.1]}])
    assert store._graph_store.last_query is None


def test_factory_dispatches(monkeypatch):
    monkeypatch.setattr(cvs.settings.graph, "backend", "neo4j", raising=False)
    monkeypatch.setattr(cvs.settings.agent, "community_vector_backend", "native", raising=False)
    assert isinstance(cvs.build_community_report_vector_store(_FakeGraphStore([])),
                      cvs.Neo4jCommunityReportVectorStore)
    # nebula -> milvus impl (patched to a sentinel; real ctor connects a MilvusClient)
    import src.graph.community_vector_store_milvus as cvsm
    sentinel = object()
    monkeypatch.setattr(cvsm, "MilvusCommunityReportVectorStore", lambda *a, **k: sentinel)
    monkeypatch.setattr(cvs.settings.graph, "backend", "nebula", raising=False)
    assert cvs.build_community_report_vector_store(_FakeGraphStore([])) is sentinel
```

(Note: the nebula-dispatch assertion imports the milvus module created in Task 2; if you run Step 2 before Task 2 exists, that import raises `ModuleNotFoundError` — write the test as shown; it goes green once Task 2 lands. Split: if the reviewer wants RED first, temporarily assert `pytest.raises(ModuleNotFoundError)` and switch to the sentinel form in Task 2, exactly as the er_vec slice did. Simpler: implement Task 1 to make the neo4j asserts pass and leave the nebula sentinel assert for Task 2's commit.)

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_vector_store.py -q`
Expected: FAIL (`No module named 'src.graph.community_vector_store'`).

- [ ] **Step 3: Add config**

`src/config.py` `AgentSettings`, next to `er_vector_backend`:
```python
    # Where community-report vectors live for semantic community-select.
    # "native" = Neo4j in-graph community_report_vec index (prod path,
    # unchanged); "milvus" = community_report_vec Milvus collection (opt-in;
    # FORCED under GRAPH_BACKEND=nebula). Dispatched in
    # src/graph/community_vector_store.py.
    community_vector_backend: Literal["native", "milvus"] = "native"
```
`scripts/make_env.py::_ENV_DESCRIPTIONS`:
```python
    "AGENT_COMMUNITY_VECTOR_BACKEND": "Где живут community-report вектора для semantic community-select: 'native' (Neo4j in-graph index) или 'milvus' (коллекция community_report_vec). Под GRAPH_BACKEND=nebula форсится 'milvus'. Дефолт 'native'.",
```

- [ ] **Step 4: Create `community_vector_store.py`**

```python
# src/graph/community_vector_store.py
"""Vector store for semantic community-report selection — backend-dispatched.

Neo4j serves the kNN from the in-graph `community_report_vec` index;
NebulaGraph has no such index, so it goes to a Milvus collection. Mirrors
src/graph/entity_vector_store.py (the merged er_vec slice)."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, runtime_checkable

# The exact query the current select_communities_semantic issues.
_SELECT_SEMANTIC_CYPHER = """
CALL db.index.vector.queryNodes('community_report_vec', $limit, $vec) YIELD node, score
WHERE node.level = $level AND node.summary IS NOT NULL AND trim(node.summary) <> ''
RETURN node.id AS community_id, node.level AS level, node.summary AS summary
ORDER BY score DESC
"""

from src.config import settings


class CommunityRef(TypedDict):
    community_id: str
    level: int
    summary: str


class CommunityReport(TypedDict):
    community_id: str
    level: int
    summary: str
    embedding: list[float]


@runtime_checkable
class CommunityReportVectorStore(Protocol):
    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]: ...
    def upsert(self, reports: list[CommunityReport]) -> None: ...


class Neo4jCommunityReportVectorStore:
    def __init__(self, graph_store: Any):
        self._graph_store = graph_store

    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]:
        rows = self._graph_store.structured_query(
            _SELECT_SEMANTIC_CYPHER,
            {"vec": list(query_vec), "level": int(level), "limit": max(0, int(limit))},
        )
        out: list[CommunityRef] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            cid = row.get("community_id")
            summary = (row.get("summary") or "").strip()
            if cid is None or not summary:
                continue
            out.append({"community_id": str(cid),
                        "level": int(row.get("level") or 0), "summary": summary})
        return out[: max(0, int(limit))]

    def upsert(self, reports: list[CommunityReport]) -> None:
        # No-op: report_vec is persisted on the :Community node by
        # community._WRITE_REPORT_CYPHER (unchanged neo4j write path).
        return None


def build_community_report_vector_store(graph_store: Any) -> CommunityReportVectorStore:
    use_milvus = (
        settings.graph.backend == "nebula"
        or settings.agent.community_vector_backend == "milvus"
    )
    if use_milvus:
        from src.graph.community_vector_store_milvus import MilvusCommunityReportVectorStore

        return MilvusCommunityReportVectorStore()
    return Neo4jCommunityReportVectorStore(graph_store)
```

- [ ] **Step 5: Run tests + ruff**

`API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_vector_store.py tests/test_scripts/test_make_env.py -q` (the nebula-sentinel assert needs Task 2; if running Task 1 alone, expect that one assert to error on the missing milvus module — land it green with Task 2, or temporarily use `pytest.raises(ModuleNotFoundError)`). `.venv/bin/python -m ruff check src/graph/community_vector_store.py src/config.py` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/graph/community_vector_store.py src/config.py scripts/make_env.py tests/test_graph/test_community_vector_store.py
git commit -m "feat(community): CommunityReportVectorStore seam + Neo4j impl + dispatch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `MilvusCommunityReportVectorStore` (`community_report_vec`)

**Files:** Create `src/graph/community_vector_store_milvus.py`; test `tests/test_graph/test_community_vector_store_milvus.py`. If Task 1's factory test used the sentinel form, this task makes it pass; reconcile like the er_vec slice.

**Interfaces:** `MilvusCommunityReportVectorStore(client=None, collection="community_report_vec")`: PK `pk` VARCHAR = `f"{community_id}:{level}"`, vector `report_vec` (dim=`settings.milvus.dim`), scalars `community_id` VARCHAR / `level` INT64 / `summary` VARCHAR. `upsert` → `client.upsert`; `knn(query_vec, *, level, limit)` → `client.search(filter=f"level == {int(level)}", output_fields=["community_id","level","summary"])` → `CommunityRef`.

- [ ] **Step 1: Write the failing test (fake MilvusClient)**

```python
# tests/test_graph/test_community_vector_store_milvus.py
from __future__ import annotations
from src.graph.community_vector_store_milvus import MilvusCommunityReportVectorStore


class _FakeClient:
    def __init__(self, search_result=None):
        self.upserts = []; self.searches = []; self._search_result = search_result or []; self._c = []
    def has_collection(self, name): return name in self._c
    def create_collection(self, **kw): self._c.append(kw.get("collection_name"))
    def create_schema(self, **kw): return _S()
    def prepare_index_params(self, **kw): return _I()
    def upsert(self, collection_name, data): self.upserts.append((collection_name, data))
    def search(self, **kw): self.searches.append(kw); return self._search_result
class _S:
    def add_field(self, **kw): return self
class _I:
    def add_index(self, **kw): return self


def _store(c):
    s = MilvusCommunityReportVectorStore.__new__(MilvusCommunityReportVectorStore)
    s._client = c; s._collection = "community_report_vec"; s._ensured = True
    return s


def test_upsert_rows():
    c = _FakeClient()
    _store(c).upsert([{"community_id": "c1", "level": 2, "summary": "s", "embedding": [0.1, 0.2]}])
    coll, data = c.upserts[0]
    assert coll == "community_report_vec"
    assert data[0]["pk"] == "c1:2" and data[0]["report_vec"] == [0.1, 0.2]
    assert data[0]["community_id"] == "c1" and data[0]["level"] == 2 and data[0]["summary"] == "s"


def test_knn_filters_level_and_maps():
    hit = {"entity": {"community_id": "c1", "level": 0, "summary": "s1"}}
    c = _FakeClient(search_result=[[hit]])
    out = _store(c).knn([0.0, 0.0], level=0, limit=5)
    assert c.searches[0]["filter"] == "level == 0" and c.searches[0]["limit"] == 5
    assert out == [{"community_id": "c1", "level": 0, "summary": "s1"}]
```

- [ ] **Step 2: RED** — `pytest tests/test_graph/test_community_vector_store_milvus.py -q` → module missing.

- [ ] **Step 3: Implement** (mirror `MilvusEntityVectorStore`)

```python
# src/graph/community_vector_store_milvus.py
"""Milvus-backed CommunityReportVectorStore (collection community_report_vec)."""
from __future__ import annotations

from typing import Any

from loguru import logger

from src.config import settings
from src.graph.community_vector_store import CommunityRef, CommunityReport

_COLLECTION = "community_report_vec"
_PK_MAX, _CID_MAX, _SUM_MAX = 256, 128, 8192


class MilvusCommunityReportVectorStore:
    def __init__(self, client: Any | None = None, collection: str = _COLLECTION):
        from pymilvus import MilvusClient
        self._client = client or MilvusClient(uri=settings.milvus.uri, timeout=settings.milvus.timeout_s)
        self._collection = collection
        self._ensured = False

    def _ensure(self) -> None:
        if self._ensured:
            return
        try:
            if not self._client.has_collection(self._collection):
                from pymilvus import DataType
                schema = self._client.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=_PK_MAX)
                schema.add_field("report_vec", DataType.FLOAT_VECTOR, dim=settings.milvus.dim)
                schema.add_field("community_id", DataType.VARCHAR, max_length=_CID_MAX)
                schema.add_field("level", DataType.INT64)
                schema.add_field("summary", DataType.VARCHAR, max_length=_SUM_MAX)
                index = self._client.prepare_index_params()
                index.add_index(field_name="report_vec", index_type=settings.milvus.index_type,
                                metric_type="COSINE",
                                params={"M": settings.milvus.hnsw_m,
                                        "efConstruction": settings.milvus.hnsw_ef_construction})
                self._client.create_collection(collection_name=self._collection, schema=schema, index_params=index)
            self._ensured = True
        except Exception as exc:
            logger.warning("ensure community_report_vec collection failed: {e}", e=exc)

    def upsert(self, reports: list[CommunityReport]) -> None:
        if not reports:
            return
        self._ensure()
        data = [{
            "pk": f"{r['community_id']}:{int(r['level'])}"[:_PK_MAX],
            "report_vec": list(r["embedding"]),
            "community_id": str(r["community_id"])[:_CID_MAX],
            "level": int(r["level"]),
            "summary": (r.get("summary") or "")[:_SUM_MAX],
        } for r in reports if r.get("embedding")]
        if data:
            self._client.upsert(collection_name=self._collection, data=data)

    def knn(self, query_vec: list[float], *, level: int, limit: int) -> list[CommunityRef]:
        self._ensure()
        try:
            res = self._client.search(
                collection_name=self._collection, data=[list(query_vec)],
                anns_field="report_vec", limit=max(0, int(limit)),
                filter=f"level == {int(level)}",
                output_fields=["community_id", "level", "summary"],
                search_params={"metric_type": "COSINE", "params": {"ef": settings.milvus.hnsw_ef_search}},
            )
        except Exception as exc:
            logger.warning("community_report_vec knn failed: {e}", e=exc)
            return []
        out: list[CommunityRef] = []
        for hits in (res or []):
            for h in hits:
                e = h.get("entity", h) if isinstance(h, dict) else h
                cid = e.get("community_id")
                summary = (e.get("summary") or "").strip()
                if cid is None or not summary:
                    continue
                out.append({"community_id": str(cid), "level": int(e.get("level") or 0), "summary": summary})
        return out
```

- [ ] **Step 4:** GREEN + ruff. Reconcile Task 1's factory test (sentinel form) if needed.
- [ ] **Step 5:** Commit `feat(community): MilvusCommunityReportVectorStore (community_report_vec)`.

**Live-verify (controller):** create collection, upsert 2 fixture reports at 2 levels, `knn` with a level filter, confirm the level filter works and summary/id round-trip.

---

### Task 3: write + read wiring

**Files:** Modify `community.py` (summarize upsert), `global_search.py` (select_communities_semantic). Test: extend a search-activity test (DB-free fake store).

- [ ] **Step 1: Write failing tests** — (a) `select_communities_semantic` routes through `build_community_report_vector_store(store).knn(...)` and maps to `CommunitySummaryRef` (fake report store returns `CommunityRef`s); (b) `summarize_community_activity` calls `report_store.upsert` with `{community_id, level, summary, embedding}` when report_vec is present (fake store records the upsert). Read the current test files for both activities first and mirror their fake-store patterns.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement**
  - `global_search.py::select_communities_semantic`: build `report_store = build_community_report_vector_store(store)` and replace the `store.structured_query(_SELECT_SEMANTIC_CYPHER, ...)` call with `await asyncio.to_thread(report_store.knn, query_vec, ...)` (pass `level=`/`limit=` as kwargs), then map the returned `CommunityRef`s to `CommunitySummaryRef` (reuse the existing skip-blank/cap logic). Neo4j impl → identical behavior. Keep the whole function fail-open (return `[]` on error).
  - `community.py::summarize_community_activity`: after `report_vec = await _embed_report(...)` and the non-empty-summary check, add (before or after the existing `_WRITE_REPORT_CYPHER` write, fail-open):
    ```python
    if report_vec is not None:
        from src.graph.community_vector_store import build_community_report_vector_store
        try:
            rs = build_community_report_vector_store(store)
            await asyncio.to_thread(rs.upsert, [{
                "community_id": params.community_id, "level": params.level,
                "summary": summary, "embedding": report_vec,
            }])
        except Exception as exc:
            activity.logger.warning("community report vec upsert err=%s", exc)
    ```
    Leave `_WRITE_REPORT_CYPHER` UNCHANGED (neo4j node write; no-op upsert for the Neo4j store).

- [ ] **Step 4:** GREEN (new tests + existing community/global_search tests unchanged — default neo4j path identical) + ruff.
- [ ] **Step 5:** Commit `feat(community): route report_vec upsert + semantic-select through the store seam`.

---

### Task 4: backfill script

**Files:** Create `scripts/backfill_report_vec_milvus.py`.

- [ ] **Step 1:** Write it (mirror `scripts/backfill_er_vec_milvus.py`):
  - Read `MATCH (c:Community) WHERE c.report_vec IS NOT NULL AND c.summary IS NOT NULL AND trim(c.summary) <> '' RETURN c.id, c.level, c.summary, c.report_vec` from `build_neo4j_graph_store()`.
  - Build `CommunityReport` dicts; `MilvusCommunityReportVectorStore().upsert(...)` in batches. Dry-run default (`--no-dry-run` to write). DB access only inside `main()`.
- [ ] **Step 2:** Static-validate (`py_compile` + import-clean, no DB at import).
- [ ] **Step 3:** Commit `test(community): report_vec Milvus backfill script`.

**Live gate (controller):** run the Milvus community store live (upsert 2 reports at different levels, knn with level filter), and optionally backfill `--no-dry-run` if a Neo4j community set exists.

---

## Self-Review

**Spec coverage:** seam+Neo4j+factory+config (T1), Milvus impl (T2), write+read wiring (T3), backfill (T4). Default neo4j semantic-select unchanged (Neo4j impl wraps the same cypher; native config → Neo4j store). descent + nebula community-build + index removal deferred per spec.

**Placeholder scan:** exact code from the read source (`_SELECT_SEMANTIC_CYPHER`, `select_communities_semantic` mapping, `_WRITE_REPORT_CYPHER`, `summarize` structure). T3 says "read the current test files and mirror their fake-store patterns" — a real instruction (the implementer reads them), not a TBD.

**Type consistency:** `CommunityRef` keys identical across protocol, both impls, `select_communities_semantic` mapping, backfill, and tests. `knn` signature `(query_vec, *, level, limit)` consistent. Factory (T1) → Milvus impl (T2) lazy import defined in 1, satisfied in 2 (sentinel-reconcile like er_vec). Milvus PK `f"{id}:{level}"`; `level` filter is `int(level)`.

**Live-verified assumptions to close during execution:** pymilvus `search(filter="level == N", output_fields=[...])` shape and the `has_collection`/schema DDL API against the installed pymilvus (same as the er_vec slice — controller will live-verify).

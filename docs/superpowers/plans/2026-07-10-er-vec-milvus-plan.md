# ER-vec → Milvus (Phase 3 er_vec slice) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ER's candidate-kNN works via **Milvus** under `GRAPH_BACKEND=nebula` (no in-graph vector index), while the **live Neo4j ER path stays byte-for-byte unchanged** by default. Milvus is opt-in on neo4j for the parity benchmark.

**Architecture:** Introduce an `EntityVectorStore` seam (`knn`/`upsert`) with a Neo4j impl (wraps the existing `db.index.vector.queryNodes('er_embedding_vec')` path, extracted verbatim) and a Milvus impl (direct `pymilvus.MilvusClient`, collection `entity_er_vec`). `resolve_entities` consumes the store; a factory dispatches on `settings.graph.backend` / an opt-in flag. Candidate `knn` results carry embeddings (ER's `_candidate_pairs` cosines candidates).

**Tech Stack:** Python/FastAPI, `pymilvus.MilvusClient` (already a dep, used in `chunk_repository.py`), `MilvusSettings` (dim=1536, HNSW, COSINE), LlamaIndex ER (`entity_resolution.py`), pytest.

## Global Constraints

- **Default neo4j ER path unchanged.** With `GRAPH_BACKEND=neo4j` and `AGENT_ER_VECTOR_BACKEND=native` (default), ER is byte-for-byte today's behavior. Milvus is reached only under `nebula` or the explicit opt-in.
- `knn` results MUST include each candidate's `embedding` and `label` (`_candidate_pairs` cosines all items — `entity_resolution.py:409,467`; `_Item.label` is set from candidate labels).
- Only **canonical** entities live in `entity_er_vec` (`upsert` is called with resolved canonicals; backfill filters `er_canonical_name IS NOT NULL`).
- `er_store` (existing `resolve_entities` param) is the **verdict cache**, unrelated — add a SEPARATE `vector_store` param.
- Unit tests DB-free (fake store / fake `MilvusClient`). Local commits only (no push). Never stage `docs/bruno/collection.bru`.
- Milvus client: `MilvusClient(uri=settings.milvus.uri, timeout=settings.milvus.timeout_s)`, collection separate from the chunk collection.

## File Structure

- Create `src/graph/entity_vector_store.py` — `EntityCandidate`, `EntityVectorStore` protocol, `Neo4jEntityVectorStore`, `MilvusEntityVectorStore`, `build_entity_vector_store`.
- Modify `src/config.py` — `AgentSettings.er_vector_backend`.
- Modify `scripts/make_env.py` — env doc for `AGENT_ER_VECTOR_BACKEND`.
- Modify `src/graph/entity_resolution.py` — `resolve_entities` gains `vector_store`; `_load_candidates_via_store` helper.
- Modify `src/workflow/activities/merge_and_resolve.py` — build + pass the store.
- Create `scripts/backfill_er_vec_milvus.py`; extend `tests/eval/scale/bench_er_native.py`.
- Tests: `tests/test_graph/test_entity_vector_store.py` (new), extend `tests/test_graph/test_entity_resolution.py`.

---

### Task 1: `EntityVectorStore` seam + Neo4j impl + factory + config

**Files:**
- Create `src/graph/entity_vector_store.py`
- Modify `src/config.py` (`AgentSettings.er_vector_backend`), `scripts/make_env.py`
- Test `tests/test_graph/test_entity_vector_store.py`, extend `tests/test_scripts/test_make_env.py` implicitly (env-doc guard)

**Interfaces produced:**
- `EntityCandidate` TypedDict: `{name: str, label: str, embedding: list[float], mention_count: int, description: str}`.
- `class EntityVectorStore(Protocol)`: `knn(query_vec: list[float], k: int) -> list[EntityCandidate]`, `upsert(entities: list[EntityCandidate]) -> None`.
- `Neo4jEntityVectorStore(graph_store)`: `knn` issues the existing `db.index.vector.queryNodes('er_embedding_vec', $k, $vec)` (ensures index first); `upsert` is a no-op.
- `build_entity_vector_store(graph_store) -> EntityVectorStore`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_entity_vector_store.py
"""EntityVectorStore: Neo4j impl query/mapping + factory dispatch (DB-free)."""
from __future__ import annotations

import json

import src.graph.entity_vector_store as evs


class _FakeGraphStore:
    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_params = None
    def structured_query(self, query, param_map=None):
        self.last_query = query
        self.last_params = param_map or {}
        return self._rows


def test_neo4j_knn_maps_rows_with_embedding_and_label(monkeypatch):
    monkeypatch.setattr(evs, "ensure_er_vector_index", lambda *a, **k: True)
    rows = [
        {"name": "Иванов", "labels": ["__Entity__", "PERSON"], "er_vec": [0.1, 0.2],
         "er_embedding": None, "mention_count": 3, "description": "инженер"},
        {"name": "Ветеран", "labels": ["__Entity__", "PERSON"], "er_vec": None,
         "er_embedding": json.dumps([0.3, 0.4]), "mention_count": 1, "description": ""},
    ]
    store = evs.Neo4jEntityVectorStore(_FakeGraphStore(rows))
    out = store.knn([0.0, 0.0], 5)
    assert "db.index.vector.queryNodes('er_embedding_vec'" in store._graph_store.last_query
    assert store._graph_store.last_params == {"k": 5, "vec": [0.0, 0.0]}
    assert out[0] == {"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                      "mention_count": 3, "description": "инженер"}
    # legacy er_embedding JSON is parsed when er_vec is absent
    assert out[1]["embedding"] == [0.3, 0.4] and out[1]["label"] == "PERSON"


def test_neo4j_upsert_is_noop():
    store = evs.Neo4jEntityVectorStore(_FakeGraphStore([]))
    store.upsert([{"name": "x", "label": "T", "embedding": [0.1],
                   "mention_count": 1, "description": ""}])  # must not raise / query
    assert store._graph_store.last_query is None


def test_factory_dispatches_on_backend(monkeypatch):
    monkeypatch.setattr(evs.settings.graph, "backend", "neo4j", raising=False)
    monkeypatch.setattr(evs.settings.agent, "er_vector_backend", "native", raising=False)
    assert isinstance(evs.build_entity_vector_store(_FakeGraphStore([])), evs.Neo4jEntityVectorStore)

    monkeypatch.setattr(evs.settings.graph, "backend", "nebula", raising=False)
    import pytest
    with pytest.raises(ModuleNotFoundError):
        # Milvus impl arrives in Task 2; dispatch must ATTEMPT it under nebula.
        evs.build_entity_vector_store(_FakeGraphStore([]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_entity_vector_store.py -q`
Expected: FAIL (`No module named 'src.graph.entity_vector_store'`).

- [ ] **Step 3: Add `AgentSettings.er_vector_backend`**

In `src/config.py`, in `AgentSettings` next to `er_use_native_vector_knn` / `er_vector_knn_k` (config.py:639-642), add:

```python
    # Where the ER candidate-kNN vectors live. "native" = Neo4j in-graph
    # vector index (db.index.vector, unchanged prod path). "milvus" =
    # entity_er_vec Milvus collection (opt-in on neo4j for the parity
    # benchmark; FORCED under GRAPH_BACKEND=nebula, which has no in-graph
    # index). Dispatched in src/graph/entity_vector_store.py.
    er_vector_backend: Literal["native", "milvus"] = "native"
```

Add its env doc to `scripts/make_env.py::_ENV_DESCRIPTIONS` under a suitable group:

```python
    "AGENT_ER_VECTOR_BACKEND": "Где живут ER-kNN вектора: 'native' (Neo4j in-graph vector index, текущий прод-путь) или 'milvus' (коллекция entity_er_vec). Под GRAPH_BACKEND=nebula форсится 'milvus'. Дефолт 'native'.",
```

- [ ] **Step 4: Create `entity_vector_store.py`**

```python
# src/graph/entity_vector_store.py
"""Vector store for ER candidate-kNN — backend-dispatched.

The ER candidate lookup (`entity_resolution._load_candidates_via_store`)
finds the k nearest stored CANONICAL entities to each new entity. Neo4j
serves this from an in-graph vector index; NebulaGraph has no such index,
so it goes to a Milvus collection. Both impls return candidates WITH their
embeddings (ER's `_candidate_pairs` cosines every item).
"""

from __future__ import annotations

import json
from typing import Any, Protocol, TypedDict, runtime_checkable

from loguru import logger

from src.config import settings
from src.graph.index import ensure_er_vector_index

_NEO4J_ER_KNN_CYPHER = """
CALL db.index.vector.queryNodes('er_embedding_vec', $k, $vec)
YIELD node
WHERE node.er_canonical_name IS NOT NULL
RETURN node.name AS name,
       labels(node) AS labels,
       node.er_vec AS er_vec,
       node.er_embedding AS er_embedding,
       coalesce(node.mention_count, 1) AS mention_count,
       coalesce(node.description, '') AS description
"""


class EntityCandidate(TypedDict):
    name: str
    label: str
    embedding: list[float]
    mention_count: int
    description: str


@runtime_checkable
class EntityVectorStore(Protocol):
    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]: ...
    def upsert(self, entities: list[EntityCandidate]) -> None: ...


def _row_embedding(row: dict) -> list[float]:
    emb = row.get("er_vec")
    if emb:
        return list(emb)
    raw = row.get("er_embedding") or "[]"
    try:
        return json.loads(raw) if isinstance(raw, str) else list(raw)
    except (json.JSONDecodeError, TypeError):
        return []


class Neo4jEntityVectorStore:
    """Wraps the existing in-graph ER vector index (unchanged behavior)."""

    def __init__(self, graph_store: Any, *, dim: int | None = None):
        self._graph_store = graph_store
        self._dim = dim if dim is not None else settings.milvus.dim
        self._ensured = False

    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]:
        if not self._ensured:
            try:
                ensure_er_vector_index(self._graph_store, self._dim)
            except Exception as exc:
                logger.warning("ensure ER vector index failed: {e}", e=exc)
            self._ensured = True
        rows = self._graph_store.structured_query(
            _NEO4J_ER_KNN_CYPHER, param_map={"k": int(k), "vec": list(query_vec)},
        )
        out: list[EntityCandidate] = []
        for row in rows or []:
            name = row.get("name") or ""
            emb = _row_embedding(row)
            if not name or not emb:
                continue
            labels = [lab for lab in (row.get("labels") or [])
                      if lab not in ("__Entity__", "__Node__")]
            out.append({
                "name": name,
                "label": labels[0] if labels else "Other",
                "embedding": emb,
                "mention_count": int(row.get("mention_count") or 1),
                "description": row.get("description") or "",
            })
        return out

    def upsert(self, entities: list[EntityCandidate]) -> None:
        # No-op: the er_vec node property is persisted by the normal graph
        # node upsert in entity_resolution (unchanged neo4j write path).
        return None


def build_entity_vector_store(graph_store: Any) -> EntityVectorStore:
    """Dispatch: nebula (or the opt-in flag) -> Milvus; else Neo4j native."""
    use_milvus = (
        settings.graph.backend == "nebula"
        or settings.agent.er_vector_backend == "milvus"
    )
    if use_milvus:
        from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore

        return MilvusEntityVectorStore()
    return Neo4jEntityVectorStore(graph_store)
```

(Note: the Milvus impl lives in a sibling module `entity_vector_store_milvus.py` created in Task 2, imported lazily so this module has no pymilvus import-time cost and unit tests run without it.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_entity_vector_store.py tests/test_scripts/test_make_env.py -q`
Expected: PASS. Then `.venv/bin/python -m ruff check src/graph/entity_vector_store.py src/config.py` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/graph/entity_vector_store.py src/config.py scripts/make_env.py tests/test_graph/test_entity_vector_store.py
git commit -m "feat(er): EntityVectorStore seam + Neo4j impl + backend dispatch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `MilvusEntityVectorStore` (direct pymilvus, `entity_er_vec`)

**Files:**
- Create `src/graph/entity_vector_store_milvus.py`
- Test `tests/test_graph/test_entity_vector_store_milvus.py`

**Interfaces produced:**
- `MilvusEntityVectorStore(client=None, collection="entity_er_vec")`: ensures the collection on first write/read; `upsert(entities)` → `client.upsert`; `knn(query_vec, k)` → `client.search` returning `EntityCandidate` WITH `embedding` (the returned `er_vec`) and `label`.

**Milvus specifics (verify against the live cluster during execution — mirrors how Phase 2 nGQL was live-probed):** collection schema (PK `name` VARCHAR 512, vector `er_vec` FLOAT_VECTOR dim from `settings.milvus.dim`, `label` VARCHAR 256, `mention_count` INT64, `description` VARCHAR 4096); HNSW index (M/efConstruction from `MilvusSettings`), metric COSINE; `client.search(..., output_fields=["name","label","mention_count","description","er_vec"])`.

- [ ] **Step 1: Write the failing test (DB-free, fake MilvusClient)**

```python
# tests/test_graph/test_entity_vector_store_milvus.py
"""MilvusEntityVectorStore knn/upsert against a fake MilvusClient."""
from __future__ import annotations

from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore


class _FakeClient:
    def __init__(self, search_result=None):
        self.upserts = []
        self.searches = []
        self._search_result = search_result or []
        self._collections = []
    def has_collection(self, name): return name in self._collections
    def create_collection(self, **kw): self._collections.append(kw.get("collection_name"))
    def create_schema(self, **kw): return _FakeSchema()
    def prepare_index_params(self, **kw): return _FakeIndex()
    def upsert(self, collection_name, data): self.upserts.append((collection_name, data))
    def search(self, **kw):
        self.searches.append(kw)
        return self._search_result


class _FakeSchema:
    def add_field(self, **kw): return self
class _FakeIndex:
    def add_index(self, **kw): return self


def _store(client):
    s = MilvusEntityVectorStore.__new__(MilvusEntityVectorStore)
    s._client = client
    s._collection = "entity_er_vec"
    s._ensured = True   # skip DDL in the unit test
    return s


def test_upsert_writes_expected_rows():
    c = _FakeClient()
    _store(c).upsert([{"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                       "mention_count": 3, "description": "инженер"}])
    coll, data = c.upserts[0]
    assert coll == "entity_er_vec"
    assert data[0]["name"] == "Иванов" and data[0]["er_vec"] == [0.1, 0.2]
    assert data[0]["label"] == "PERSON" and data[0]["mention_count"] == 3


def test_knn_maps_hits_with_embedding_and_label():
    hit = {"entity": {"name": "Иванов", "label": "PERSON", "mention_count": 3,
                      "description": "инженер", "er_vec": [0.1, 0.2]}}
    c = _FakeClient(search_result=[[hit]])   # pymilvus: list-per-query of hits
    out = _store(c).knn([0.0, 0.0], 5)
    assert c.searches[0]["anns_field"] == "er_vec" and c.searches[0]["limit"] == 5
    assert out == [{"name": "Иванов", "label": "PERSON", "embedding": [0.1, 0.2],
                    "mention_count": 3, "description": "инженер"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_entity_vector_store_milvus.py -q`
Expected: FAIL (`No module named 'src.graph.entity_vector_store_milvus'`).

- [ ] **Step 3: Implement `entity_vector_store_milvus.py`**

```python
# src/graph/entity_vector_store_milvus.py
"""Milvus-backed EntityVectorStore (collection `entity_er_vec`).

Direct pymilvus.MilvusClient (mirrors src/storage/chunk_repository.py),
separate from the chunk collection. Only canonical entities are stored.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from src.config import settings
from src.graph.entity_vector_store import EntityCandidate

_COLLECTION = "entity_er_vec"
_NAME_MAX, _LABEL_MAX, _DESC_MAX = 512, 256, 4096


class MilvusEntityVectorStore:
    def __init__(self, client: Any | None = None, collection: str = _COLLECTION):
        from pymilvus import MilvusClient

        self._client = client or MilvusClient(
            uri=settings.milvus.uri, timeout=settings.milvus.timeout_s,
        )
        self._collection = collection
        self._ensured = False

    def _ensure(self) -> None:
        if self._ensured:
            return
        try:
            if not self._client.has_collection(self._collection):
                from pymilvus import DataType

                schema = self._client.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field("name", DataType.VARCHAR, is_primary=True, max_length=_NAME_MAX)
                schema.add_field("er_vec", DataType.FLOAT_VECTOR, dim=settings.milvus.dim)
                schema.add_field("label", DataType.VARCHAR, max_length=_LABEL_MAX)
                schema.add_field("mention_count", DataType.INT64)
                schema.add_field("description", DataType.VARCHAR, max_length=_DESC_MAX)
                index = self._client.prepare_index_params()
                index.add_index(
                    field_name="er_vec", index_type=settings.milvus.index_type,
                    metric_type="COSINE",
                    params={"M": settings.milvus.hnsw_m,
                            "efConstruction": settings.milvus.hnsw_ef_construction},
                )
                self._client.create_collection(
                    collection_name=self._collection, schema=schema, index_params=index,
                )
            self._ensured = True
        except Exception as exc:
            logger.warning("ensure entity_er_vec collection failed: {e}", e=exc)

    def upsert(self, entities: list[EntityCandidate]) -> None:
        if not entities:
            return
        self._ensure()
        data = [{
            "name": e["name"][:_NAME_MAX],
            "er_vec": list(e["embedding"]),
            "label": (e.get("label") or "")[:_LABEL_MAX],
            "mention_count": int(e.get("mention_count") or 1),
            "description": (e.get("description") or "")[:_DESC_MAX],
        } for e in entities if e.get("embedding")]
        if data:
            self._client.upsert(collection_name=self._collection, data=data)

    def knn(self, query_vec: list[float], k: int) -> list[EntityCandidate]:
        self._ensure()
        try:
            res = self._client.search(
                collection_name=self._collection, data=[list(query_vec)],
                anns_field="er_vec", limit=int(k),
                output_fields=["name", "label", "mention_count", "description", "er_vec"],
                search_params={"metric_type": "COSINE",
                               "params": {"ef": settings.milvus.hnsw_ef_search}},
            )
        except Exception as exc:
            logger.warning("entity_er_vec knn failed: {e}", e=exc)
            return []
        out: list[EntityCandidate] = []
        for hits in (res or []):
            for h in hits:
                e = h.get("entity", h) if isinstance(h, dict) else h
                name = e.get("name") or ""
                emb = e.get("er_vec")
                if not name or not emb:
                    continue
                out.append({
                    "name": name, "label": e.get("label") or "Other",
                    "embedding": list(emb),
                    "mention_count": int(e.get("mention_count") or 1),
                    "description": e.get("description") or "",
                })
        return out
```

- [ ] **Step 4: Run tests + ruff**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_entity_vector_store_milvus.py -q` → PASS.
Run: `.venv/bin/python -m ruff check src/graph/entity_vector_store_milvus.py` → clean.

- [ ] **Step 5: Commit**

```bash
git add src/graph/entity_vector_store_milvus.py tests/test_graph/test_entity_vector_store_milvus.py
git commit -m "feat(er): MilvusEntityVectorStore (entity_er_vec collection)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

**Live-verify note (controller, during execution):** against the running Milvus, create the collection, upsert 2 fixture entities, `knn` one, confirm the returned `er_vec`/`label` round-trip and the pymilvus `search` result shape matches `_FakeClient` (adjust the `h.get("entity", h)` unwrap if the installed pymilvus returns a different hit shape).

---

### Task 3: Wire `resolve_entities` to the vector store

**Files:**
- Modify `src/graph/entity_resolution.py` (`resolve_entities` + `_load_candidates_via_store`)
- Modify `src/workflow/activities/merge_and_resolve.py`
- Test: extend `tests/test_graph/test_entity_resolution.py`

**Interfaces:**
- Consumes: `EntityVectorStore` (Task 1), `build_entity_vector_store` (Task 1).
- Produces: `resolve_entities(..., vector_store: EntityVectorStore | None = None)`; when `cfg.use_native_vector_knn` and `vector_store` is provided, candidates come from `_load_candidates_via_store(vector_store, new_items, k)`; after canonicals are built, `vector_store.upsert(canonicals)` persists them (no-op for neo4j). Backward-compat: `vector_store=None` falls back to the existing `_load_candidates_native(graph_store, ...)`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_graph/test_entity_resolution.py
@pytest.mark.asyncio
async def test_resolve_entities_uses_vector_store_knn_and_upsert(monkeypatch):
    """When a vector_store is passed with native kNN on, ER pulls candidates
    from store.knn and upserts canonicals to store.upsert."""
    from src.graph.entity_resolution import ERConfig, resolve_entities

    class _VS:
        def __init__(self): self.knn_calls = 0; self.upserted = []
        def knn(self, vec, k):
            self.knn_calls += 1
            return []          # no stored candidates -> within-batch ER only
        def upsert(self, ents): self.upserted.extend(ents)

    vs = _VS()
    # one new entity; stub embed so it gets an embedding
    ents = [EntityNode(name="Иванов", label="PERSON", properties={})]
    async def _fake_embed(items, _model):
        for it in items: it.embedding = [0.1, 0.2]
        return True
    monkeypatch.setattr("src.graph.entity_resolution._embed_entities", _fake_embed)

    out_ents, _, _ = await resolve_entities(
        ents, [], [], llm=object(), embed_model=object(), graph_store=None,
        config=ERConfig(use_native_vector_knn=True, vector_knn_k=5),
        vector_store=vs,
    )
    assert vs.knn_calls == 1          # queried the store for the new entity
    assert len(vs.upserted) >= 1      # canonicals upserted to the vector store
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest "tests/test_graph/test_entity_resolution.py::test_resolve_entities_uses_vector_store_knn_and_upsert" -q`
Expected: FAIL (`resolve_entities() got an unexpected keyword argument 'vector_store'`).

- [ ] **Step 3: Add `_load_candidates_via_store` + wire `resolve_entities`**

In `src/graph/entity_resolution.py`, add a helper near `_load_candidates_native`:

```python
async def _load_candidates_via_store(
    vector_store: Any, new_items: list[_Item], *, k: int,
) -> list[_Item]:
    """Per new entity, fetch k nearest canonicals from an EntityVectorStore,
    dedup by name into stored _Items (mirrors _load_candidates_native but
    backend-agnostic)."""
    seen: dict[str, _Item] = {}
    for it in new_items:
        if not it.embedding:
            continue
        try:
            cands = await asyncio.to_thread(vector_store.knn, list(it.embedding), int(k))
        except Exception as exc:
            logger.warning("ER vector-store kNN failed: {e}", e=exc)
            continue
        for c in cands or []:
            name = c.get("name") or ""
            emb = c.get("embedding") or []
            if not name or name in seen or not emb:
                continue
            seen[name] = _Item(
                name=name, norm=_normalize_entity_name(name),
                label=c.get("label") or "Other",
                description=c.get("description") or "",
                mention_count=int(c.get("mention_count") or 1),
                source="stored", embedding=list(emb),
            )
    return list(seen.values())
```

Change `resolve_entities`'s signature to add `vector_store: Any | None = None`, and the step-3 dispatch (currently `entity_resolution.py:1374-1383`):

```python
    if cfg.use_native_vector_knn:
        if vector_store is not None:
            stored_items = await _load_candidates_via_store(
                vector_store, new_items, k=cfg.vector_knn_k,
            )
        else:
            from src.config import settings as _settings
            stored_items = await _load_candidates_native(
                graph_store, new_items, k=cfg.vector_knn_k, dim=_settings.milvus.dim,
            )
    else:
        stored_items = await _load_existing_canonicals(
            graph_store, limit=cfg.incremental_window,
        )
```

After the canonical `EntityNode`s are finalized (the block around `entity_resolution.py:1548-1560` where `ent.properties["er_vec"]` is set), add — persist to the vector store:

```python
    if vector_store is not None and cfg.use_native_vector_knn:
        canon_cands = [{
            "name": it.name, "label": it.label or "Other",
            "embedding": list(it.embedding), "mention_count": int(it.mention_count or 1),
            "description": it.description or "",
        } for it in all_items if getattr(it, "embedding", None)]
        try:
            await asyncio.to_thread(vector_store.upsert, canon_cands)
        except Exception as exc:
            logger.warning("ER vector-store upsert failed: {e}", e=exc)
```

(Confirm during implementation that `all_items` at that point are the resolved canonicals; if the code emits a distinct canonical list, upsert that instead. The exact variable is whatever holds the final canonical `_Item`s with embeddings.)

- [ ] **Step 4: Wire `merge_and_resolve.py`**

In `src/workflow/activities/merge_and_resolve.py` (around line 200), build and pass the store:

```python
        graph_store = build_graph_store()
        from src.graph.entity_vector_store import build_entity_vector_store
        vector_store = build_entity_vector_store(graph_store)
```
and add `vector_store=vector_store,` to the `resolve_entities(...)` call.

- [ ] **Step 5: Run tests + ruff**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_entity_resolution.py -q` → PASS (new test + existing ER tests green — the native path is unchanged when `vector_store=None`).
Run: `.venv/bin/python -m ruff check src/graph/entity_resolution.py src/workflow/activities/merge_and_resolve.py` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/graph/entity_resolution.py src/workflow/activities/merge_and_resolve.py tests/test_graph/test_entity_resolution.py
git commit -m "feat(er): route candidate-kNN + canonical upsert through EntityVectorStore

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Backfill script + parity benchmark + live gate

**Files:**
- Create `scripts/backfill_er_vec_milvus.py`
- Modify `tests/eval/scale/bench_er_native.py` (add a Milvus arm)

**Interfaces:** a `python -m scripts.backfill_er_vec_milvus` (dry-run default) that reads canonical `__Entity__` (`er_canonical_name IS NOT NULL`) er_vec/er_embedding+name/label/mention_count/description from Neo4j and `upsert`s into `entity_er_vec` via `MilvusEntityVectorStore`. Bench arm compares native-kNN vs Milvus-kNN recall + p95 on the synthetic set.

- [ ] **Step 1: Write the backfill script**

```python
# scripts/backfill_er_vec_milvus.py
"""Backfill entity_er_vec (Milvus) from existing Neo4j __Entity__ er_vec.

    python -m scripts.backfill_er_vec_milvus            # dry-run (counts only)
    python -m scripts.backfill_er_vec_milvus --no-dry-run

Greenfield nebula needs no backfill (ER writes to Milvus from the start).
"""
from __future__ import annotations

import argparse
import json

from loguru import logger

from src.graph.entity_vector_store_milvus import MilvusEntityVectorStore
from src.graph.store import build_neo4j_graph_store

_READ = """
MATCH (e:__Entity__) WHERE e.er_canonical_name IS NOT NULL
RETURN e.name AS name, labels(e) AS labels, e.er_vec AS er_vec,
       e.er_embedding AS er_embedding,
       coalesce(e.mention_count,1) AS mention_count,
       coalesce(e.description,'') AS description
"""


def _emb(row):
    v = row.get("er_vec")
    if v:
        return list(v)
    raw = row.get("er_embedding") or "[]"
    try:
        return json.loads(raw) if isinstance(raw, str) else list(raw)
    except Exception:
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=1000)
    args = ap.parse_args()

    store = build_neo4j_graph_store()
    rows = store.structured_query(_READ) or []
    cands = []
    for r in rows:
        emb = _emb(r)
        if not r.get("name") or not emb:
            continue
        labels = [x for x in (r.get("labels") or []) if x not in ("__Entity__", "__Node__")]
        cands.append({"name": r["name"], "label": labels[0] if labels else "Other",
                      "embedding": emb, "mention_count": int(r.get("mention_count") or 1),
                      "description": r.get("description") or ""})
    logger.info("backfill: {n} canonical entities with vectors", n=len(cands))
    if not args.no_dry_run:
        logger.info("dry-run — pass --no-dry-run to write to Milvus")
        return
    ms = MilvusEntityVectorStore()
    for i in range(0, len(cands), args.batch):
        ms.upsert(cands[i:i + args.batch])
    logger.info("backfill: upserted {n} to entity_er_vec", n=len(cands))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Static-validate the backfill**

Run: `.venv/bin/python -m py_compile scripts/backfill_er_vec_milvus.py` → OK.
Run: `API_ENV=development .venv/bin/python -c "import scripts.backfill_er_vec_milvus"` → imports clean (no DB touch at import).

- [ ] **Step 3: Add the Milvus arm to `bench_er_native.py`**

Read `tests/eval/scale/bench_er_native.py` first; add a `--store {native,milvus}` option (or a second measured arm) that runs the same synthetic-vector recall/latency loop through `MilvusEntityVectorStore.knn` and prints recall@k + p95 alongside the native numbers. Keep the existing native arm unchanged. (Exact code depends on the bench's current structure — implement to match it; this task's deliverable is the comparison arm, not a rewrite.)

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_er_vec_milvus.py tests/eval/scale/bench_er_native.py
git commit -m "test(er): entity_er_vec backfill + native-vs-milvus parity bench arm

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

**Live gate (controller, during execution):** against the running Milvus + a small graph, run the backfill `--no-dry-run` then the bench Milvus arm; confirm Milvus-kNN recall ≈ native and p95 acceptable. Also run one end-to-end ER pass under `GRAPH_BACKEND=nebula` (a couple of entities → merge_and_resolve) and confirm candidates come from `entity_er_vec`.

---

## Self-Review

**Spec coverage:** seam + Neo4j impl + factory + config (Task 1), Milvus impl (Task 2), ER wiring read+upsert (Task 3), backfill + parity bench (Task 4). Default neo4j path untouched (native branch, `vector_store=None` fallback preserved). `report_vec` + native-index removal remain deferred per spec.

**Placeholder scan:** Task 1/2/3 carry complete code from the read source. Task 3's exact upsert-variable and Task 4's bench arm are flagged "confirm against the current code" because they depend on lines the implementer will read — not TBDs, but code-shaped-to-existing-structure. No `add error handling`-style placeholders.

**Type consistency:** `EntityCandidate` keys (`name/label/embedding/mention_count/description`) are identical across the protocol, both impls, `_load_candidates_via_store`, the backfill, and every test. `knn` returns `embedding` everywhere (the `_candidate_pairs` requirement). `build_entity_vector_store` (Task 1) → `MilvusEntityVectorStore` (Task 2) lazy import is defined in 1 and satisfied in 2 (the Task-1 factory test asserts `ModuleNotFoundError` until then, mirroring the Phase-0 seam pattern).

**Live-verified assumptions to close during execution:** the pymilvus `search` hit shape (`h["entity"]` unwrap) and collection-DDL API (`create_schema`/`add_field`/`prepare_index_params`) against the installed pymilvus version; the exact canonical-`_Item` variable at the upsert point in `resolve_entities`.

# Nebula Read-Slice (Phase 2 vertical slice) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** With `GRAPH_BACKEND=nebula`, make `GraphRetriever.afind_entities_by_name` (entry point) and `GraphRetriever.awalk` (bounded N-hop) answer end-to-end against NebulaGraph, returning the same `RoundGraphData` shape as Neo4j — verified live.

**Architecture:** Backend-branch the two hand-rolled read methods on `settings.graph.backend`. Neo4j path untouched. Nebula path issues nGQL (`LOOKUP` for find-by-name, `GET SUBGRAPH` for walk) via the seam store, escaping user input with `_q` (no param binding), and maps nGQL results into the SAME intermediate row shapes the existing `_map_walk_rows` / entity-dict code consumes, so filtering, caps, dedup, and downstream serialisation are reused. Entity–entity relations use a generic `RELATED` edge carrying the original type in a `rel_type` property.

**Tech Stack:** Python/FastAPI, NebulaGraph 3.8 + `nebula3-python` 3.8.2 (data API: `ResultSet.row_values`, `ValueWrapper.as_list/as_node/as_relationship`, `Node.get_id/tags/properties`, `Relationship.start_vertex_id/end_vertex_id/edge_name/properties`), pytest.

## Global Constraints

- Default `GRAPH_BACKEND=neo4j`; the Neo4j read path (`_WALK_CYPHER`, `_FIND_BY_NAME_CYPHER`, `aretrieve`) is UNCHANGED and is the behavior for the default backend. nGQL runs only when `settings.graph.backend == "nebula"`.
- `awalk`/`afind_entities_by_name` return the SAME structures regardless of backend: `RoundGraphData` with `entities=[{entity_name, entity_type, description}]`, `relations=[{src_id, tgt_id, label, polarity?, valid_from?, valid_to?}]`.
- User-supplied entity names interpolated into nGQL MUST be escaped via `src.graph.nebula_store._q`; nGQL is issued with NO `param_map` (NebulaGraphStore.structured_query raises on a non-empty param_map).
- Reuse existing filtering/caps: `_map_walk_rows`, `_map_rel`, `_relation_is_live`, `_dedupe_entities`, `_dedupe_relations`, `GRAPH_WALK_NODE_CAP`, `GRAPH_WALK_EDGE_CAP`, `GRAPH_WALK_MAX_HOPS`.
- Unit tests DB-free (fake stores/sessions). Live parity verified on the running cluster (`docker compose --profile nebula`). Local commits only (no push). Never stage `docs/bruno/collection.bru`.
- Vector/synonym retrieve (`aretrieve`, `as_retriever`, `er_vec`) is OUT (Phase 3): under nebula it degrades to empty, it does not error.

## Verified-live facts (from probes, 2026-07-10 — use verbatim)

- **LOOKUP:** `LOOKUP ON \`Entity\` WHERE \`Entity\`.name == "<name>" YIELD id(vertex) AS vid, properties(vertex) AS p;` → rows with `row["vid"]` (32-hex str) and `row["p"]` (dict: `name/label/description/mention_count/created_at`). `.cast()` on the `p` column yields a Python dict. Works via `structured_query` (scalar/map columns). Index `entity_name_idx` is maintained for writes made after index creation (a real backfill of pre-existing data would need `REBUILD TAG INDEX entity_name_idx;` — out of scope for the slice; note it for the data-migration phase).
- **GET SUBGRAPH:** `GET SUBGRAPH WITH PROP <hops> STEPS FROM "<vid>" BOTH \`RELATED\` YIELD VERTICES AS nodes, EDGES AS rels;` (edge spec is `BOTH \`RELATED\``, NOT `OVER`). Returns keys `['nodes','rels']`, ONE ROW PER BFS LEVEL; each cell is a list value. Extract with the nebula3 data API on the raw `ResultSet` (NOT via `structured_query`, which `.cast()`-flattens):
  - `rs.row_values(i)[col_idx].as_list()` → list of `ValueWrapper`.
  - node: `vw.as_node()` → `node.get_id().cast()` (vid), `node.tags()[0]` (="Entity"), `node.properties("Entity")` → `{k: v.cast()}` (has `name/label/description/...`).
  - edge: `vw.as_relationship()` → `.start_vertex_id().cast()`, `.end_vertex_id().cast()`, `.edge_name()` (="RELATED"), `.properties()` → `{k: v.cast()}` (has `polarity/valid_from/valid_to` and, after Task 1, `rel_type`).
- **Wiring:** `src/workflow/_search_deps.py:66` already does `gs = build_graph_store()`, then `build_property_graph_index(graph_store=gs, ...)`, then `GraphRetriever(pg, ...)`. LlamaIndex's `PropertyGraphIndex` does NOT accept a `NebulaGraphStore` (subset store) → under nebula this raises and the retriever is silently disabled. The nebula path must construct a store-only `GraphRetriever`.

## File Structure

- Modify `src/graph/nebula_schema.py` — `RELATED` DDL gains `rel_type`.
- Modify `src/graph/nebula_store.py` — `upsert_relations` writes `RELATED` + `rel_type`; add `subgraph()` read method.
- Modify `src/graph/retriever.py` — nebula branch in `awalk`/`afind_entities_by_name`; store-only constructor; `aretrieve` guard.
- Modify `src/workflow/_search_deps.py` — backend branch for retriever construction.
- Tests: `tests/test_graph/test_nebula_store_writes.py`, `tests/test_graph/test_nebula_store_subgraph.py` (new), `tests/test_retrieval/test_graph_walk_retriever.py`, `tests/test_retrieval/test_nebula_read_slice.py` (new).
- Live: `tests/eval/migration/parity_read.py` (new).

---

### Task 1: Data model — generic `RELATED` + `rel_type`

**Files:**
- Modify: `src/graph/nebula_schema.py` (SCHEMA_DDL RELATED edge)
- Modify: `src/graph/nebula_store.py` (`upsert_relations`)
- Test: `tests/test_graph/test_nebula_store_writes.py`, `tests/test_graph/test_nebula_schema.py`

**Interfaces:**
- Produces: `RELATED` edge has property `rel_type string DEFAULT ''`. `upsert_relations` emits `INSERT EDGE \`RELATED\` (rel_type, polarity, valid_from, valid_to) VALUES <src> -> <tgt>:(<rel_type>, ...)` for every entity–entity relation, storing the original label in `rel_type` (a `_q`-escaped value). No caller label is spliced into the edge-type identifier.

- [ ] **Step 1: Update the failing tests first**

In `tests/test_graph/test_nebula_store_writes.py`, change `test_upsert_relations_inserts_edge` to assert the new shape, and update the injection test:

```python
def test_upsert_relations_inserts_related_with_rel_type():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="Иванов", target_id="Москва",
                          label="WORKS_AT", properties={"polarity": "pos"})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob
    assert f'"{entity_vid("Иванов")}" -> "{entity_vid("Москва")}"' in blob
    assert '"WORKS_AT"' in blob            # original type preserved as rel_type value


def test_upsert_relations_rel_type_is_a_value_not_identifier():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="a", target_id="b",
                          label='X`; DROP SPACE', properties={})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob     # always RELATED edge type
    assert "DROP SPACE" not in blob            # escaped inside a quoted value
```

Delete the old `test_upsert_relations_inserts_edge` and `test_upsert_relations_rejects_unsafe_label` (superseded — the edge type is now always `RELATED`, the label is a value).

In `tests/test_graph/test_nebula_schema.py`, add to `test_space_and_core_schema_present`:

```python
    assert "rel_type string" in "\n".join(SCHEMA_DDL)  # RELATED carries original type
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_store_writes.py tests/test_graph/test_nebula_schema.py -q`
Expected: FAIL (old RELATED DDL has no `rel_type`; upsert emits `INSERT EDGE \`WORKS_AT\``).

- [ ] **Step 3: Add `rel_type` to the RELATED DDL**

In `src/graph/nebula_schema.py`, change the RELATED line in `SCHEMA_DDL`:

```python
    "CREATE EDGE IF NOT EXISTS `RELATED` ("
    "rel_type string DEFAULT '', polarity string DEFAULT '', "
    "valid_from int DEFAULT 0, valid_to int DEFAULT 0);",
```

- [ ] **Step 4: Rewrite `upsert_relations` to emit RELATED + rel_type**

In `src/graph/nebula_store.py`, replace the body of `upsert_relations`:

```python
    def upsert_relations(self, relations: list[Any]) -> None:
        for r in relations:
            # Neo4j allows dynamic relationship types; Nebula needs declared
            # edge types. Entity-entity relations all become `RELATED`, with
            # the original type stored in the `rel_type` PROPERTY (a value,
            # so no edge-identifier injection). See ADR / Phase-2 spec.
            rel_type = getattr(r, "label", "") or ""
            props = getattr(r, "properties", {}) or {}
            src = entity_vid(getattr(r, "source_id", ""))
            tgt = entity_vid(getattr(r, "target_id", ""))
            stmt = (
                "INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to) VALUES "
                f"{_q(src)} -> {_q(tgt)}:("
                f"{_q(rel_type)}, "
                f"{_q(props.get('polarity', ''))}, "
                f"{int(props.get('valid_from', 0) or 0)}, "
                f"{int(props.get('valid_to', 0) or 0)});"
            )
            self._exec(stmt)
```

`_safe_edge_label` / `_SAFE_EDGE_LABEL` are now unused by `upsert_relations`; leave them in place only if another caller uses them, otherwise delete both (grep `_safe_edge_label` first — if the only reference is its own definition, remove it and its import of `re` if now unused).

- [ ] **Step 5: Run tests to verify they pass**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_store_writes.py tests/test_graph/test_nebula_schema.py -q`
Expected: PASS. Then `.venv/bin/python -m ruff check src/graph/nebula_store.py src/graph/nebula_schema.py` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/graph/nebula_schema.py src/graph/nebula_store.py tests/test_graph/test_nebula_store_writes.py tests/test_graph/test_nebula_schema.py
git commit -m "feat(graph): generic RELATED edge + rel_type property (dynamic-type mapping)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Store-only `GraphRetriever` for nebula + `aretrieve` guard

**Files:**
- Modify: `src/graph/retriever.py` (`__init__` or a classmethod, `aretrieve`)
- Modify: `src/workflow/_search_deps.py` (backend branch)
- Test: `tests/test_retrieval/test_nebula_read_slice.py` (new)

**Interfaces:**
- Consumes: `build_graph_store()`, `settings.graph.backend`.
- Produces: `GraphRetriever.for_store(store, *, filter_polarity_temporal=True, similarity_top_k=10) -> GraphRetriever` — a constructor that sets `_graph_store=store`, `_retriever=None`, `_retrievers={}`, and the same filter flag, WITHOUT building a LlamaIndex retriever. `aretrieve` returns an empty `RoundGraphData` when `_retriever is None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_retrieval/test_nebula_read_slice.py
"""Nebula read slice: store-only retriever construction + aretrieve guard."""
from __future__ import annotations

import pytest

from src.graph.retriever import GraphRetriever, RoundGraphData


class _FakeStore:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.last_query = None
    def structured_query(self, query, param_map=None):
        self.last_query = query
        assert not param_map, "nebula path must not pass param_map"
        return self._rows


def test_for_store_builds_without_llamaindex_retriever():
    store = _FakeStore()
    r = GraphRetriever.for_store(store)
    assert r._graph_store is store
    assert r._retriever is None


@pytest.mark.asyncio
async def test_aretrieve_empty_without_retriever():
    r = GraphRetriever.for_store(_FakeStore())
    out = await r.aretrieve("что угодно")
    assert isinstance(out, RoundGraphData)
    assert out.entities == [] and out.relations == [] and out.chunks == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_retrieval/test_nebula_read_slice.py -q`
Expected: FAIL (`GraphRetriever has no attribute 'for_store'`).

- [ ] **Step 3: Add the `for_store` classmethod + guard `aretrieve`**

In `src/graph/retriever.py`, add a classmethod on `GraphRetriever` (place after `__init__`):

```python
    @classmethod
    def for_store(
        cls,
        store,
        *,
        similarity_top_k: int = 10,
        filter_polarity_temporal: bool = True,
    ) -> "GraphRetriever":
        """Build a retriever backed only by a KbGraphStore (structured_query),
        without a LlamaIndex PropertyGraphIndex. Used for the nebula backend:
        awalk/afind_entities_by_name work over nGQL; the vector/synonym
        `aretrieve` path is unavailable (Phase 3) and returns empty."""
        r = cls.__new__(cls)
        r._pg_index = None
        r._retriever = None
        r._retrievers = {}
        r._similarity_top_k = similarity_top_k
        r._include_text = True
        r._default_path_depth = 1
        r._filter_polarity_temporal = filter_polarity_temporal
        r._graph_store = store
        return r
```

Guard `aretrieve` — insert at the very top of the method body (before `retriever = ...`):

```python
        if self._retriever is None:
            return RoundGraphData()
```

- [ ] **Step 4: Branch retriever construction on backend in `_search_deps.py`**

In `src/workflow/_search_deps.py`, inside `_get_graph_retriever` (the `try` block), after `gs = build_graph_store()` (line 66), branch before building the PropertyGraphIndex:

```python
        gs = build_graph_store()
        if settings.graph.backend == "nebula":
            # Nebula: no LlamaIndex PropertyGraphStore; the retriever works
            # over nGQL (awalk/afind). Vector/synonym aretrieve is Phase 3.
            return GraphRetriever.for_store(
                gs,
                similarity_top_k=settings.agent.graph_similarity_top_k,
                filter_polarity_temporal=(
                    settings.agent.graph_walk_filter_polarity_temporal
                ),
            )
        pg = build_property_graph_index(
```

(The existing neo4j path below is unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_retrieval/test_nebula_read_slice.py tests/test_retrieval/test_graph_walk_retriever.py -q`
Expected: PASS (new tests green; existing walk tests unaffected).

- [ ] **Step 6: Commit**

```bash
git add src/graph/retriever.py src/workflow/_search_deps.py tests/test_retrieval/test_nebula_read_slice.py
git commit -m "feat(graph): store-only GraphRetriever for nebula backend (aretrieve degrades)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: find-by-name via nGQL `LOOKUP`

**Files:**
- Modify: `src/graph/retriever.py` (`afind_entities_by_name` nebula branch + `_FIND_BY_NAME_NGQL` builder)
- Test: `tests/test_retrieval/test_nebula_read_slice.py`

**Interfaces:**
- Consumes: `settings.graph.backend`, `src.graph.nebula_store._q`, `self._graph_store.structured_query`.
- Produces: under nebula, `afind_entities_by_name(query, limit)` issues `LOOKUP ON \`Entity\` WHERE \`Entity\`.name == "<_q name>" YIELD id(vertex) AS vid, properties(vertex) AS p;` (no param_map), maps `row["p"]` dict → `{entity_name, entity_type=p["label"], description=p["description"]}`, dedupes, caps at `limit`.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_find_by_name_nebula_lookup(monkeypatch):
    monkeypatch.setattr("src.config.settings.graph", type(  # simple backend stub
        "G", (), {"backend": "nebula"})())
    rows = [{"vid": "abc", "p": {"name": "Иванов Иван", "label": "PERSON",
                                 "description": "инженер"}}]
    store = _FakeStore(rows=rows)
    r = GraphRetriever.for_store(store)
    out = await r.afind_entities_by_name("Иванов", limit=5)
    assert "LOOKUP ON `Entity`" in store.last_query
    assert '"Иванов"' in store.last_query
    assert out.entities == [{"entity_name": "Иванов Иван",
                             "entity_type": "PERSON", "description": "инженер"}]
```

(If monkeypatching `settings.graph` is awkward, instead `monkeypatch.setattr(src.graph.retriever.settings.graph, "backend", "nebula", raising=False)` after importing `settings` into retriever.py — see Step 3.)

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_retrieval/test_nebula_read_slice.py::test_find_by_name_nebula_lookup -q`
Expected: FAIL (nebula branch not implemented; query is the Neo4j fulltext one).

- [ ] **Step 3: Implement the nebula branch**

In `src/graph/retriever.py`: add imports at top — `from src.config import settings` and `from src.graph.nebula_store import _q as _nebula_q, entity_vid`. Add the nGQL builder near `_FIND_BY_NAME_CYPHER`:

```python
def _find_by_name_ngql(name: str) -> str:
    return (
        "LOOKUP ON `Entity` WHERE `Entity`.name == "
        f"{_nebula_q(name)} YIELD id(vertex) AS vid, properties(vertex) AS p;"
    )
```

In `afind_entities_by_name`, add the backend branch right after the `if self._graph_store is None: return RoundGraphData()` guard:

```python
        if settings.graph.backend == "nebula":
            cap = limit if limit is not None else self._similarity_top_k
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.structured_query, _find_by_name_ngql(query),
                )
            except Exception as exc:
                logger.warning("find_entities_by_name (nebula) failed: {e}", e=exc)
                return RoundGraphData()
            out = RoundGraphData()
            for row in (rows or [])[: int(cap)]:
                p = (row or {}).get("p") or {}
                name = p.get("name")
                if not name:
                    continue
                out.entities.append({
                    "entity_name": name,
                    "entity_type": p.get("label") or "",
                    "description": p.get("description") or "",
                })
            out.entities = _dedupe_entities(out.entities)
            return out
```

(The existing Neo4j `build_fulltext_query` path below is unchanged.)

- [ ] **Step 4: Run test to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_retrieval/test_nebula_read_slice.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/retriever.py tests/test_retrieval/test_nebula_read_slice.py
git commit -m "feat(graph): find-by-name via nGQL LOOKUP under nebula backend

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: bounded walk via nGQL `GET SUBGRAPH`

**Files:**
- Modify: `src/graph/nebula_store.py` (add `subgraph()`)
- Modify: `src/graph/retriever.py` (`awalk` nebula branch)
- Test: `tests/test_graph/test_nebula_store_subgraph.py` (new), `tests/test_retrieval/test_nebula_read_slice.py`

**Interfaces:**
- Produces: `NebulaGraphStore.subgraph(vid: str, hops: int, *, edge: str = "RELATED") -> list[dict]` returning a single-element list `[{"entities": [{name,label,description}], "relations": [{src,tgt,label,polarity,valid_from,valid_to}]}]` (the shape `GraphRetriever._map_walk_rows` consumes; `src`/`tgt` are entity NAMES resolved from vids, `label` is the edge's `rel_type` property). Uses the nebula3 data API on the raw ResultSet.
- Consumes (awalk): `settings.graph.backend`, `entity_vid`, `_map_walk_rows`.

- [ ] **Step 1: Write the failing test (subgraph mapper, DB-free with minimal fakes)**

```python
# tests/test_graph/test_nebula_store_subgraph.py
"""NebulaGraphStore.subgraph maps GET SUBGRAPH results into _map_walk_rows shape."""
from __future__ import annotations

from src.graph.nebula_store import NebulaGraphStore


class _VW:  # ValueWrapper stub
    def __init__(self, v): self._v = v
    def cast(self): return self._v

class _Node:
    def __init__(self, vid, props): self._vid, self._props = vid, props
    def get_id(self): return _VW(self._vid)
    def tags(self): return ["Entity"]
    def properties(self, tag): return {k: _VW(v) for k, v in self._props.items()}

class _Rel:
    def __init__(self, s, t, props): self._s, self._t, self._props = s, t, props
    def start_vertex_id(self): return _VW(self._s)
    def end_vertex_id(self): return _VW(self._t)
    def edge_name(self): return "RELATED"
    def properties(self): return {k: _VW(v) for k, v in self._props.items()}

class _Cell:  # a VERTICES/EDGES column value -> .as_list() of element wrappers
    def __init__(self, items, kind): self._items, self._kind = items, kind
    def as_list(self): return [_Elem(x, self._kind) for x in self._items]

class _Elem:
    def __init__(self, obj, kind): self._obj, self._kind = obj, kind
    def as_node(self): return self._obj
    def as_relationship(self): return self._obj

class _ResultSet:
    def __init__(self, rows): self._rows = rows  # rows: list[(nodes_cell, rels_cell)]
    def is_succeeded(self): return True
    def error_msg(self): return ""
    def keys(self): return ["nodes", "rels"]
    def row_size(self): return len(self._rows)
    def row_values(self, i): return list(self._rows[i])

class _Session:
    def __init__(self, rs): self._rs = rs
    def execute(self, q): self.last = q; return self._rs


def test_subgraph_maps_to_walk_rows_shape():
    ivan = _Node("v_ivan", {"name": "Иванов", "label": "PERSON", "description": "инженер"})
    mosk = _Node("v_mosk", {"name": "Москва", "label": "CITY", "description": "город"})
    edge = _Rel("v_ivan", "v_mosk",
                {"rel_type": "WORKS_AT", "polarity": "pos", "valid_from": 0, "valid_to": 0})
    rs = _ResultSet([(_Cell([ivan], "n"), _Cell([edge], "e")),
                     (_Cell([mosk], "n"), _Cell([], "e"))])
    store = NebulaGraphStore.__new__(NebulaGraphStore)
    store._session = _Session(rs)
    rows = store.subgraph("v_ivan", 2)
    assert len(rows) == 1
    ents = {e["name"] for e in rows[0]["entities"]}
    assert ents == {"Иванов", "Москва"}
    rels = rows[0]["relations"]
    assert rels == [{"src": "Иванов", "tgt": "Москва", "label": "WORKS_AT",
                     "polarity": "pos", "valid_from": 0, "valid_to": 0}]
    assert 'GET SUBGRAPH WITH PROP 2 STEPS FROM "v_ivan" BOTH `RELATED`' in store._session.last
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_store_subgraph.py -q`
Expected: FAIL (`NebulaGraphStore has no attribute 'subgraph'`).

- [ ] **Step 3: Implement `subgraph()`**

In `src/graph/nebula_store.py`, add a method to `NebulaGraphStore` (after `structured_query`):

```python
    def subgraph(self, vid: str, hops: int, *, edge: str = "RELATED") -> list[dict]:
        """Bounded GET SUBGRAPH from `vid`, mapped to the shape
        GraphRetriever._map_walk_rows consumes: a single-element list
        [{entities:[{name,label,description}], relations:[{src,tgt,label,
        polarity,valid_from,valid_to}]}] with src/tgt as entity NAMES and
        `label` taken from the edge's rel_type property."""
        q = (
            f"GET SUBGRAPH WITH PROP {int(hops)} STEPS FROM {_q(vid)} "
            f"BOTH `{edge}` YIELD VERTICES AS nodes, EDGES AS rels;"
        )
        rs = self._session.execute(q)
        if not rs.is_succeeded():
            logger.warning("nebula subgraph failed: {e}", e=rs.error_msg())
            return [{"entities": [], "relations": []}]
        vid_name: dict[str, str] = {}
        entities: list[dict] = []
        edges: list[dict] = []
        keys = rs.keys()
        ni, ei = keys.index("nodes"), keys.index("rels")
        for i in range(rs.row_size()):
            row = rs.row_values(i)
            for nv in row[ni].as_list():
                node = nv.as_node()
                nid = node.get_id().cast()
                props = {k: v.cast() for k, v in node.properties(node.tags()[0]).items()}
                name = props.get("name") or ""
                vid_name[nid] = name
                entities.append({
                    "name": name,
                    "label": props.get("label") or "",
                    "description": props.get("description") or "",
                })
            for ev in row[ei].as_list():
                e = ev.as_relationship()
                ep = {k: v.cast() for k, v in e.properties().items()}
                edges.append({
                    "_src_id": e.start_vertex_id().cast(),
                    "_tgt_id": e.end_vertex_id().cast(),
                    "rel_type": ep.get("rel_type") or "",
                    "polarity": ep.get("polarity"),
                    "valid_from": ep.get("valid_from"),
                    "valid_to": ep.get("valid_to"),
                })
        relations = [{
            "src": vid_name.get(e["_src_id"], ""),
            "tgt": vid_name.get(e["_tgt_id"], ""),
            "label": e["rel_type"],
            "polarity": e["polarity"],
            "valid_from": e["valid_from"],
            "valid_to": e["valid_to"],
        } for e in edges]
        return [{"entities": entities, "relations": relations}]
```

- [ ] **Step 4: Add the `awalk` nebula branch**

In `src/graph/retriever.py` `awalk`, right after the `if self._graph_store is None: return RoundGraphData()` guard:

```python
        if settings.graph.backend == "nebula":
            safe_hops = max(1, min(int(hops), GRAPH_WALK_MAX_HOPS))
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.subgraph, entity_vid(start_entity), safe_hops,
                )
            except Exception as exc:
                logger.warning("graph_walk (nebula) failed: {e}", e=exc)
                return RoundGraphData()
            out = self._map_walk_rows(rows)
            if rel_filter:
                allow = set(rel_filter)
                out.relations = [r for r in out.relations if r.get("label") in allow]
            return out
```

(`_map_walk_rows` already applies `_map_rel` polarity/temporal filtering + dedup + caps. `rel_filter` now filters on the mapped `label` = rel_type.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_store_subgraph.py tests/test_retrieval/ -q`
Expected: PASS. Then ruff clean on the two modified src files.

- [ ] **Step 6: Commit**

```bash
git add src/graph/nebula_store.py src/graph/retriever.py tests/test_graph/test_nebula_store_subgraph.py tests/test_retrieval/test_nebula_read_slice.py
git commit -m "feat(graph): bounded walk via nGQL GET SUBGRAPH under nebula backend

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Live parity gate

**Files:**
- Create: `tests/eval/migration/parity_read.py`

**Interfaces:**
- Consumes: `build_graph_store()`, `GraphRetriever.for_store`, the Task-1 fixture write via `NebulaGraphStore.upsert_*`.

- [ ] **Step 1: Write the read-parity harness**

```python
# tests/eval/migration/parity_read.py
"""Live read-slice gate: write the fixture, then find-by-name + walk under
nebula and print the RoundGraphData. Manual, needs a live cluster:

    GRAPH_BACKEND=nebula API_ENV=development \
      python -m tests.eval.migration.parity_read
"""
from __future__ import annotations

import asyncio
import json

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.retriever import GraphRetriever
from src.graph.store import build_graph_store

NODES = [
    EntityNode(name="Иванов", label="PERSON", properties={"description": "инженер", "mention_count": 3}),
    EntityNode(name="Москва", label="CITY", properties={"description": "город", "mention_count": 9}),
]
RELS = [Relation(source_id="Иванов", target_id="Москва", label="WORKS_AT",
                 properties={"polarity": "pos"})]


async def main() -> None:
    store = build_graph_store()
    store.upsert_nodes(NODES)
    store.upsert_relations(RELS)
    r = GraphRetriever.for_store(store)
    found = await r.afind_entities_by_name("Иванов")
    walked = await r.awalk("Иванов", hops=2)
    print(json.dumps({
        "found_entities": found.entities,
        "walk_entities": walked.entities,
        "walk_relations": walked.relations,
    }, ensure_ascii=False, indent=2))
    assert any(e["entity_name"] == "Иванов" for e in found.entities), "find-by-name failed"
    assert {e["entity_name"] for e in walked.entities} >= {"Иванов", "Москва"}, "walk entities"
    assert any(rl["label"] == "WORKS_AT" for rl in walked.relations), "walk rel_type lost"
    print("PARITY READ OK")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Static-validate (no live run in CI)**

Run: `.venv/bin/python -m py_compile tests/eval/migration/parity_read.py` → OK.

- [ ] **Step 3: Live run (manual gate — controller/user runs against the cluster)**

```bash
docker compose --profile nebula up -d nebula-metad nebula-storaged nebula-graphd
python scripts/nebula_bootstrap.py    # if not already registered
GRAPH_BACKEND=nebula API_ENV=development .venv/bin/python -m tests.eval.migration.parity_read
```
Expected: prints `PARITY READ OK` with Иванов found, {Иванов, Москва} walked, and a `WORKS_AT` relation (proves rel_type round-trips through GET SUBGRAPH).

- [ ] **Step 4: Commit**

```bash
git add tests/eval/migration/parity_read.py
git commit -m "test(graph): live read-parity gate for the nebula read slice

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:** data model (Task 1), store-only retriever + aretrieve guard (Task 2, the wiring gap the probes found), find-by-name LOOKUP (Task 3), walk GET SUBGRAPH (Task 4), live parity (Task 5). All map to the approved spec's in-scope items; vector/synonym/full-text/other-read-sites remain explicitly deferred.

**Placeholder scan:** every code step contains complete code derived from live probes (exact nGQL syntax `BOTH \`RELATED\``, the nebula3 `as_node/as_relationship` API, the LOOKUP column names). No TBD/TODO.

**Type consistency:** `subgraph()` returns the exact `[{"entities":[{name,label,description}], "relations":[{src,tgt,label,polarity,valid_from,valid_to}]}]` shape that `_map_walk_rows` (retriever.py:431) consumes via `_map_rel` (which reads `src`/`tgt`/`label`/`polarity`/`valid_from`/`valid_to`). `for_store` sets every attribute `awalk`/`afind`/`aretrieve` read (`_graph_store`, `_retriever`, `_retrievers`, `_similarity_top_k`, `_filter_polarity_temporal`, `_include_text`, `_default_path_depth`, `_pg_index`). find-by-name maps `row["p"]` per the verified LOOKUP column.

**Known caveats recorded:** LOOKUP needs `REBUILD TAG INDEX` for pre-existing data (data-migration phase, not this slice); GET SUBGRAPH returns per-BFS-level rows (handled by aggregating across rows); nebula `aretrieve` returns empty (vector = Phase 3).

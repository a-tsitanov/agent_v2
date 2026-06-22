# Community detection offload — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move GDS Leiden community detection out of Neo4j's JVM heap (OOMs with "Java heap space" as the `__Entity__` graph grows) into the `kb-graph-build` Temporal worker using `leidenalg`/`python-igraph`, behind an opt-in, benchmarked backend switch.

**Architecture:** Only the compute step changes. Both `detect_communities` (single level) and `detect_hierarchy` (dendrogram) build a `rows` list (`[{name, communityId, ids}]`); everything downstream (`_coarsest_from_rows`, `_group_by_levels`, `:Community` persistence) consumes `rows` and stays UNCHANGED. A new `src/graph/community_leiden.py` produces the identical `rows` shape from a streamed edge list + in-process `leidenalg`. A config flag selects `gds` (default) or `leidenalg`; the default flips only after a strict-parity benchmark.

**Tech Stack:** Python 3.12, `python-igraph`, `leidenalg`, Temporal (`temporalio`), Neo4j driver via `store.structured_query`, pytest, uv.

## Global Constraints

- Python `>=3.12`; dependency pins use the project's `>=X,<Y` style in `pyproject.toml`, lock via `uv lock`, regenerate `requirements.txt` (pip-freeze style, `uv export --no-hashes --no-emit-project --no-dev --no-annotate --no-header`).
- Ruff lint clean on changed files (`uv run ruff check <files>`; select E,F,I,B,UP,SIM,RUF).
- Every new app env var MUST have a Russian description in `scripts/make_env.py` `_ENV_DESCRIPTIONS` (enforced by `tests/test_scripts/test_make_env.py::test_every_env_var_has_russian_description`).
- Project policy: benchmark before adopting; the swap is opt-in; **default `community_backend` stays `"gds"`** until the strict-parity benchmark (Task 7) passes.
- Output contract is FROZEN: the leidenalg backend must emit rows `{"name": str, "communityId": <hashable>, "ids": [finest..coarsest]}` so `_coarsest_from_rows` and `_group_by_levels` work unchanged (`communityId == ids[-1]`, `ids` finest→coarsest).
- Run tests with `uv run pytest`.

---

### Task 1: Dependencies + backend config flag

**Files:**
- Modify: `pyproject.toml` (dependencies list)
- Modify: `src/config.py:258-269` (`TemporalSettings`)
- Modify: `scripts/make_env.py` (`_ENV_DESCRIPTIONS` dict, TEMPORAL_* block ~line 322)
- Test: `tests/test_config/` (new assertion) and existing `tests/test_scripts/test_make_env.py`
- Generate: `requirements.txt`

**Interfaces:**
- Produces: `settings.temporal.community_backend: Literal["gds","leidenalg"]` (default `"gds"`), env `TEMPORAL_COMMUNITY_BACKEND`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_config/test_community_backend.py`:
```python
from src.config import settings


def test_community_backend_defaults_to_gds():
    assert settings.temporal.community_backend == "gds"


def test_community_backend_is_constrained():
    import typing
    from src.config import TemporalSettings
    hints = typing.get_type_hints(TemporalSettings)
    # Literal["gds","leidenalg"]
    assert "gds" in typing.get_args(hints["community_backend"])
    assert "leidenalg" in typing.get_args(hints["community_backend"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config/test_community_backend.py -q`
Expected: FAIL (`AttributeError: ... has no attribute 'community_backend'`).

- [ ] **Step 3: Add the dependencies**

In `pyproject.toml`, under the `# Storage clients` / graph block (near `neo4j`), add:
```toml
    "python-igraph>=0.11,<0.12",
    "leidenalg>=0.10,<0.11",
```

- [ ] **Step 4: Add the config field**

In `src/config.py`, add `Literal` to the typing import if missing (it is already imported), then in `TemporalSettings` (after `staging_bucket`, near the other `community_*` knobs):
```python
    # Community-detection backend.  "gds" = in-Neo4j GDS Leiden (legacy);
    # "leidenalg" = in-worker leidenalg/igraph (memory off Neo4j).  Default
    # stays "gds" until the strict-parity benchmark passes (project policy:
    # benchmark before adopting).
    community_backend: Literal["gds", "leidenalg"] = "gds"
```

- [ ] **Step 5: Add the Russian env description**

In `scripts/make_env.py` `_ENV_DESCRIPTIONS`, in the alphabetically-sorted TEMPORAL_ block (right after `"TEMPORAL_COMMUNITY_SUMMARY_PARALLELISM": ...`):
```python
    "TEMPORAL_COMMUNITY_BACKEND": "Движок детекции сообществ: 'gds' (Leiden в Neo4j, легаси) или 'leidenalg' (leidenalg/igraph в воркере, память вне Neo4j). Дефолт 'gds' до прохождения бенчмарка паритета.",
```

- [ ] **Step 6: Lock, sync, regenerate requirements**

```bash
uv lock && uv sync --extra dev --extra gliner
uv export --format requirements-txt --no-hashes --no-emit-project --no-dev --no-annotate --no-header -o requirements.txt
uv run python -c "import igraph, leidenalg; print('igraph', igraph.__version__, 'leidenalg', leidenalg.version)"
```
Expected: prints versions; no import error.

- [ ] **Step 7: Run tests**

Run: `uv run pytest tests/test_config/test_community_backend.py tests/test_scripts/test_make_env.py -q`
Expected: PASS (including `test_every_env_var_has_russian_description`).

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock requirements.txt src/config.py scripts/make_env.py tests/test_config/test_community_backend.py
git commit -m "feat(community): add leidenalg deps + community_backend flag (default gds)"
```

---

### Task 2: Streamed edge extractor

**Files:**
- Create: `src/graph/community_leiden.py`
- Test: `tests/test_graph/test_community_leiden_extract.py`

**Interfaces:**
- Produces: `extract_entity_edges(store, *, batch_size: int = 50_000) -> tuple[list[tuple[str, str, float]], list[str]]` — returns `(edges, node_names)`. `edges` are `(src_name, tgt_name, weight)`; `node_names` includes isolated entities. Streams via keyset pagination so no single query holds the whole result.

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_leiden_extract.py`:
```python
from src.graph.community_leiden import extract_entity_edges


class _FakeStore:
    """Returns one page of node rows, then one page of edge rows, then empties."""

    def __init__(self):
        self.calls = 0

    def structured_query(self, cypher, param_map=None):
        param_map = param_map or {}
        if "RETURN e.name AS name" in cypher:           # node page
            if param_map.get("after") in (None, ""):
                return [{"name": "A"}, {"name": "B"}, {"name": "C"}]
            return []
        # edge page
        if param_map.get("after") in (None, ""):
            return [
                {"src": "A", "tgt": "B", "weight": 2.0},
                {"src": "B", "tgt": "C", "weight": 1.0},
            ]
        return []


def test_extract_returns_edges_and_all_node_names():
    edges, nodes = extract_entity_edges(_FakeStore(), batch_size=10)
    assert ("A", "B", 2.0) in edges
    assert ("B", "C", 1.0) in edges
    assert set(nodes) == {"A", "B", "C"}      # includes isolated entities
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_graph/test_community_leiden_extract.py -q`
Expected: FAIL (`ModuleNotFoundError: src.graph.community_leiden`).

- [ ] **Step 3: Implement the extractor**

Create `src/graph/community_leiden.py`:
```python
"""In-worker Leiden community detection (leidenalg/igraph).

Produces the SAME ``rows`` shape as the GDS path
(``[{name, communityId, ids}]``, ``ids`` finest->coarsest) so
``communities._coarsest_from_rows`` / ``_group_by_levels`` and all
:Community persistence are reused unchanged.  Memory lives in the worker
process, not Neo4j's JVM heap.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

# Keyset-paginated reads so we never materialise one giant result set.
_NODES_CYPHER = """
MATCH (e:__Entity__)
WHERE $after = '' OR e.name > $after
RETURN e.name AS name
ORDER BY e.name
LIMIT $limit
"""

_EDGES_CYPHER = """
MATCH (s:__Entity__)-[r]->(t:__Entity__)
WHERE $after = '' OR s.name > $after
RETURN s.name AS src, t.name AS tgt,
       coalesce(r.weight, 1.0) AS weight, s.name AS cursor
ORDER BY s.name
LIMIT $limit
"""


def extract_entity_edges(
    store: Any, *, batch_size: int = 50_000,
) -> tuple[list[tuple[str, str, float]], list[str]]:
    """Stream the ``__Entity__`` graph out of Neo4j as (edges, node_names)."""
    names: list[str] = []
    after = ""
    while True:
        page = store.structured_query(
            _NODES_CYPHER, param_map={"after": after, "limit": batch_size},
        )
        if not page:
            break
        for row in page:
            n = row.get("name")
            if n:
                names.append(str(n))
        after = names[-1] if names else after
        if len(page) < batch_size:
            break

    edges: list[tuple[str, str, float]] = []
    after = ""
    while True:
        page = store.structured_query(
            _EDGES_CYPHER, param_map={"after": after, "limit": batch_size},
        )
        if not page:
            break
        for row in page:
            s, t = row.get("src"), row.get("tgt")
            if s and t:
                edges.append((str(s), str(t), float(row.get("weight") or 1.0)))
        after = str(page[-1].get("cursor") or after)
        if len(page) < batch_size:
            break

    logger.info(
        "community_leiden: streamed {e} edges / {n} entities from Neo4j",
        e=len(edges), n=len(names),
    )
    return edges, names
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_graph/test_community_leiden_extract.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/community_leiden.py tests/test_graph/test_community_leiden_extract.py
git commit -m "feat(community): streamed __Entity__ edge extractor for leidenalg backend"
```

---

### Task 3: Clusterer — flat single-level rows

**Files:**
- Modify: `src/graph/community_leiden.py`
- Test: `tests/test_graph/test_community_leiden_cluster.py`

**Interfaces:**
- Consumes: `extract_entity_edges` output (edges, nodes).
- Produces:
  - `build_graph(edges, node_names) -> tuple[Any, list[str]]` — returns `(igraph.Graph, names_by_index)`; parallel edges summed; undirected.
  - `single_level_rows(edges, node_names, *, gamma: float, seed: int = 19) -> list[dict]` — rows `[{"name","communityId","ids":[cid]}]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_leiden_cluster.py`:
```python
from src.graph.community_leiden import single_level_rows


def test_two_cliques_split_into_two_communities():
    # Two triangles joined by a single weak edge → two communities.
    edges = [
        ("a", "b", 5.0), ("b", "c", 5.0), ("a", "c", 5.0),
        ("x", "y", 5.0), ("y", "z", 5.0), ("x", "z", 5.0),
        ("c", "x", 0.1),
    ]
    nodes = ["a", "b", "c", "x", "y", "z"]
    rows = single_level_rows(edges, nodes, gamma=1.0, seed=19)

    by_name = {r["name"]: r["communityId"] for r in rows}
    assert by_name["a"] == by_name["b"] == by_name["c"]
    assert by_name["x"] == by_name["y"] == by_name["z"]
    assert by_name["a"] != by_name["x"]
    # contract: communityId == ids[-1]
    for r in rows:
        assert r["ids"][-1] == r["communityId"]


def test_deterministic_with_fixed_seed():
    edges = [("a", "b", 1.0), ("b", "c", 1.0), ("x", "y", 1.0)]
    nodes = ["a", "b", "c", "x", "y"]
    r1 = single_level_rows(edges, nodes, gamma=1.0, seed=19)
    r2 = single_level_rows(edges, nodes, gamma=1.0, seed=19)
    assert {r["name"]: r["communityId"] for r in r1} == \
           {r["name"]: r["communityId"] for r in r2}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_graph/test_community_leiden_cluster.py -q`
Expected: FAIL (`ImportError: cannot import name 'single_level_rows'`).

- [ ] **Step 3: Implement build_graph + single_level_rows**

Append to `src/graph/community_leiden.py`:
```python
def build_graph(
    edges: list[tuple[str, str, float]], node_names: list[str],
) -> tuple[Any, list[str]]:
    """Build an undirected weighted igraph; parallel edges summed."""
    import igraph as ig

    names: list[str] = list(dict.fromkeys(
        list(node_names) + [e[0] for e in edges] + [e[1] for e in edges],
    ))
    idx = {n: i for i, n in enumerate(names)}
    g = ig.Graph(n=len(names), directed=False)
    elist = [(idx[s], idx[t]) for s, t, _ in edges if s in idx and t in idx]
    weights = [w for s, t, w in edges if s in idx and t in idx]
    g.add_edges(elist)
    if weights:
        g.es["weight"] = weights
    # Collapse parallel/self edges (GDS undirected projection is simple).
    # NB: simplify() mutates in place — do NOT reassign (it can return None).
    g.simplify(multiple=True, loops=True, combine_edges={"weight": "sum"})
    return g, names


def single_level_rows(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int = 19,
) -> list[dict]:
    """Flat leidenalg partition → rows ``[{name, communityId, ids:[cid]}]``."""
    import leidenalg as la

    g, names = build_graph(edges, node_names)
    weights = g.es["weight"] if "weight" in g.es.attributes() else None
    part = la.find_partition(
        g, la.RBConfigurationVertexPartition,
        weights=weights, resolution_parameter=gamma, seed=seed,
    )
    membership = part.membership  # community index per vertex
    rows: list[dict] = []
    for i, name in enumerate(names):
        cid = str(membership[i])
        rows.append({"name": name, "communityId": cid, "ids": [cid]})
    return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_graph/test_community_leiden_cluster.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/graph/community_leiden.py tests/test_graph/test_community_leiden_cluster.py
git commit -m "feat(community): leidenalg flat single-level clustering -> rows"
```

---

### Task 4: Clusterer — hierarchy via iterative aggregation

**Files:**
- Modify: `src/graph/community_leiden.py`
- Test: `tests/test_graph/test_community_leiden_hierarchy.py`

**Interfaces:**
- Consumes: `build_graph`, `single_level_rows` (Task 3).
- Produces: `hierarchy_rows(edges, node_names, *, gamma: float, max_levels: int, seed: int = 19) -> list[dict]` — rows `[{"name","communityId","ids":[finest..coarsest]}]`. `ids` ordering and `communityId == ids[-1]` match GDS `intermediateCommunityIds`, so `_group_by_levels` works unchanged.

**Approach (the spec's flagged risk — validated here):** leidenalg gives a flat partition, so build the dendrogram by iterative aggregation: cluster the current graph (finest first), record each original node's community id at that level, then aggregate (one supernode per community, summed inter-community weights) and re-cluster the smaller graph. Repeat until ≤1 community or `max_levels` reached. Each original node accumulates `[finest_cid, ..., coarsest_cid]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_leiden_hierarchy.py`:
```python
from src.graph.community_leiden import hierarchy_rows


def _two_level_graph():
    # Four triangles; triangles pair up at a coarser level.
    edges = []
    for a, b, c in [("a","b","c"), ("d","e","f"), ("p","q","r"), ("x","y","z")]:
        edges += [(a,b,5.0),(b,c,5.0),(a,c,5.0)]
    edges += [("c","d",1.0), ("r","x",1.0)]   # pair (abc,def) and (pqr,xyz)
    edges += [("f","p",0.05)]                  # very weak cross-pair link
    nodes = list("abcdefpqrxyz")
    return edges, nodes


def test_ids_are_finest_to_coarsest_and_nested():
    edges, nodes = _two_level_graph()
    rows = hierarchy_rows(edges, nodes, gamma=1.0, max_levels=3, seed=19)
    by_name = {r["name"]: r["ids"] for r in rows}
    # contract: communityId == ids[-1]
    for r in rows:
        assert r["communityId"] == r["ids"][-1]
    # nesting: nodes sharing the finest id share every coarser id
    for n1 in nodes:
        for n2 in nodes:
            if by_name[n1][0] == by_name[n2][0]:
                assert by_name[n1] == by_name[n2]


def test_single_level_when_max_levels_1_matches_flat():
    edges, nodes = _two_level_graph()
    rows = hierarchy_rows(edges, nodes, gamma=1.0, max_levels=1, seed=19)
    for r in rows:
        assert len(r["ids"]) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_graph/test_community_leiden_hierarchy.py -q`
Expected: FAIL (`ImportError: cannot import name 'hierarchy_rows'`).

- [ ] **Step 3: Implement hierarchy_rows**

Append to `src/graph/community_leiden.py`:
```python
def hierarchy_rows(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, max_levels: int, seed: int = 19,
) -> list[dict]:
    """Build a Leiden dendrogram by iterative aggregation.

    Returns rows ``[{name, communityId, ids:[finest..coarsest]}]`` matching
    the GDS ``intermediateCommunityIds`` contract.
    """
    import leidenalg as la

    g, names = build_graph(edges, node_names)
    if max_levels <= 1:
        return single_level_rows(edges, node_names, gamma=gamma, seed=seed)

    # path[name] accumulates community ids finest->coarsest.
    path: dict[str, list[str]] = {n: [] for n in names}
    # current_members[super_idx] = list of ORIGINAL node names it represents.
    current_members: list[list[str]] = [[n] for n in names]
    cur = g

    for _level in range(max_levels):
        weights = cur.es["weight"] if "weight" in cur.es.attributes() else None
        part = la.find_partition(
            cur, la.RBConfigurationVertexPartition,
            weights=weights, resolution_parameter=gamma, seed=seed,
        )
        membership = part.membership
        ncomm = len(set(membership))
        # Stamp this level's community id onto every original node.
        for super_idx, comm in enumerate(membership):
            cid = str(comm)
            for orig in current_members[super_idx]:
                path[orig].append(cid)
        if ncomm <= 1:
            break
        # Aggregate: one supernode per community; sum inter-community weights.
        next_members: list[list[str]] = [[] for _ in range(ncomm)]
        for super_idx, comm in enumerate(membership):
            next_members[comm].extend(current_members[super_idx])
        agg_w: dict[tuple[int, int], float] = {}
        ew = cur.es["weight"] if "weight" in cur.es.attributes() else None
        for eidx, e in enumerate(cur.es):
            cu, cv = membership[e.source], membership[e.target]
            if cu == cv:
                continue
            key = (cu, cv) if cu < cv else (cv, cu)
            agg_w[key] = agg_w.get(key, 0.0) + (ew[eidx] if ew else 1.0)
        import igraph as ig
        nxt = ig.Graph(n=ncomm, directed=False)
        if agg_w:
            nxt.add_edges(list(agg_w.keys()))
            nxt.es["weight"] = list(agg_w.values())
        cur, current_members = nxt, next_members
        if ncomm <= 1:
            break

    # path is finest->coarsest already (level 0 appended first).
    rows: list[dict] = []
    for name in names:
        ids = path[name] or ["0"]
        rows.append({"name": name, "communityId": ids[-1], "ids": ids})
    return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_graph/test_community_leiden_hierarchy.py -q`
Expected: PASS. (If nesting fails, the aggregation propagation is wrong — fix `current_members` threading before moving on; do NOT proceed with a broken hierarchy.)

- [ ] **Step 5: Commit**

```bash
git add src/graph/community_leiden.py tests/test_graph/test_community_leiden_hierarchy.py
git commit -m "feat(community): leidenalg hierarchy via iterative aggregation -> rows"
```

---

### Task 5: Wire the backend switch into communities.py

**Files:**
- Modify: `src/graph/communities.py:368-467` (`detect_communities`), `:470-616` (`detect_hierarchy`)
- Test: `tests/test_graph/test_community_backend_switch.py`

**Interfaces:**
- Consumes: `single_level_rows`, `hierarchy_rows`, `extract_entity_edges` (Tasks 2-4); `settings.temporal.community_backend` (Task 1); existing `_coarsest_from_rows`, `_group_by_levels`, persistence Cypher (unchanged).

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_backend_switch.py`:
```python
import asyncio

import src.graph.communities as comm


def test_leidenalg_backend_produces_communityrefs(monkeypatch):
    # Backend = leidenalg; stub edge extraction + a no-op store write path.
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "leidenalg")

    def fake_extract(store, *, batch_size=50_000):
        edges = [("a","b",5.0),("b","c",5.0),("a","c",5.0),
                 ("x","y",5.0),("y","z",5.0),("x","z",5.0),("c","x",0.1)]
        return edges, list("abcxyz")
    monkeypatch.setattr(comm, "extract_entity_edges", fake_extract)

    class _Store:
        def structured_query(self, cypher, param_map=None):
            return []          # swallow all writes/reads
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert len(refs) == 2                       # two cliques
    assert all(r.level == 0 for r in refs)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_graph/test_community_backend_switch.py -q`
Expected: FAIL (`AttributeError: module ... has no attribute 'extract_entity_edges'` — not imported yet).

- [ ] **Step 3: Add imports + a rows helper to communities.py**

At the top of `src/graph/communities.py` (with the other imports), add:
```python
from src.config import settings
from src.graph.community_leiden import (
    extract_entity_edges, hierarchy_rows, single_level_rows,
)
```
Add a helper above `detect_communities`:
```python
async def _leiden_rows(
    store: Any, *, gamma: float, max_levels: int, seed: int = 19,
) -> list[dict]:
    """leidenalg backend: stream edges + cluster in-worker (off Neo4j heap).

    Returns the SAME rows shape as the GDS leiden stream
    (``[{name, communityId, ids}]``).  CPU-bound clustering runs in a
    thread so the activity event loop (and its heartbeat) stays live.
    """
    edges, names = await asyncio.to_thread(extract_entity_edges, store)
    if max_levels > 1:
        return await asyncio.to_thread(
            hierarchy_rows, edges, names,
            gamma=gamma, max_levels=max_levels, seed=seed,
        )
    return await asyncio.to_thread(
        single_level_rows, edges, names, gamma=gamma, seed=seed,
    )
```

- [ ] **Step 4: Branch the compute step in `detect_communities`**

In `detect_communities`, replace the GDS project/stream block (the `try: ... rows = await asyncio.to_thread(_run_query, ... _leiden_stream_cypher ...)` section, lines ~396-414) with a backend branch — GDS path unchanged, leidenalg path uses `_leiden_rows`:
```python
    if settings.temporal.community_backend == "leidenalg":
        try:
            rows = await _leiden_rows(store, gamma=gamma, max_levels=1)
            stats = {"nodes": len({r["name"] for r in rows}), "rels": -1}
        except Exception as exc:  # noqa: BLE001
            logger.error("communities: leidenalg detection FAILED: {e}", e=exc)
            return []
    else:
        graph_name = _new_graph_name()
        try:
            await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
            proj_rows = await asyncio.to_thread(_run_query, store, _project_cypher(graph_name))
            stats = _projection_stats(proj_rows)
            rows = await asyncio.to_thread(
                _run_query, store,
                _leiden_stream_cypher(graph_name, gamma=gamma, concurrency=concurrency),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("communities: GDS Leiden detection FAILED: {e}", e=exc)
            with contextlib.suppress(Exception):
                await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
            return []
```
Then in the persistence `finally:` block, guard the GDS-only drop so the leidenalg path skips it:
```python
    finally:
        if settings.temporal.community_backend != "leidenalg":
            with contextlib.suppress(Exception):
                await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))
```

- [ ] **Step 5: Branch the compute step in `detect_hierarchy` identically**

Apply the same backend branch in `detect_hierarchy` (use `max_levels=max_levels` in `_leiden_rows`), and guard its `finally:` drop the same way. Everything after `rows` (`_group_by_levels`, `_read_old_reports`, carry-forward, persistence) is UNCHANGED.

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/test_graph/test_community_backend_switch.py tests/test_workflow/test_search_community.py -q`
Expected: the new test PASSES; pre-existing `test_search_community` results unchanged from baseline (note: 2 pre-existing failures on stale `gamma`/`concurrency` stubs are NOT caused by this task — confirm they are the same 2).

- [ ] **Step 7: Commit**

```bash
git add src/graph/communities.py tests/test_graph/test_community_backend_switch.py
git commit -m "feat(community): route detect_communities/detect_hierarchy through community_backend"
```

---

### Task 6: Orchestration hardening (heartbeat + no blind OOM retry)

**Files:**
- Modify: `src/workflow/search/community_wf.py:110-117` (detect activity invocation)
- Modify: `src/workflow/search/activities/community.py:235-299` (heartbeat during clustering)
- Create: `src/workflow/search/_retry.py` addition — a no-retry-on-resource policy
- Test: `tests/test_workflow/test_community_build_hardening.py`

**Interfaces:**
- Consumes: existing `FAST_RETRY`; `temporalio.common.RetryPolicy`.
- Produces: `DETECT_RETRY` (RetryPolicy with `non_retryable_error_types=["MemoryError"]`) in `_retry.py`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_workflow/test_community_build_hardening.py`:
```python
from src.workflow.search._retry import DETECT_RETRY


def test_detect_retry_marks_memory_errors_non_retryable():
    assert "MemoryError" in (DETECT_RETRY.non_retryable_error_types or [])
    # still allows a couple of transient retries
    assert DETECT_RETRY.maximum_attempts and DETECT_RETRY.maximum_attempts <= 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_workflow/test_community_build_hardening.py -q`
Expected: FAIL (`ImportError: cannot import name 'DETECT_RETRY'`).

- [ ] **Step 3: Add DETECT_RETRY**

In `src/workflow/search/_retry.py`, append:
```python
# Detect-communities is heavy and resource-bound: a true OOM/resource error
# will recur on retry and only pile load onto Neo4j/the worker, so it is
# non-retryable.  Transient transport errors still get a couple of tries.
DETECT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=2,
    non_retryable_error_types=["MemoryError"],
)
```

- [ ] **Step 4: Use it + add a heartbeat_timeout in the workflow**

In `src/workflow/search/community_wf.py`, import `DETECT_RETRY` (add to the existing `_retry` import) and change the detect invocation:
```python
        detect: DetectCommunitiesResult = await workflow.execute_activity(
            "detect_communities_activity",
            params,
            result_type=DetectCommunitiesResult,
            start_to_close_timeout=timedelta(minutes=60),
            heartbeat_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=90),
            retry_policy=DETECT_RETRY,
        )
```

- [ ] **Step 5: Heartbeat during the clustering run**

In `src/workflow/search/activities/community.py` `detect_communities_activity`, wrap the detect call in the existing heartbeater (`src/workflow/heartbeat.py` `heartbeat_every`) so the loop pulses while clustering runs in its thread:
```python
    from src.workflow.heartbeat import heartbeat_every

    store = _get_store()
    async with heartbeat_every(30.0, {"stage": "detect"}):
        if params.max_levels > 1:
            from src.graph.communities import detect_hierarchy
            communities = await detect_hierarchy(
                store, max_levels=params.max_levels, min_size=params.min_size,
                gamma=params.gamma, concurrency=params.concurrency,
            )
        else:
            from src.graph.communities import detect_communities
            communities = await detect_communities(
                store, min_size=params.min_size, level=params.level,
                gamma=params.gamma, concurrency=params.concurrency,
            )
```

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/test_workflow/test_community_build_hardening.py tests/test_workflow/ -q`
Expected: new test PASSES; no NEW failures vs baseline.

- [ ] **Step 7: Commit**

```bash
git add src/workflow/search/_retry.py src/workflow/search/community_wf.py src/workflow/search/activities/community.py tests/test_workflow/test_community_build_hardening.py
git commit -m "fix(community): heartbeat_timeout + non-retryable OOM for detect activity"
```

---

### Task 7: Strict-parity benchmark (gate for flipping the default)

**Files:**
- Create: `tests/eval/bench_community_backends.py`
- Create: `tests/eval/README` entry (append a short section)

**Interfaces:**
- Consumes: `detect_hierarchy` with both backends; a live Neo4j (skipped when unreachable, like `tests/test_storage/test_ingest_metrics.py`).

- [ ] **Step 1: Write the benchmark (runnable script + skip guard)**

Create `tests/eval/bench_community_backends.py`:
```python
"""Strict-parity benchmark: GDS vs leidenalg community detection.

Run: uv run python -m tests.eval.bench_community_backends
Skips (exit 0) when Neo4j is unreachable.  Reports modularity, community
count, size distribution, wall time, and peak memory per backend so the
default flip (community_backend -> leidenalg) is an evidence-based decision.
"""

from __future__ import annotations

import time
import tracemalloc

from src.config import settings
from src.graph.community_leiden import build_graph, extract_entity_edges
from src.workflow.search.activities.community import _get_store


def _modularity(edges, nodes, name_to_cid) -> float:
    import leidenalg as la
    g, names = build_graph(edges, nodes)
    membership = [int(name_to_cid.get(n, -1)) for n in names]
    part = la.RBConfigurationVertexPartition(
        g, initial_membership=membership,
        weights=g.es["weight"] if "weight" in g.es.attributes() else None,
    )
    return part.quality()


def main() -> int:
    store = _get_store()
    if store is None:
        print("Neo4j unreachable — benchmark skipped")
        return 0

    edges, nodes = extract_entity_edges(store)
    print(f"graph: {len(nodes)} entities / {len(edges)} edges")

    from src.graph.community_leiden import hierarchy_rows
    for label, fn in [("leidenalg", lambda: hierarchy_rows(
            edges, nodes, gamma=settings.temporal.community_leiden_gamma,
            max_levels=settings.agent.community_max_levels))]:
        tracemalloc.start()
        t0 = time.perf_counter()
        rows = fn()
        dt = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        coarsest = {r["name"]: r["ids"][-1] for r in rows}
        ncomm = len(set(coarsest.values()))
        mod = _modularity(edges, nodes, coarsest)
        print(f"[{label}] time={dt:.1f}s peak_rss~{peak/1e6:.0f}MB "
              f"communities={ncomm} modularity={mod:.4f}")
    # NB: GDS comparison is run separately by flipping community_backend=gds
    # and re-running the rebuild; this script measures the leidenalg side +
    # its modularity so it can be compared to the GDS modularity from logs.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Run it (offline → skips cleanly)**

Run: `uv run python -m tests.eval.bench_community_backends`
Expected (no Neo4j): prints "Neo4j unreachable — benchmark skipped", exit 0.

- [ ] **Step 3: Document the acceptance gate**

Append to `tests/eval/README` (or create a short `tests/eval/community_backends.md`):
```
## Community backend parity (gate for flipping TEMPORAL_COMMUNITY_BACKEND -> leidenalg)
Run bench_community_backends.py against a representative Neo4j. Flip the
default to leidenalg only when, vs GDS: modularity is within ~5% on the
coarsest level, community count/size distribution is not pathologically
different, AND Neo4j peak heap during detection drops materially.
Until then community_backend stays "gds".
```

- [ ] **Step 4: Commit**

```bash
git add tests/eval/bench_community_backends.py tests/eval/community_backends.md
git commit -m "test(eval): GDS vs leidenalg community-detection parity benchmark"
```

---

## Post-implementation (NOT a code task — operator decision)

After Task 7's benchmark passes on real data, flip the default by setting
`TEMPORAL_COMMUNITY_BACKEND=leidenalg` (env) — or change the `community_backend`
default in `src/config.py` — in a separate, reviewed change. Do not flip the
default as part of this plan.

## Out of scope (future)

- Incremental hierarchy maintenance (assign new entities cheaply; rare full recompute).
- Partitioning / distributed clustering when a single worker's RAM is exceeded.

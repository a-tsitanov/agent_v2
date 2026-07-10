# GraphScope community backend (Phase 4 slice) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `community_backend="graphscope"` (single-level Leiden via GraphScope, distributed) producing the same rows as `leidenalg`, plus a benchmark arm vs leidenalg. Default unchanged; opt-in. The GraphScope-specific call is isolated behind one adapter function (mocked in unit tests; finalized against the installed GraphScope at a manual gate).

**Architecture:** Mirror the existing igraph offload (`community_leiden.py` + the `communities.py::_leiden_rows` branch). `community_graphscope.py` has one GraphScope-touching adapter (`_run_graphscope_community`) and a pure mapping (`single_level_rows_graphscope`). Everything except the real GraphScope run is DB-free/GraphScope-free unit-testable.

**Tech Stack:** Python, GraphScope (lazy, manual-gate), pytest. Reuses `community_leiden.extract_entity_edges`.

## Global Constraints

- **Default path unchanged:** `graphscope` is opt-in; `gds`/`leidenalg` byte-for-byte unchanged; default `community_backend` unchanged.
- GraphScope import is LAZY (inside `_run_graphscope_community` only). Unit tests mock `_run_graphscope_community` — no GraphScope install needed. Fail-safe: graphscope errors → `[]` (logged), matching the leidenalg branch.
- Local commits only. Never stage `docs/bruno/collection.bru`.

## File Structure
- Modify `src/config.py` (Literal), `scripts/make_env.py` (env doc).
- Create `src/graph/community_graphscope.py`.
- Modify `src/graph/communities.py` (`_graphscope_rows` + branch).
- Modify `tests/eval/bench_community_backends.py` (graphscope arm).
- Tests: `tests/test_graph/test_community_graphscope.py`, extend `tests/test_graph/test_communities.py`.

---

### Task 1: config + `community_graphscope.py` (adapter + mapping)

**Files:** Modify `src/config.py`, `scripts/make_env.py`; create `src/graph/community_graphscope.py`; test `tests/test_graph/test_community_graphscope.py`.

**Interfaces produced:**
- `_run_graphscope_community(edges, node_names, *, gamma, seed) -> dict[str, str]` (name→communityId; GraphScope-touching, lazy import).
- `single_level_rows_graphscope(edges, node_names, *, gamma, seed=19) -> list[dict]` → `[{name, communityId, ids:[cid]}]`.

- [ ] **Step 1: Write the failing test (mocks the GraphScope adapter)**

```python
# tests/test_graph/test_community_graphscope.py
"""single_level_rows_graphscope maps a mocked GraphScope partition to rows."""
from __future__ import annotations

import src.graph.community_graphscope as cg


def test_single_level_rows_maps_membership(monkeypatch):
    # Mock the only GraphScope-touching function with a canned partition.
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {"A": "0", "B": "0", "C": "1"})
    edges = [("A", "B", 1.0), ("B", "C", 1.0)]
    rows = cg.single_level_rows_graphscope(edges, ["A", "B", "C"], gamma=1.0)
    by = {r["name"]: r for r in rows}
    assert by["A"] == {"name": "A", "communityId": "0", "ids": ["0"]}
    assert by["C"]["communityId"] == "1" and by["C"]["ids"] == ["1"]


def test_rows_cover_edge_endpoint_names_and_default(monkeypatch):
    # A name only present in edges (not node_names) must still get a row;
    # a name absent from the membership map defaults to "0".
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {"A": "5"})
    rows = cg.single_level_rows_graphscope([("A", "Z", 1.0)], ["A"], gamma=1.0)
    names = {r["name"] for r in rows}
    assert names == {"A", "Z"}                 # Z recovered from the edge
    assert {r["name"]: r["communityId"] for r in rows}["Z"] == "0"  # default
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_graphscope.py -q`
Expected: FAIL (`No module named 'src.graph.community_graphscope'`).

- [ ] **Step 3: Add the config value**

`src/config.py:386`: `community_backend: Literal["gds", "leidenalg", "graphscope"] = "gds"` (keep the current default value — only add `"graphscope"` to the Literal). Update the `TEMPORAL_COMMUNITY_BACKEND` description in `scripts/make_env.py::_ENV_DESCRIPTIONS` to mention `'graphscope' (distributed Leiden via GraphScope, off Neo4j/igraph)`.

- [ ] **Step 4: Create `community_graphscope.py`**

```python
# src/graph/community_graphscope.py
"""GraphScope community-detection backend (single-level Leiden, distributed).

Mirrors community_leiden.py's single-level entry, but runs on GraphScope so
detection scales off single-machine igraph / off Neo4j's GDS heap. The ONLY
GraphScope-touching code is `_run_graphscope_community`; everything else is
pure and unit-testable by mocking it. Selected via community_backend='graphscope'.
"""

from __future__ import annotations

from typing import Any

from loguru import logger


def _all_names(edges: list[tuple[str, str, float]], node_names: list[str]) -> list[str]:
    """Dedup names across node_names + edge endpoints (mirrors
    community_leiden.build_graph's name set)."""
    return list(dict.fromkeys(
        list(node_names) + [e[0] for e in edges] + [e[1] for e in edges],
    ))


def _run_graphscope_community(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int,
) -> dict[str, str]:
    """Build a GraphScope graph from `edges` and run its modularity community
    algorithm; return {entity_name -> communityId}.

    NOTE (manual-gate): the exact GraphScope API — session bootstrap, graph
    load, and the algorithm call (native `leiden` if available, else
    `louvain`) — is finalized against the INSTALLED GraphScope on the cluster.
    Import is lazy so nothing here is needed for the DB-free unit tests (which
    mock this whole function). Fail-open: any error -> {} (caller yields []).
    """
    try:
        import graphscope  # noqa: F401  (lazy; heavy cluster dep)
        # --- finalize against installed GraphScope at the manual gate ---
        # sess = graphscope.session(cluster_type=...)
        # g = sess.load_from(edges=...)  # weighted undirected
        # ctx = graphscope.<leiden|louvain>(g, resolution=gamma, ...)
        # return {name: str(cid) for name, cid in ctx.to_dataframe(...)...}
        raise NotImplementedError(
            "GraphScope community call not finalized — complete against the "
            "installed GraphScope on the cluster (manual gate).",
        )
    except Exception as exc:
        logger.warning("graphscope community run failed: {e}", e=exc)
        return {}


def single_level_rows_graphscope(
    edges: list[tuple[str, str, float]], node_names: list[str],
    *, gamma: float, seed: int = 19,
) -> list[dict]:
    """Flat GraphScope partition -> rows [{name, communityId, ids:[cid]}]
    (same shape as community_leiden.single_level_rows)."""
    membership = _run_graphscope_community(edges, node_names, gamma=gamma, seed=seed)
    rows: list[dict] = []
    for name in _all_names(edges, node_names):
        cid = str(membership.get(name, "0"))
        rows.append({"name": name, "communityId": cid, "ids": [cid]})
    return rows
```

- [ ] **Step 5: Run tests + ruff**

`API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_graphscope.py tests/test_scripts/test_make_env.py -q` → PASS. `.venv/bin/python -m ruff check src/graph/community_graphscope.py src/config.py` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/graph/community_graphscope.py src/config.py scripts/make_env.py tests/test_graph/test_community_graphscope.py
git commit -m "feat(community): GraphScope community backend (single-level, adapter-isolated)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `communities.py` dispatch (`_graphscope_rows` + branch)

**Files:** Modify `src/graph/communities.py`; test extend `tests/test_graph/test_communities.py`.

**Interfaces:** `_graphscope_rows(store, *, gamma, seed=19) -> list[dict]`; `detect_communities` gains an `elif community_backend == "graphscope"` branch (fail-safe → `[]`).

- [ ] **Step 1: Write the failing test** — mirror the existing leidenalg-branch test in `tests/test_graph/test_communities.py` (read it first). Set `settings.temporal.community_backend="graphscope"` (monkeypatch), monkeypatch `communities.extract_entity_edges` → canned `(edges, names)` and `communities.single_level_rows_graphscope` → canned rows, call `detect_communities` with a fake store, assert it used the graphscope path (returned rows flowed to write-back) and is fail-safe (raising `single_level_rows_graphscope` → `[]`). Match the existing test's fake-store/monkeypatch conventions.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — add near `_leiden_rows` (`communities.py:390`):

```python
async def _graphscope_rows(store: Any, *, gamma: float, seed: int = 19) -> list[dict]:
    """GraphScope backend: stream edges + distributed single-level Leiden.
    Same rows shape as _leiden_rows ([{name, communityId, ids}])."""
    from src.graph.community_graphscope import single_level_rows_graphscope

    edges, names = await asyncio.to_thread(extract_entity_edges, store)
    return await asyncio.to_thread(
        single_level_rows_graphscope, edges, names, gamma=gamma, seed=seed,
    )
```

In `detect_communities`, extend the backend branch (`communities.py:435`):

```python
    if settings.temporal.community_backend == "leidenalg":
        try:
            rows = await _leiden_rows(store, gamma=gamma, max_levels=1)
            stats = {"nodes": len({r["name"] for r in rows}), "rels": -1}
        except Exception as exc:
            logger.error("communities: leidenalg detection FAILED: {e}", e=exc)
            return []
    elif settings.temporal.community_backend == "graphscope":
        try:
            rows = await _graphscope_rows(store, gamma=gamma)
            stats = {"nodes": len({r["name"] for r in rows}), "rels": -1}
        except Exception as exc:
            logger.error("communities: graphscope detection FAILED: {e}", e=exc)
            return []
    else:
        # ... unchanged GDS path ...
```

(If `detect_hierarchy` also has a leidenalg branch and hierarchy is in scope later, leave it GDS/leidenalg-only for now — this slice is single-level via `detect_communities`.)

- [ ] **Step 4:** GREEN (new test + existing communities tests green — GDS/leidenalg paths unchanged) + ruff clean.
- [ ] **Step 5:** Commit `feat(community): route detect_communities to the graphscope backend`.

---

### Task 3: benchmark arm

**Files:** Modify `tests/eval/bench_community_backends.py`.

- [ ] **Step 1:** Read the current bench. Add a graphscope arm that builds the SAME synthetic/representative graph + seed as the leidenalg arm, runs `single_level_rows_graphscope`, and reports build time + `_modularity(edges, nodes, name_to_cid)` + community parity (NMI or ARI vs the leidenalg single-level `name->cid`). Skip cleanly (`status="skipped"`/print) when GraphScope is unavailable (mirror how the file handles a missing backend). Keep the leidenalg/GDS arms unchanged.

- [ ] **Step 2:** Static-validate: `py_compile` + `import`-clean (no GraphScope at import — it's lazy in `_run_graphscope_community`). Do NOT run the graphscope arm (needs a cluster).

- [ ] **Step 3:** Commit `test(community): graphscope arm in the community-backend parity bench`.

**Manual gate (controller/user):** on a GraphScope cluster — finalize `_run_graphscope_community` against the installed GraphScope API (session bootstrap, graph load, `leiden`/`louvain` call), then run the bench arm on a representative graph and compare modularity/NMI/time vs leidenalg. That evidence decides whether to flip `community_backend`.

---

## Self-Review

**Spec coverage:** config (T1), adapter+mapping (T1), dispatch (T2), bench (T3). Default `gds`/`leidenalg` unchanged. Hierarchy, direct-read, centralities, default-flip all deferred per spec.

**Placeholder scan:** the ONE deliberate `NotImplementedError` is the GraphScope adapter body — explicitly a manual-gate finalize (the spec/plan make this a documented boundary, not a hidden TODO); everything around it is complete + unit-tested by mocking it. No other placeholders.

**Type consistency:** `single_level_rows_graphscope` returns `[{name, communityId, ids:[cid]}]` — identical to `community_leiden.single_level_rows` and what `communities.py` write-back consumes. `_graphscope_rows` mirrors `_leiden_rows`. `_run_graphscope_community` returns `{name->cid}`, mapped by `single_level_rows_graphscope` and mocked in every unit test.

**Manual-gate items:** the real GraphScope API (algorithm name, hierarchy, session) + the distributed bench run — the ONLY parts not exercisable in-session (GraphScope is a cluster dep).

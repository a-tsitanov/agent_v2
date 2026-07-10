# Phase 4 (slice) — distributed community-detection via GraphScope, benchmark-first

**Status:** approved 2026-07-11. Sub-project of the NebulaGraph migration (Phase 4 = distributed graph-compute replacing GDS+igraph). Branch `feat/community-graphscope` off `main`.

## Goal

Add a `community_backend="graphscope"` option that runs **single-level** Leiden community-detection on GraphScope (distributed — off single-machine igraph and off Neo4j's JVM/GDS), producing the same rows shape as the `leidenalg` backend, plus a benchmark comparing it to `leidenalg`. This is **store-agnostic** — it pays off on the current Neo4j prod immediately (the recurring GDS-OOM pain), and is the distributed-compute foundation for billion-scale (the migration's dominant workload). The distributed run + benchmark are a **manual gate** on a GraphScope cluster.

## Background (current seam, grounded)

- `community_backend: Literal["gds","leidenalg"]` (`src/config.py:386`, exposed as `settings.temporal.community_backend`).
- `communities.py::detect_communities` (`:410`) branches: `if community_backend == "leidenalg": rows = await _leiden_rows(store, gamma=, max_levels=1)` (`:435-441`), else GDS `gds.leiden.stream`.
- `_leiden_rows` (`:390`): `edges, names = extract_entity_edges(store)`; then `single_level_rows(edges, names, gamma=, seed=)` (max_levels==1) → `[{name, communityId, ids:[cid]}]`.
- `community_leiden.py`: `extract_entity_edges(store) -> (edges, names)` (keyset-paginated Cypher export); `build_graph` (igraph); `single_level_rows(edges, names, *, gamma, seed=19)` (flat `la.find_partition(RBConfigurationVertexPartition)`); `hierarchy_rows` (multi-level).
- `tests/eval/bench_community_backends.py`: measures leidenalg modularity/timing (`hierarchy_rows`), GDS run separately by flipping the flag; `_modularity(edges, nodes, name_to_cid)`.

## Global Constraints

- **Default path unchanged.** `graphscope` is opt-in; `gds`/`leidenalg` behavior byte-for-byte unchanged. Default `community_backend` stays as-is.
- **Benchmark before adopting** (project policy): the graphscope arm produces the evidence to flip the flag; do not change the default.
- GraphScope is a heavy, cluster-oriented dep — its import is LAZY (only under `backend=graphscope`), and the GraphScope-specific call is isolated behind a thin adapter so unit tests mock it (no GraphScope install needed for CI). The real distributed run is a manual gate.
- Fail-safe: any graphscope error → `[]` (logged, never raised), matching the leidenalg branch. Local commits only. Never stage `docs/bruno/collection.bru`.

## Design

### 1. Config

`community_backend: Literal["gds","leidenalg","graphscope"]` (`config.py:386`). Default unchanged. Env doc for the new value updated in `scripts/make_env.py` (the `TEMPORAL_COMMUNITY_BACKEND` description).

### 2. `src/graph/community_graphscope.py` (new)

Mirrors `community_leiden.py`'s single-level entry, with the GraphScope call isolated:

```python
def _run_graphscope_community(edges, node_names, *, gamma, seed) -> dict[str, str]:
    """GraphScope-specific: build a distributed graph from `edges` and run
    GraphScope's modularity community algorithm; return {name -> communityId}.
    Isolated so unit tests mock it and the exact GraphScope API (algorithm
    name, hierarchy, session bootstrap) is finalized against the installed
    GraphScope at the manual gate. Lazy `import graphscope` inside."""

def single_level_rows_graphscope(edges, node_names, *, gamma, seed=19) -> list[dict]:
    """Same shape as community_leiden.single_level_rows, via GraphScope:
    [{name, communityId, ids:[communityId]}]. Reuses the caller's edges/names
    (from extract_entity_edges)."""
    membership = _run_graphscope_community(edges, node_names, gamma=gamma, seed=seed)
    return [{"name": n, "communityId": membership.get(n, "0"),
             "ids": [membership.get(n, "0")]} for n in <names incl. edge endpoints>]
```

`_run_graphscope_community` is the only GraphScope-touching function; `single_level_rows_graphscope`'s mapping logic is fully unit-testable by mocking it.

**Algorithm note (manual-gate verification):** GraphScope's GAE has Louvain/LPA built in; native **Leiden** availability + hierarchy must be confirmed against the installed GraphScope. If Leiden isn't a builtin, `_run_graphscope_community` uses GraphScope's closest modularity algorithm (Louvain — Leiden's predecessor); the benchmark then measures how close it lands to leidenalg (modularity + NMI/ARI). This is exactly what the benchmark-first gate decides.

### 3. `communities.py` dispatch

Add `_graphscope_rows(store, *, gamma, seed)` (parallel to `_leiden_rows`): `edges, names = await asyncio.to_thread(extract_entity_edges, store)`; `return await asyncio.to_thread(single_level_rows_graphscope, edges, names, gamma=gamma, seed=seed)`. In `detect_communities`, add `elif community_backend == "graphscope": rows = await _graphscope_rows(...)` (fail-safe → `[]`), parallel to the leidenalg branch. Write-back/prune/report-carry unchanged.

### 4. Benchmark (adoption gate)

Extend `tests/eval/bench_community_backends.py` with a graphscope arm: same synthetic/representative graph + seed, run `single_level_rows_graphscope`, report build time + `_modularity` + community parity (NMI/ARI vs the leidenalg single-level partition). Skips cleanly when GraphScope is unavailable (like the other arms).

### 5. Tests + manual gate

- DB-free AND GraphScope-free unit tests: mock `_run_graphscope_community` to a canned `{name->cid}`, assert `single_level_rows_graphscope` maps to the correct rows (incl. edge-endpoint names, default cid), and that the `communities.py` graphscope branch calls the graphscope path + is fail-safe.
- **Manual gate (controller/user):** on a GraphScope cluster, finalize `_run_graphscope_community` against the real API, run the bench arm, compare modularity/NMI/time vs leidenalg on a representative graph. GraphScope setup cost is real (pip + vineyard/k8s session) — heavier than the Milvus/nebula gates.

## Out of scope (deferred)

- **Hierarchical** (multi-level dendrogram) Leiden on GraphScope — single-level first; recursive-per-level or native-hierarchy is a follow-up.
- **Direct graph-read** into GraphScope — the slice reuses `extract_entity_edges` (worker-side streaming, itself a bottleneck at true billion scale); a direct dump/connector read is the real-scale optimization.
- **Centralities** (`analysis.py`/`materialize.py` GDS pagerank/betweenness/eigenvector → distributed) — separate seam.
- Flipping the default `community_backend` to graphscope — only after the benchmark passes.

## Interfaces produced

- `src/config.py`: `community_backend` Literal gains `"graphscope"`.
- `src/graph/community_graphscope.py`: `_run_graphscope_community`, `single_level_rows_graphscope`.
- `src/graph/communities.py`: `_graphscope_rows` + the `detect_communities` branch.
- `tests/eval/bench_community_backends.py`: graphscope arm.

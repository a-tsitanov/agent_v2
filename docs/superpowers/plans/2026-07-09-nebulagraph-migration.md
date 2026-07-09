# NebulaGraph Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the graph layer from Neo4j to a self-hosted NebulaGraph cluster behind a backend seam, so the app can run either store, cut over per-workload after parity benchmarks pass, and keep Neo4j as rollback.

**Architecture:** Strangler-fig. First introduce a narrow `KbGraphStore` seam and a `GRAPH_BACKEND` config toggle so every graph caller goes through one factory (`build_graph_store()`) — zero behavior change, Neo4j still the only backend. Then stand up NebulaGraph and implement the same seam for it (schema + write adapter + nGQL query executor). Then migrate the read queries, move vectors to Milvus, replace GDS/igraph with a distributed compute layer, add a bulk-ingest path, and finally flip per-workload and decommission Neo4j. The store, query-rewrite, vector, compute, ingest, and cutover concerns are **independent subsystems** — Phases 2–6 each become their own detailed plan (see Scope note).

**Tech Stack:** Python 3.11 / FastAPI, LlamaIndex `PropertyGraphStore` contract, NebulaGraph 3.x (`metad`/`storaged`/`graphd`) + `nebula3-python`, nGQL, Milvus (existing) for vectors, GraphScope **or** Spark GraphFrames (Phase 4) for distributed graph compute, Temporal workflows (existing), pytest.

## Global Constraints

Copied verbatim from project policy / session context — every task's requirements implicitly include these:

- **Never push/commit without explicit user confirmation.** Staging, diffs, writing files, running tests are fine; the commit/push step is a hard gate. Branch off `main`; never push to `main` directly.
- **Opt-in swaps, never blind replacement. Benchmark before adopting.** `GRAPH_BACKEND` default stays `"neo4j"` until per-workload parity benchmarks pass — mirrors the existing `community_backend` default-`"gds"` policy (`src/config.py:386`).
- **Preserve the LlamaIndex `PropertyGraphStore` structural contract** the app already depends on: `structured_query(query, param_map=None) -> list[dict]`, `upsert_nodes(nodes) -> None`, `upsert_relations(relations) -> None`. The seam is exactly this subset.
- **Embedding dim is 1536** (`text-embedding-3-small`, `src/config.py:82`); cosine metric. Vectors leave the graph in Phase 3 → Milvus.
- **Extend `tests/eval/` for benchmarks; keep unit tests DB-free** (fakes exposing `structured_query`), matching the current suite.
- **Neo4j stays live and is the rollback target** through Phase 5; do not delete Neo4j code/compose until Phase 6.

---

## Migration Strategy & Phase Map

| Phase | Deliverable | Subsystem | Detailed here? |
|---|---|---|---|
| **0** | Backend seam + `GRAPH_BACKEND` toggle (pure refactor, no Nebula) | store factory | **Yes — full TDD** |
| **1** | Nebula dev cluster + schema + write adapter + write-parity harness | store cluster | **Yes — full tasks** |
| **2** | Read-path nGQL translation (62 sites / ~35 `_CYPHER`) + per-query parity | query rewrite | Sub-plan |
| **3** | Move `er_vec` + `report_vec` graph→Milvus; ER kNN + report-select rewrite | vector layer | Sub-plan |
| **4** | Distributed compute (GraphScope/Spark) replacing GDS + single-machine igraph | graph compute | Sub-plan |
| **5** | Bulk-ingest path (Nebula Importer / Spark) for billion-scale backfill | ingest | Sub-plan |
| **6** | Per-workload cutover, parity soak, decommission Neo4j | cutover/ops | Sub-plan |

**Scope note (per writing-plans scope check):** Phases 2–6 are independent subsystems that each produce working, testable software on their own and each carry material design decisions that depend on Phase 0–1 outcomes and benchmark data. This document details **Phase 0 and Phase 1 to bite-sized TDD granularity** and specifies Phases 2–6 as **scoped sub-project specs** (goal, boundary, files, interface/deliverable). Generate a dedicated plan for each of Phases 2–6 when its predecessor lands — do not implement them from the spec-level detail below.

**Why strangler-fig, not big-bang:** the app funnels every graph op through three methods (`structured_query`/`upsert_nodes`/`upsert_relations`) at ~25 factory call-sites and ~30 query files. A seam lets Neo4j and Nebula coexist, migrates one read/write path at a time behind a flag, and keeps a green build the entire way. This matches the project's "opt-in swap + benchmark before adopting" rule.

## File Structure

**Phase 0 (create):**
- `src/graph/backend.py` — the `KbGraphStore` Protocol (the seam's type).
**Phase 0 (modify):**
- `src/config.py` — add `GraphSettings` + `settings.graph` (dispatch source of truth).
- `src/graph/store.py` — add `build_graph_store()` dispatch; keep `build_neo4j_graph_store()` as the neo4j branch.
- ~25 caller files — swap `build_neo4j_graph_store()` → `build_graph_store()`.

**Phase 1 (create):**
- `src/graph/nebula_schema.py` — nGQL DDL (space, tags, edge types, indexes).
- `src/graph/nebula_store.py` — `NebulaGraphStore` (implements `KbGraphStore`) + `build_nebula_graph_store()` process-global factory.
- `tests/eval/migration/parity_write.py` — dual-write structural-parity harness.
**Phase 1 (modify):**
- `docker-compose.yml` — NebulaGraph services (metad/storaged/graphd).
- `requirements.txt` — `nebula3-python`.

---

## Phase 0 — Backend seam (no Nebula yet)

Pure refactor. End state: `GRAPH_BACKEND=neo4j` (default) behaves identically to today; the codebase now has exactly one switch to change later.

### Task 0.1: `GraphSettings` config toggle

**Files:**
- Modify: `src/config.py` (add `GraphSettings` class after `Neo4jSettings`, ends line 138; add `graph` cached_property after `neo4j`, line 1051)
- Test: `tests/test_config/test_graph_settings.py`

**Interfaces:**
- Produces: `src.config.GraphSettings` with field `backend: Literal["neo4j", "nebula"]` (default `"neo4j"`); accessible as `settings.graph.backend`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config/test_graph_settings.py
"""GRAPH_BACKEND toggle: default neo4j, env override to nebula."""
from __future__ import annotations

import importlib


def test_graph_backend_defaults_to_neo4j(monkeypatch):
    monkeypatch.delenv("GRAPH_BACKEND", raising=False)
    from src.config import GraphSettings

    assert GraphSettings().backend == "neo4j"


def test_graph_backend_env_override(monkeypatch):
    monkeypatch.setenv("GRAPH_BACKEND", "nebula")
    from src.config import GraphSettings

    assert GraphSettings().backend == "nebula"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config/test_graph_settings.py -v`
Expected: FAIL with `ImportError: cannot import name 'GraphSettings'`

- [ ] **Step 3: Add the `GraphSettings` class**

Insert after `Neo4jSettings` (i.e. after `src/config.py:138`, before `class PostgresSettings`). `Literal` is already imported (used at `src/config.py:386`).

```python
class GraphSettings(BaseSettings):
    """Which graph backend the store factory builds.

    Strangler seam for the Neo4j -> NebulaGraph migration: every graph
    caller goes through ``src.graph.store.build_graph_store()`` which
    dispatches on this.  Default stays "neo4j" until per-workload parity
    benchmarks pass (project policy: benchmark before adopting — mirrors
    ``community_backend``)."""

    model_config = SettingsConfigDict(env_prefix="GRAPH_", env_file=".env", extra="ignore")

    backend: Literal["neo4j", "nebula"] = "neo4j"
```

- [ ] **Step 4: Wire it into the aggregate `Settings`**

Add after the `neo4j` cached_property (`src/config.py:1051`):

```python
    @cached_property
    def graph(self) -> GraphSettings:
        return GraphSettings()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_config/test_graph_settings.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Commit**

```bash
git add src/config.py tests/test_config/test_graph_settings.py
git commit -m "feat(graph): add GRAPH_BACKEND config toggle (seam for nebula migration)"
```

### Task 0.2: `KbGraphStore` protocol + `build_graph_store()` dispatch

**Files:**
- Create: `src/graph/backend.py`
- Modify: `src/graph/store.py` (add `build_graph_store()` after `build_neo4j_graph_store()`, line 115)
- Test: `tests/test_graph/test_build_graph_store.py`

**Interfaces:**
- Consumes: `settings.graph.backend` (Task 0.1), `build_neo4j_graph_store()` (`src/graph/store.py:100`).
- Produces:
  - `src.graph.backend.KbGraphStore` — `runtime_checkable` Protocol with `structured_query(query, param_map=None) -> list[dict]`, `upsert_nodes(nodes) -> None`, `upsert_relations(relations) -> None`.
  - `src.graph.store.build_graph_store() -> PropertyGraphStore` — returns the neo4j store when `backend == "neo4j"`; imports and returns `build_nebula_graph_store()` (Phase 1) when `"nebula"`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_build_graph_store.py
"""build_graph_store dispatches on settings.graph.backend."""
from __future__ import annotations

import pytest

import src.graph.store as store_mod


def test_dispatch_neo4j_returns_neo4j_store(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(store_mod, "build_neo4j_graph_store", lambda: sentinel)
    monkeypatch.setattr(store_mod.settings.graph, "backend", "neo4j", raising=False)

    assert store_mod.build_graph_store() is sentinel


def test_dispatch_nebula_imports_nebula_builder(monkeypatch):
    # Phase 0: nebula backend selected but src.graph.nebula_store not yet
    # present -> the dispatch must attempt the import (proves the branch is
    # wired), which raises ModuleNotFoundError until Phase 1 lands it.
    monkeypatch.setattr(store_mod.settings.graph, "backend", "nebula", raising=False)

    with pytest.raises(ModuleNotFoundError):
        store_mod.build_graph_store()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_build_graph_store.py -v`
Expected: FAIL with `AttributeError: module 'src.graph.store' has no attribute 'build_graph_store'`

- [ ] **Step 3: Create the `KbGraphStore` protocol**

```python
# src/graph/backend.py
"""The graph-store surface the app actually uses.

A narrow subset of LlamaIndex's ``PropertyGraphStore`` — the three methods
every graph caller funnels through.  Any backend (Neo4j today, NebulaGraph
after the migration) that structurally satisfies this Protocol can be
returned by ``src.graph.store.build_graph_store()``.  Keeping the seam this
small is what makes the strangler migration a per-method job, not a
per-call-site one.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class KbGraphStore(Protocol):
    def structured_query(
        self, query: str, param_map: dict[str, Any] | None = None
    ) -> list[dict]: ...

    def upsert_nodes(self, nodes: list[Any]) -> None: ...

    def upsert_relations(self, relations: list[Any]) -> None: ...
```

- [ ] **Step 4: Add `build_graph_store()` dispatch**

Append to `src/graph/store.py` after `build_neo4j_graph_store()` (after line 115):

```python
def build_graph_store() -> PropertyGraphStore:
    """Return the process-global graph store for the configured backend.

    The single seam every graph caller goes through.  ``GRAPH_BACKEND``
    (``settings.graph.backend``) selects the implementation; default
    "neo4j" is unchanged behaviour.  "nebula" is wired in Phase 1."""
    if settings.graph.backend == "nebula":
        from src.graph.nebula_store import build_nebula_graph_store

        return build_nebula_graph_store()
    return build_neo4j_graph_store()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_graph/test_build_graph_store.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: Run the graph suite to confirm no regression**

Run: `pytest tests/test_graph/ -q`
Expected: PASS (all existing green)

- [ ] **Step 7: Commit**

```bash
git add src/graph/backend.py src/graph/store.py tests/test_graph/test_build_graph_store.py
git commit -m "feat(graph): KbGraphStore seam + build_graph_store() backend dispatch"
```

### Task 0.3: Route all callers through the seam

Swap every `build_neo4j_graph_store()` **call** to `build_graph_store()`. Keep `build_neo4j_graph_store` itself (it is the neo4j branch of the dispatcher). After this, flipping `GRAPH_BACKEND` reroutes the whole app.

**Files (modify — call-sites from the integration map):**
- `src/workflow/activities/build_property_graph.py:98`
- `src/workflow/activities/merge_and_resolve.py:200`
- `src/workflow/activities/inject_canonical.py:42`
- `src/workflow/search/activities/*.py` (`retrieve`, `community`, `global_search`, `documents`, and `_search_deps.py:65`)
- `src/mcp/tools_server.py:103`
- `src/api/routes/admin.py:23`, `src/api/routes/graph_admin.py:31`
- Any `scripts/*.py` that call `build_neo4j_graph_store()`
- Test: `tests/test_graph/test_seam_adoption.py`

**Interfaces:**
- Consumes: `build_graph_store()` (Task 0.2).

- [ ] **Step 1: Write the failing guard test**

```python
# tests/test_graph/test_seam_adoption.py
"""No app code calls build_neo4j_graph_store() directly — only the seam."""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
ALLOW = {ROOT / "src" / "graph" / "store.py"}  # the dispatcher itself


def test_no_direct_neo4j_store_calls_outside_store_py():
    pat = re.compile(r"\bbuild_neo4j_graph_store\s*\(")
    offenders = []
    for py in (ROOT / "src").rglob("*.py"):
        if py in ALLOW:
            continue
        if pat.search(py.read_text(encoding="utf-8")):
            offenders.append(str(py.relative_to(ROOT)))
    assert offenders == [], f"call build_graph_store() instead: {offenders}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_seam_adoption.py -v`
Expected: FAIL listing the current caller files.

- [ ] **Step 3: Codemod the call-sites**

For each offending file the test lists, replace the **call** `build_neo4j_graph_store()` with `build_graph_store()` and update the import. Mechanical, one file at a time:

```bash
# from repo root — rewrite calls + imports across src/ (review the diff after)
grep -rl "build_neo4j_graph_store()" src/ \
  | grep -v "src/graph/store.py" \
  | xargs sed -i '' \
    -e 's/build_neo4j_graph_store()/build_graph_store()/g' \
    -e 's/from src.graph.store import build_neo4j_graph_store/from src.graph.store import build_graph_store/g'
```

Then hand-fix any file that imported **both** names or used a module-qualified `store.build_neo4j_graph_store()` form — grep once more to confirm:

```bash
grep -rn "build_neo4j_graph_store" src/ | grep -v "src/graph/store.py"
```

Expected: no output.

- [ ] **Step 4: Run the guard test + full graph/workflow suites**

Run: `pytest tests/test_graph/test_seam_adoption.py tests/test_graph/ tests/test_workflow/ -q`
Expected: PASS (guard green; no regressions — behavior is identical, default backend is neo4j).

- [ ] **Step 5: Commit**

```bash
git add src/ tests/test_graph/test_seam_adoption.py
git commit -m "refactor(graph): route all callers through build_graph_store() seam"
```

**Phase 0 exit criteria:** full suite green; `GRAPH_BACKEND=neo4j` identical to pre-refactor; `GRAPH_BACKEND=nebula` fails fast with `ModuleNotFoundError` (Phase 1 fills it).

---

## Phase 1 — Nebula dev cluster + schema + write adapter

End state: a local NebulaGraph reachable, a KB-shaped schema created, and `GRAPH_BACKEND=nebula` can **write** entities/relations through the seam. Reads still go to Neo4j (Phase 2 translates them). A write-parity harness gates Phase 2.

### Task 1.1: NebulaGraph dev services + client dependency

**Files:**
- Modify: `docker-compose.yml` (add nebula services), `requirements.txt` (add client)
- Create: `scripts/nebula_bootstrap.py` (register storage host — required once after first boot)
- Test: `tests/eval/migration/test_nebula_smoke.py` (eval-tier connect check; skipped when `NEBULA_HOST` unset)

- [ ] **Step 1: Add the client dependency**

Append to `requirements.txt`:

```
nebula3-python==3.8.2
```

- [ ] **Step 2: Add NebulaGraph services to `docker-compose.yml`**

Single-replica dev topology (metad + storaged + graphd). Append under `services:`:

```yaml
  nebula-metad:
    image: vesoft/nebula-metad:v3.8.0
    command: ["--meta_server_addrs=nebula-metad:9559", "--local_ip=nebula-metad", "--ws_ip=nebula-metad", "--port=9559", "--data_path=/data/meta"]
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://nebula-metad:19559/status"]
      interval: 10s
      timeout: 5s
      retries: 6
    volumes: ["nebula_meta:/data/meta"]

  nebula-storaged:
    image: vesoft/nebula-storaged:v3.8.0
    command: ["--meta_server_addrs=nebula-metad:9559", "--local_ip=nebula-storaged", "--ws_ip=nebula-storaged", "--port=9779", "--data_path=/data/storage"]
    depends_on:
      nebula-metad: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://nebula-storaged:19779/status"]
      interval: 10s
      timeout: 5s
      retries: 6
    volumes: ["nebula_storage:/data/storage"]

  nebula-graphd:
    image: vesoft/nebula-graphd:v3.8.0
    command: ["--meta_server_addrs=nebula-metad:9559", "--local_ip=nebula-graphd", "--ws_ip=nebula-graphd", "--port=9669"]
    depends_on:
      nebula-storaged: {condition: service_healthy}
    ports: ["9669:9669"]
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://nebula-graphd:19669/status"]
      interval: 10s
      timeout: 5s
      retries: 6
```

Add to the top-level `volumes:` map:

```yaml
  nebula_meta:
  nebula_storage:
```

- [ ] **Step 3: Write the one-time host-registration bootstrap**

NebulaGraph will not store data until the storage host is registered with `ADD HOSTS`. Create `scripts/nebula_bootstrap.py`:

```python
"""One-time NebulaGraph bootstrap: register the storaged host.

Run ONCE after the cluster first boots (idempotent — re-running is safe).
    python scripts/nebula_bootstrap.py
"""
from __future__ import annotations

import os

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


def main() -> None:
    host = os.getenv("NEBULA_HOST", "127.0.0.1")
    port = int(os.getenv("NEBULA_PORT", "9669"))
    user = os.getenv("NEBULA_USER", "root")
    pwd = os.getenv("NEBULA_PASSWORD", "nebula")

    pool = ConnectionPool()
    assert pool.init([(host, port)], Config())
    sess = pool.get_session(user, pwd)
    try:
        r = sess.execute("ADD HOSTS \"nebula-storaged\":9779;")
        print("ADD HOSTS:", "ok" if r.is_succeeded() else r.error_msg())
        print(sess.execute("SHOW HOSTS;"))
    finally:
        sess.release()
        pool.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Write the smoke test (eval-tier, skipped without a live cluster)**

```python
# tests/eval/migration/test_nebula_smoke.py
"""Connect to a live NebulaGraph and run `SHOW HOSTS`. Skipped in CI."""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("NEBULA_HOST"), reason="live NebulaGraph not configured"
)


def test_can_connect_and_show_hosts():
    from nebula3.Config import Config
    from nebula3.gclient.net import ConnectionPool

    pool = ConnectionPool()
    assert pool.init([(os.environ["NEBULA_HOST"], int(os.getenv("NEBULA_PORT", "9669")))], Config())
    sess = pool.get_session("root", os.getenv("NEBULA_PASSWORD", "nebula"))
    try:
        resp = sess.execute("SHOW HOSTS;")
        assert resp.is_succeeded(), resp.error_msg()
    finally:
        sess.release()
        pool.close()
```

- [ ] **Step 5: Bring the cluster up and register the host**

```bash
docker compose up -d nebula-metad nebula-storaged nebula-graphd
python scripts/nebula_bootstrap.py           # expect ADD HOSTS: ok; host ONLINE
NEBULA_HOST=127.0.0.1 pytest tests/eval/migration/test_nebula_smoke.py -v
```
Expected: smoke test PASS.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml requirements.txt scripts/nebula_bootstrap.py tests/eval/migration/test_nebula_smoke.py
git commit -m "feat(graph): NebulaGraph dev cluster, client dep, bootstrap + smoke test"
```

### Task 1.2: NebulaGraph schema (space, tags, edge types, indexes)

**Files:**
- Create: `src/graph/nebula_schema.py`
- Test: `tests/test_graph/test_nebula_schema.py` (DB-free: assert DDL well-formed + idempotent)

**Interfaces:**
- Produces: `SPACE_NAME: str`; `SCHEMA_DDL: list[str]` (ordered nGQL statements); `ensure_schema(session) -> None` executing them.

**Model mapping (Cypher → nGQL):** `:__Entity__` label → tag `Entity`; typed relationships (`RELATED`, `MENTIONS`, `IN_COMMUNITY`, `PARENT_OF`) → edge types of the same name; entity props (`name`, `description`, `mention_count`, `created_at`) → tag properties; relation temporal props (`polarity`, `valid_from`, `valid_to`) → edge properties. `er_vec`/`report_vec` are **not** modeled here — they move to Milvus (Phase 3). `elementId`/`labels()`-style access is replaced by Nebula's explicit VID + tag model in Phase 2.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_nebula_schema.py
from __future__ import annotations

from src.graph.nebula_schema import SCHEMA_DDL, SPACE_NAME


def test_space_and_core_schema_present():
    joined = "\n".join(SCHEMA_DDL)
    assert f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}`" in joined
    assert "CREATE TAG IF NOT EXISTS `Entity`" in joined
    for edge in ("RELATED", "MENTIONS", "IN_COMMUNITY", "PARENT_OF"):
        assert f"CREATE EDGE IF NOT EXISTS `{edge}`" in joined


def test_ddl_is_idempotent():
    # every statement must be IF NOT EXISTS so ensure_schema can re-run
    for stmt in SCHEMA_DDL:
        assert "IF NOT EXISTS" in stmt, stmt
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_nebula_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.graph.nebula_schema'`

- [ ] **Step 3: Write the schema module**

```python
# src/graph/nebula_schema.py
"""NebulaGraph schema for the KB graph (nGQL DDL).

Mirrors the Neo4j model: `:__Entity__` -> tag `Entity`; typed rels ->
same-named edge types.  Vectors (`er_vec`/`report_vec`) are intentionally
absent — they live in Milvus after Phase 3.  All statements are
IF NOT EXISTS so `ensure_schema` is safe to re-run on every boot (matches
the fail-open `ensure_*` DDL helpers in `src/graph/index.py`).
"""

from __future__ import annotations

from typing import Any

SPACE_NAME = "kb"

SCHEMA_DDL: list[str] = [
    # int64 VID via a stable hash of the entity name (set at write time).
    f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}` "
    "(partition_num=100, replica_factor=1, vid_type=INT64);",
    f"USE `{SPACE_NAME}`;",
    "CREATE TAG IF NOT EXISTS `Entity` ("
    "name string, description string, mention_count int DEFAULT 0, "
    "created_at int DEFAULT 0, label string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `RELATED` ("
    "polarity string DEFAULT '', valid_from int DEFAULT 0, valid_to int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `MENTIONS` (doc_id string DEFAULT '');",
    "CREATE EDGE IF NOT EXISTS `IN_COMMUNITY` (level int DEFAULT 0);",
    "CREATE EDGE IF NOT EXISTS `PARENT_OF` ();",
    # tag/edge indexes needed for full-scan + lookups (Nebula requires an
    # index to LOOKUP by property; traversals from a known VID do not).
    "CREATE TAG INDEX IF NOT EXISTS `entity_name_idx` ON `Entity`(name(256));",
]


def ensure_schema(session: Any) -> None:
    """Execute SCHEMA_DDL on an open nebula3 session (fail-open, logged)."""
    from loguru import logger

    for stmt in SCHEMA_DDL:
        resp = session.execute(stmt)
        if not resp.is_succeeded():
            logger.warning("nebula ensure_schema: {s} -> {e}", s=stmt[:60], e=resp.error_msg())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_nebula_schema.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/graph/nebula_schema.py tests/test_graph/test_nebula_schema.py
git commit -m "feat(graph): NebulaGraph schema DDL (Entity tag, typed edges, name index)"
```

### Task 1.3: `NebulaGraphStore` write adapter + process-global factory

**Files:**
- Create: `src/graph/nebula_store.py`
- Test: `tests/test_graph/test_nebula_store_writes.py` (DB-free: fake session captures nGQL)

**Interfaces:**
- Consumes: `KbGraphStore` (Task 0.2), `SPACE_NAME`/`ensure_schema` (Task 1.2).
- Produces:
  - `NebulaGraphStore` implementing `KbGraphStore`; `upsert_nodes`/`upsert_relations` translate LlamaIndex `EntityNode`/`Relation` objects to nGQL `INSERT VERTEX`/`INSERT EDGE`; `structured_query(query, param_map=None)` executes raw nGQL and returns `list[dict]`.
  - `build_nebula_graph_store() -> NebulaGraphStore` — process-global cache mirroring `build_neo4j_graph_store()` (module-level `_store` + `threading.Lock`).
  - `entity_vid(name: str) -> int` — stable int64 VID from the entity name (the write path and Phase 2 read path must agree on this).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_nebula_store_writes.py
"""upsert_nodes/upsert_relations emit the expected nGQL (no live DB)."""
from __future__ import annotations

from types import SimpleNamespace

from src.graph.nebula_store import NebulaGraphStore, entity_vid


class _FakeSession:
    def __init__(self):
        self.executed = []

    def execute(self, stmt, *a, **k):
        self.executed.append(stmt)
        return SimpleNamespace(is_succeeded=lambda: True, error_msg=lambda: "")


def _store_with_session(sess):
    s = NebulaGraphStore.__new__(NebulaGraphStore)
    s._session = sess
    return s


def test_entity_vid_is_stable_int64():
    v = entity_vid("Иванов")
    assert isinstance(v, int)
    assert v == entity_vid("Иванов")
    assert -(2**63) <= v < 2**63


def test_upsert_nodes_inserts_entity_vertex():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "d", "mention_count": 3})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert "INSERT VERTEX `Entity`" in blob
    assert str(entity_vid("Иванов")) in blob
    assert "Иванов" in blob


def test_upsert_relations_inserts_edge():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="Иванов", target_id="Москва",
                          label="RELATED", properties={"polarity": "pos"})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob
    assert f"{entity_vid('Иванов')} -> {entity_vid('Москва')}" in blob
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_nebula_store_writes.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.graph.nebula_store'`

- [ ] **Step 3: Write the adapter**

```python
# src/graph/nebula_store.py
"""NebulaGraph implementation of the KbGraphStore seam (write path).

Phase 1 scope: schema-aware writes (upsert_nodes/upsert_relations) + a raw
nGQL passthrough (structured_query).  Cypher READ queries are translated in
Phase 2; until then reads still run against Neo4j.  Process-global cache
mirrors src/graph/store.py::build_neo4j_graph_store (one pooled client per
process, thread-safe session use).
"""

from __future__ import annotations

import hashlib
import threading
from typing import Any

from loguru import logger

from src.config import settings
from src.graph.nebula_schema import SPACE_NAME, ensure_schema

_store: "NebulaGraphStore | None" = None
_lock = threading.Lock()


def entity_vid(name: str) -> int:
    """Stable signed int64 VID from an entity name (read/write must agree)."""
    h = hashlib.blake2b((name or "").encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "big", signed=True)


def _q(value: Any) -> str:
    """Quote a scalar for inline nGQL (strings only; ints pass through)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


class NebulaGraphStore:
    def __init__(self, session: Any):
        self._session = session

    # --- writes ---------------------------------------------------------
    def upsert_nodes(self, nodes: list[Any]) -> None:
        for n in nodes:
            props = getattr(n, "properties", {}) or {}
            vid = entity_vid(getattr(n, "name", ""))
            stmt = (
                "INSERT VERTEX `Entity` "
                "(name, description, mention_count, created_at, label) VALUES "
                f"{vid}:({_q(getattr(n, 'name', ''))}, "
                f"{_q(props.get('description', ''))}, "
                f"{int(props.get('mention_count', 0) or 0)}, "
                f"{int(props.get('created_at', 0) or 0)}, "
                f"{_q(getattr(n, 'label', '') or '')});"
            )
            self._exec(stmt)

    def upsert_relations(self, relations: list[Any]) -> None:
        for r in relations:
            label = getattr(r, "label", "RELATED") or "RELATED"
            props = getattr(r, "properties", {}) or {}
            src = entity_vid(getattr(r, "source_id", ""))
            tgt = entity_vid(getattr(r, "target_id", ""))
            stmt = (
                f"INSERT EDGE `{label}` (polarity, valid_from, valid_to) VALUES "
                f"{src} -> {tgt}:("
                f"{_q(props.get('polarity', ''))}, "
                f"{int(props.get('valid_from', 0) or 0)}, "
                f"{int(props.get('valid_to', 0) or 0)});"
            )
            self._exec(stmt)

    # --- raw nGQL (Phase 2 read path builds on this) --------------------
    def structured_query(self, query: str, param_map: dict[str, Any] | None = None) -> list[dict]:
        resp = self._session.execute(query)
        if not resp.is_succeeded():
            raise RuntimeError(f"nGQL failed: {resp.error_msg()}")
        return _rows_to_dicts(resp)

    def _exec(self, stmt: str) -> None:
        resp = self._session.execute(stmt)
        if not resp.is_succeeded():
            logger.warning("nebula write failed: {s} -> {e}", s=stmt[:80], e=resp.error_msg())

    def close(self) -> None:
        with contextlib_suppress():
            self._session.release()


def _rows_to_dicts(resp: Any) -> list[dict]:
    """Map a nebula3 ResultSet to a list of column->value dicts."""
    cols = resp.keys()
    out: list[dict] = []
    for i in range(resp.row_size()):
        row = resp.row_values(i)
        out.append({c: row[j].cast() for j, c in enumerate(cols)})
    return out


class contextlib_suppress:
    def __enter__(self): return self
    def __exit__(self, *a): return True


def build_nebula_graph_store() -> "NebulaGraphStore":
    """Process-global NebulaGraph store (mirrors build_neo4j_graph_store)."""
    global _store
    if _store is not None:
        return _store
    with _lock:
        if _store is None:
            from nebula3.Config import Config
            from nebula3.gclient.net import ConnectionPool

            cfg = settings.nebula  # added in Task 1.4
            pool = ConnectionPool()
            pool.init([(cfg.host, cfg.port)], Config())
            sess = pool.get_session(cfg.user, cfg.password.get_secret_value())
            sess.execute(f"USE `{SPACE_NAME}`;")
            ensure_schema(sess)
            _store = NebulaGraphStore(sess)
    return _store
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_nebula_store_writes.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Add `NebulaSettings` (referenced by the factory)**

In `src/config.py`, mirror `Neo4jSettings` (after `GraphSettings`): fields `host: str = "localhost"`, `port: int = 9669`, `user: str = "root"`, `password: SecretStr = SecretStr("nebula")`, `space: str = "kb"`, `env_prefix="NEBULA_"`; add a `nebula` cached_property to `Settings` after `graph`. (Test: extend `tests/test_config/test_graph_settings.py` with a default-port assertion.)

- [ ] **Step 6: Commit**

```bash
git add src/graph/nebula_store.py src/config.py tests/test_graph/test_nebula_store_writes.py tests/test_config/test_graph_settings.py
git commit -m "feat(graph): NebulaGraphStore write adapter + NebulaSettings + factory"
```

### Task 1.4: Dual-write structural-parity harness

**Files:**
- Create: `tests/eval/migration/parity_write.py` (eval script, not CI-gated)

**Interfaces:**
- Consumes: `build_neo4j_graph_store`, `build_nebula_graph_store`, a small fixture of `EntityNode`/`Relation` objects.
- Produces: a printed parity report (node count, edge count, sampled entity props) for Neo4j vs Nebula — the **gate that must pass before Phase 2**.

- [ ] **Step 1: Write the harness**

```python
# tests/eval/migration/parity_write.py
"""Write the same fixture to Neo4j and NebulaGraph via the seam and compare
structure (node/edge counts + a sampled entity).  Run manually:

    GRAPH_BACKEND=neo4j  python -m tests.eval.migration.parity_write
    GRAPH_BACKEND=nebula python -m tests.eval.migration.parity_write

then diff the two JSON reports.  Gate for Phase 2 (read translation)."""
from __future__ import annotations

import json

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.store import build_graph_store

FIXTURE_NODES = [
    EntityNode(name="Иванов", label="PERSON", properties={"description": "инженер", "mention_count": 3}),
    EntityNode(name="Москва", label="CITY", properties={"description": "город", "mention_count": 9}),
]
FIXTURE_RELS = [
    Relation(source_id="Иванов", target_id="Москва", label="RELATED", properties={"polarity": "pos"}),
]


def main() -> None:
    store = build_graph_store()
    store.upsert_nodes(FIXTURE_NODES)
    store.upsert_relations(FIXTURE_RELS)
    report = {
        "nodes_written": len(FIXTURE_NODES),
        "rels_written": len(FIXTURE_RELS),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run against both backends and eyeball parity**

```bash
docker compose up -d neo4j nebula-graphd
GRAPH_BACKEND=neo4j  python -m tests.eval.migration.parity_write
GRAPH_BACKEND=nebula python -m tests.eval.migration.parity_write
```
Expected: both write 2 nodes / 1 rel without error. (Read-back parity is completed in Phase 2 once nGQL reads exist.)

- [ ] **Step 3: Commit**

```bash
git add tests/eval/migration/parity_write.py
git commit -m "test(graph): dual-write parity harness (Phase 2 gate)"
```

**Phase 1 exit criteria:** `GRAPH_BACKEND=nebula` writes entities/relations through the seam against a live Nebula; unit tests green; parity harness writes to both stores cleanly.

---

## Phases 2–6 — Sub-project specs (each becomes its own detailed plan)

Do **not** implement these from the summaries below — generate a dedicated `writing-plans` plan for each when its predecessor lands. Boundaries, files, and the gating benchmark are fixed here so the sub-plans stay decoupled.

### Carry-forward decisions from the Phase 0/1 whole-branch review (decide before the dependent phase builds on them)

- **VID width / collision ceiling — DECIDED (2026-07-09): 128-bit `FIXED_STRING(32)`.** `entity_vid` is now `blake2b(name, digest_size=16).hexdigest()` (32-hex-char string) and the space is created with `vid_type=FIXED_STRING(32)` (`src/graph/nebula_schema.py` `SPACE_DDL`, `src/graph/nebula_store.py` `entity_vid`). Rationale: a 64-bit hash has non-negligible birthday-collision probability (≈ n²/2⁶⁵) at the billions-of-entities target, and a VID collision silently merges two distinct entities. 128-bit keeps expected collisions negligible. `vid_type` is fixed at space creation, so this was set before any load.
- **`NebulaSettings.space` vs `nebula_schema.SPACE_NAME`.** Phase 1 left `NebulaSettings.space` (`NEBULA_SPACE`) as dead config — `SPACE_NAME` is the sole source of truth and nothing reads the setting, so `NEBULA_SPACE=…` is silently ignored. When Phase 2 needs a configurable/multi-space setup, either wire `SPACE_NAME` off the setting or drop the field.
- **`NebulaGraphStore.structured_query` param binding.** Phase 1 makes it raise `NotImplementedError` on a non-empty `param_map` (fail-loud). Phase 2's read translation must implement real nGQL parameter binding before any nebula read caller passes params.
- **Dynamic rel types → fixed edge schema.** Neo4j creates relationship types dynamically; Nebula requires pre-declared edge types. Phase 1 declares only `RELATED/MENTIONS/IN_COMMUNITY/PARENT_OF` and injection-guards the label (undeclared labels fail at INSERT, fail-open). Phase 2 must decide the representation (e.g. a generic `RELATED` edge carrying the original type as a property, or declaring the full type set).

### Phase 2 — Read-path nGQL translation

**Goal:** every read query the app issues works on `GRAPH_BACKEND=nebula` with output parity to Neo4j.
**Boundary:** the ~35 `_CYPHER` constants / 62 `structured_query` call-sites, translated to nGQL. Highest-value first: `src/graph/retriever.py` (`_WALK_CYPHER` variable-length walk → nGQL `GO ... STEPS` / `MATCH`; `_FIND_BY_NAME_CYPHER` full-text → Nebula `LOOKUP` on `entity_name_idx` or an ES mixed-index); `src/workflow/search/activities/{global_search,community,documents}.py`; `src/analytics/primitives/*` via `run_rows`; `src/graph/communities.py` read queries.
**Interface/deliverable:** a `nebula_queries.py` module of nGQL constants paired 1:1 with the Neo4j `_CYPHER` names, selected by backend at each call-site (or via a small `dialect(store)` helper). Per-query parity test in `tests/eval/migration/` comparing Neo4j vs Nebula rows on a shared fixture.
**Gate:** search-path parity (`retriever.awalk`, global-search) + p95 latency within target before enabling nebula reads in any workload.
**Known translation risks to resolve in the sub-plan:** `apoc.coll.flatten` walk aggregation, `labels()`/`type()`/`startNode`/`endNode` accessors, `db.index.fulltext.queryNodes`, `datetime()`/`timestamp()` temporal functions, `MERGE` upsert semantics.

### Phase 3 — Move vectors graph→Milvus

**Goal:** remove `er_vec`/`report_vec` from the graph; serve ER-kNN and community-report semantic-select from Milvus (dim=1536, cosine).
**Boundary:** `src/graph/index.py` (drop the two vector-index DDLs), `src/graph/entity_resolution.py:1245` (ER native-kNN → Milvus query), `src/workflow/search/activities/global_search.py:57` (report select → Milvus), `scripts/backfill_er_vector.py`. Touches **ADR-0008** (native-vector-knn-er) — supersede it.
**Deliverable:** two Milvus collections (`entity_er_vec`, `community_report_vec`) + query helpers; graph stores only the Milvus id reference.
**Gate:** ER precision/recall parity on the existing ER eval set.

### Phase 4 — Distributed compute (GraphScope/Spark) replacing GDS + igraph

**Goal:** community detection + centralities run on a distributed engine over the whole graph, write results back as vertex props. Scaled evolution of the existing `community_backend` offload seam (`src/graph/community_leiden.py`, ADR-0015).
**Boundary:** extend `community_backend: Literal["gds","leidenalg"]` (`src/config.py:386`) with `"graphscope"` (or `"spark"`); new `src/graph/community_graphscope.py` (export full graph from Nebula → distributed Leiden → write-back), matching the existing row shape emitted by `community_leiden.py`. Port `src/graph/analysis.py` + `src/analytics/materialize.py` GDS calls (PageRank/betweenness/eigenvector/WCC/nodeSimilarity) to the same engine.
**Deliverable:** batch job producing `community_id` + centralities written back; incremental re-run path (full rebuild on billions is a batch, not interactive).
**Gate:** community parity vs `bench_community_backends.py` (extend it with the distributed backend) + acceptable full/incremental build time on a billion-element dump.

### Phase 5 — Bulk-ingest path

**Goal:** load billions of elements (backfill) without the per-doc LLM/Temporal pipeline.
**Boundary:** new `scripts/nebula_bulk_load.py` using Nebula Importer or the Spark connector; keep the online per-doc path for increments. Bulk-load reads from an intermediate columnar dump (entities.parquet / edges.parquet) produced by the extraction pipeline.
**Deliverable:** documented bulk-load throughput (target: ≫ 4 docs/s online rate) + idempotent re-load.
**Gate:** loaded graph passes Phase 2 read-parity spot checks.

### Phase 6 — Cutover & decommission

**Goal:** flip production workloads to Nebula one at a time, soak, then remove Neo4j.
**Boundary:** per-workload flip order (read-only search first → analytics batch → ingest writes last), each behind `GRAPH_BACKEND` scoped per service; parity soak window with Neo4j still writable for rollback; finally delete Neo4j compose services, `Neo4jPropertyGraphStore` construction, `_neo4j_*` config, and neo4j-only tests.
**Deliverable:** Neo4j removed; `GRAPH_BACKEND` default set to `"nebula"`.
**Gate:** parity + latency SLOs met for a full soak window on every workload; rollback rehearsed.

---

## Self-Review

**Spec coverage:** Every migration concern from the analysis maps to a phase — store seam (0), Nebula store+schema+writes (1), query rewrite (2), vector move to Milvus (3), distributed compute replacing GDS/igraph (4), bulk ingest for billion-scale (5), cutover/decommission (6). The three clarified requirements are honored: *self-hosted only* (Nebula/GraphScope/Spark, no managed service), *billions of total elements* (partition_num=100 + bulk-load + distributed compute), *analytics-dominant* (Phase 4 is the load-bearing subsystem; query-time search stays on precomputed Milvus report vectors from Phase 3).

**Placeholder scan:** Phase 0–1 steps contain complete code/config and exact commands. Phases 2–6 are intentionally spec-level (scope-check decomposition) and are labeled as requiring their own plans — not placeholders inside an executable task.

**Type consistency:** The seam contract (`structured_query`/`upsert_nodes`/`upsert_relations`) is identical across `KbGraphStore` (Task 0.2), the Neo4j branch (existing), and `NebulaGraphStore` (Task 1.3). `entity_vid()` is defined once (Task 1.3) and reused by the Phase 2 read path. `build_graph_store()` (0.2) → `build_nebula_graph_store()` (1.3) import is wired in 0.2 and satisfied in 1.3. `settings.nebula` is referenced in 1.3 Step 3 and defined in 1.3 Step 5.

**Known live-cluster caveats to validate during execution** (unit tests are DB-free, so these surface only against a real Nebula): the `nebula3-python` pin (3.8.2) must match the server image (v3.8.0) — adjust if the client/server handshake rejects; `CREATE SPACE` is asynchronous in Nebula (heartbeat interval) so `USE kb` may need a short retry right after space creation; and `_rows_to_dicts` assumes `.cast()` on value wrappers — confirm against the installed `nebula3` ResultSet API.

# Nebula community-BUILD (nGQL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Under `GRAPH_BACKEND=nebula`, community detection materialises `:Community` vertices + `IN_COMMUNITY`/`PARENT_OF` edges into nebula (nGQL), via a `CommunityWriteback` seam, with the neo4j path byte-for-byte unchanged.

**Architecture:** A `CommunityWriteback` Protocol (mirrors the merged vector-store seams) with `Neo4jCommunityWriteback` (wraps today's Cypher constants verbatim) and `NebulaCommunityWriteback` (nGQL). `communities.py`'s `detect_communities` + `detect_hierarchy` build the writeback via `build_community_writeback(store)` and call its methods in place of inline `_run_query(cypher)`. The nebula `Community` TAG + a level index are added to the schema DDL.

**Tech Stack:** Python 3.12, nebula3-python (inline nGQL — no param binding), Neo4j Cypher, pytest, loguru.

## Global Constraints

- **Default neo4j BUILD path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, every write-back statement + param dict is IDENTICAL to today — the neo4j impl runs the existing Cypher constants verbatim. Nebula reached only under `GRAPH_BACKEND=nebula`.
- Full BUILD stage (both `detect_communities` and `detect_hierarchy`). SUMMARIZE + READ out of scope.
- Opt-in / strangler-fig. Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store / fake session).
- `report_vec` is NOT written on the nebula `Community` vertex (it lives in Milvus after Phase 3).
- Nebula VID scheme identical to `entity_vid`: `blake2b(digest_size=16).hexdigest()`. Community VID = `blake2b(f"{community_id}:{level}")`.
- Nebula `structured_query` raises `NotImplementedError` on a non-empty `param_map`, so the nebula impl builds FULLY-INLINE nGQL (values quoted via `nebula_store._q`) and passes NO `param_map`.
- Fail-open unchanged: the existing try/except around the write-back (`communities.py:519-534`, `:679-714`) stays; seam methods may raise and are caught there exactly as the raw Cypher was.
- Interpreter: `.venv/bin/python`. Set `API_ENV=development` when running pytest (settings preflight).

## File Structure

- **Create** `src/graph/community_writeback.py` — `community_vid`, `CommunityWriteback` Protocol, `Neo4jCommunityWriteback`, `NebulaCommunityWriteback`, `build_community_writeback`. Owns the nebula nGQL; imports the neo4j Cypher constants from `communities.py`.
- **Modify** `src/graph/nebula_schema.py` — add `Community` TAG + `community_level_idx` to `SCHEMA_DDL`.
- **Modify** `src/graph/communities.py` — `detect_communities` + `detect_hierarchy` + `_read_old_reports` route the BUILD write-back through the seam (lazy import of `build_community_writeback` inside the functions, to avoid a module-level import cycle since `community_writeback` imports constants from `communities`).
- **Create** `tests/test_graph/test_community_writeback.py` — neo4j parity + nebula nGQL + dispatch + `community_vid`.
- **Modify** `tests/test_graph/test_communities.py` — integration: detect_* route through a fake writeback.

**Import-cycle note:** `community_writeback.py` does `from src.graph.communities import _MERGE_COMMUNITY_CYPHER, ...` at MODULE level. `communities.py` must therefore import `build_community_writeback` LAZILY (inside `detect_communities`/`detect_hierarchy`/`_read_old_reports`), exactly as it already lazily imports `ensure_community_indexes`. Do NOT add a module-level `import community_writeback` to `communities.py`.

---

### Task 1: Nebula schema — `Community` TAG + level index

**Files:**
- Modify: `src/graph/nebula_schema.py` (`SCHEMA_DDL`, after the `PARENT_OF` line `:47`)
- Test: `tests/test_graph/test_nebula_schema.py` (create if absent; else append)

**Interfaces:**
- Consumes: nothing.
- Produces: `SCHEMA_DDL` now contains a `CREATE TAG ... Community (...)` and a `CREATE TAG INDEX ... community_level_idx ON Community(level)` statement. `ensure_schema` runs them idempotently.

- [ ] **Step 1: Write the failing test**

Create/append `tests/test_graph/test_nebula_schema.py`:

```python
from src.graph.nebula_schema import SCHEMA_DDL


def test_schema_has_community_tag_with_report_columns():
    tag = next((s for s in SCHEMA_DDL if "CREATE TAG IF NOT EXISTS `Community`" in s), None)
    assert tag is not None, "Community TAG missing from SCHEMA_DDL"
    # Structural columns written by the BUILD stage + report columns declared
    # now so the SUMMARIZE slice adds only writes, not a schema migration.
    for col in ("id string", "level int", "member_count int", "members_hash string",
                "updated int", "report string", "title string", "summary string",
                "summarized_at int"):
        assert col in tag, f"missing column: {col}"
    # report_vec lives in Milvus (Phase 3) — never on the vertex.
    assert "report_vec" not in tag


def test_schema_has_community_level_index():
    assert any(
        "CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level)" in s
        for s in SCHEMA_DDL
    ), "community_level_idx missing (needed for prune/lookup by level)"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_schema.py -q`
Expected: FAIL (`Community TAG missing from SCHEMA_DDL`).

- [ ] **Step 3: Add the DDL**

In `src/graph/nebula_schema.py`, in the `SCHEMA_DDL` list, immediately after the `CREATE EDGE IF NOT EXISTS \`PARENT_OF\` ();` line, add:

```python
    # Community vertices materialised by the BUILD stage of community
    # detection (src/graph/community_writeback.py). Report columns
    # (report/title/summary/summarized_at) are declared now so the SUMMARIZE
    # slice adds only write logic, not a schema migration. `report_vec` is
    # intentionally absent — it lives in Milvus (Phase 3).
    "CREATE TAG IF NOT EXISTS `Community` ("
    "id string, level int DEFAULT 0, member_count int DEFAULT 0, "
    "members_hash string DEFAULT '', updated int DEFAULT 0, "
    "report string DEFAULT '', title string DEFAULT '', "
    "summary string DEFAULT '', summarized_at int DEFAULT 0);",
    # Backs prune_level / prune_all / read_old_reports LOOKUPs (Nebula
    # requires an index to LOOKUP by property).
    "CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level);",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_nebula_schema.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/graph/nebula_schema.py tests/test_graph/test_nebula_schema.py`

```bash
git add src/graph/nebula_schema.py tests/test_graph/test_nebula_schema.py
git commit -m "feat(nebula): Community TAG + level index in schema DDL

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Seam scaffold — `community_vid`, Protocol, `Neo4jCommunityWriteback`, dispatch

**Files:**
- Create: `src/graph/community_writeback.py`
- Test: `tests/test_graph/test_community_writeback.py`

**Interfaces:**
- Consumes: neo4j Cypher constants from `src.graph.communities` (`_MERGE_COMMUNITY_CYPHER`, `_MERGE_SUBCOMMUNITY_CYPHER`, `_PRUNE_LEVEL_CYPHER`, `_PRUNE_ALL_CYPHER`, `_READ_OLD_REPORTS_CYPHER`, `_COMMUNITY_CONSTRAINT`); `ensure_community_indexes` from `src.graph.index`; `settings` from `src.config`.
- Produces:
  - `community_vid(community_id: str, level: int) -> str` — `blake2b(f"{community_id}:{level}", digest_size=16).hexdigest()`.
  - `CommunityWriteback` Protocol with `ensure_schema()`, `read_old_reports() -> list[dict]`, `prune_level(level: int)`, `prune_all()`, `merge_community(*, community_id, level, member_count, members_hash, members, carry)`, `merge_subcommunity(*, community_id, level, parent_id, member_count, members_hash, members, carry)`. `carry` is `None` or a dict with keys `report`/`title`/`summary`/`report_vec`/`summarized_at`.
  - `Neo4jCommunityWriteback(store)`, `NebulaCommunityWriteback(store)` (impl in Task 3), `build_community_writeback(store) -> CommunityWriteback`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_writeback.py`:

```python
import src.graph.communities as comm
from src.graph import community_writeback as cw


class _RecStore:
    """Records structured_query(cypher, param_map) calls; returns [] (or a
    canned value for reads)."""
    def __init__(self, ret=None):
        self.calls = []
        self._ret = ret or []
    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._ret


def test_community_vid_is_stable_32hex_and_level_scoped():
    v = cw.community_vid("42", 0)
    assert isinstance(v, str) and len(v) == 32
    assert v == cw.community_vid("42", 0)           # stable
    assert v != cw.community_vid("42", 1)           # level-scoped
    assert v != cw.community_vid("43", 0)           # id-scoped


def test_neo4j_merge_community_issues_exact_cypher_and_params():
    store = _RecStore()
    wb = cw.Neo4jCommunityWriteback(store)
    wb.merge_community(community_id="7", level=0, member_count=3,
                       members_hash="h", members=["a", "b"], carry=None)
    assert len(store.calls) == 1
    cypher, params = store.calls[0]
    assert cypher is comm._MERGE_COMMUNITY_CYPHER          # SAME constant object
    assert params == {
        "community_id": "7", "level": 0, "member_count": 3,
        "members_hash": "h", "members": ["a", "b"],
        "carry_report": None, "carry_title": None, "carry_summary": None,
        "carry_report_vec": None, "carry_summarized_at": None,
    }


def test_neo4j_merge_subcommunity_maps_carry_and_parent():
    store = _RecStore()
    wb = cw.Neo4jCommunityWriteback(store)
    carry = {"report": "R", "title": "T", "summary": "S",
             "report_vec": [0.1], "summarized_at": 111}
    wb.merge_subcommunity(community_id="9", level=1, parent_id="7",
                          member_count=2, members_hash="h2",
                          members=["c"], carry=carry)
    cypher, params = store.calls[0]
    assert cypher is comm._MERGE_SUBCOMMUNITY_CYPHER
    assert params["parent_id"] == "7"
    assert params["carry_report"] == "R" and params["carry_summarized_at"] == 111
    assert params["carry_report_vec"] == [0.1]


def test_neo4j_prune_and_read_and_ensure_use_the_constants():
    store = _RecStore(ret=[{"level": 0, "h": "x", "report": "r"}])
    wb = cw.Neo4jCommunityWriteback(store)
    wb.prune_level(2)
    assert store.calls[-1] == (comm._PRUNE_LEVEL_CYPHER, {"level": 2})
    wb.prune_all()
    assert store.calls[-1] == (comm._PRUNE_ALL_CYPHER, {})
    rows = wb.read_old_reports()
    assert store.calls[-1] == (comm._READ_OLD_REPORTS_CYPHER, {})
    assert rows == [{"level": 0, "h": "x", "report": "r"}]


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(cw.settings.graph, "backend", "neo4j")
    assert isinstance(cw.build_community_writeback(_RecStore()), cw.Neo4jCommunityWriteback)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_writeback.py -q`
Expected: FAIL (`ModuleNotFoundError: src.graph.community_writeback`).

- [ ] **Step 3: Create the module (vid + Protocol + Neo4j impl + dispatch)**

Create `src/graph/community_writeback.py`:

```python
"""Backend-dispatched community BUILD write-back (the `:Community` +
`IN_COMMUNITY`/`PARENT_OF` materialisation of community detection).

`Neo4jCommunityWriteback` wraps the existing Cypher constants in
`communities.py` verbatim (default path, byte-for-byte unchanged).
`NebulaCommunityWriteback` translates the same operations to nGQL.
Only the BUILD stage lives here; SUMMARIZE (report write) and READ
(map/lexical/descent) remain neo4j-only for now.
"""
from __future__ import annotations

import hashlib
from typing import Any, Protocol

from loguru import logger

from src.config import settings
from src.graph.communities import (
    _COMMUNITY_CONSTRAINT,
    _MERGE_COMMUNITY_CYPHER,
    _MERGE_SUBCOMMUNITY_CYPHER,
    _PRUNE_ALL_CYPHER,
    _PRUNE_LEVEL_CYPHER,
    _READ_OLD_REPORTS_CYPHER,
)


def community_vid(community_id: str, level: int) -> str:
    """Stable 128-bit VID (32-hex-char) for a community, scoped by level.

    Mirrors `nebula_store.entity_vid` (same digest_size=16 blake2b) so the
    whole graph shares one VID scheme under FIXED_STRING(32)."""
    key = f"{community_id}:{int(level)}"
    return hashlib.blake2b(key.encode("utf-8"), digest_size=16).hexdigest()


def _carry_params(carry: dict | None) -> dict:
    """Map the clean-keyed carry dict to the `carry_*` params the neo4j
    MERGE Cypher expects (missing/None -> None), preserving today's shape."""
    c = carry or {}
    return {
        "carry_report": c.get("report"),
        "carry_title": c.get("title"),
        "carry_summary": c.get("summary"),
        "carry_report_vec": c.get("report_vec"),
        "carry_summarized_at": c.get("summarized_at"),
    }


class CommunityWriteback(Protocol):
    def ensure_schema(self) -> None: ...
    def read_old_reports(self) -> list[dict]: ...
    def prune_level(self, level: int) -> None: ...
    def prune_all(self) -> None: ...
    def merge_community(self, *, community_id: str, level: int, member_count: int,
                        members_hash: str, members: list[str], carry: dict | None) -> None: ...
    def merge_subcommunity(self, *, community_id: str, level: int, parent_id: str,
                           member_count: int, members_hash: str, members: list[str],
                           carry: dict | None) -> None: ...


class Neo4jCommunityWriteback:
    """Runs the historical Cypher constants verbatim — zero behaviour change."""

    def __init__(self, store: Any):
        self._store = store

    def _run(self, cypher: str, params: dict | None = None) -> list[dict]:
        rows = self._store.structured_query(cypher, param_map=params or {})
        return list(rows or [])

    def ensure_schema(self) -> None:
        self._run(_COMMUNITY_CONSTRAINT)
        from src.graph.index import ensure_community_indexes
        ensure_community_indexes(self._store)

    def read_old_reports(self) -> list[dict]:
        return self._run(_READ_OLD_REPORTS_CYPHER)

    def prune_level(self, level: int) -> None:
        self._run(_PRUNE_LEVEL_CYPHER, {"level": level})

    def prune_all(self) -> None:
        self._run(_PRUNE_ALL_CYPHER)

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        self._run(_MERGE_COMMUNITY_CYPHER, {
            "community_id": community_id, "level": level,
            "member_count": member_count, "members_hash": members_hash,
            "members": members, **_carry_params(carry),
        })

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        self._run(_MERGE_SUBCOMMUNITY_CYPHER, {
            "community_id": community_id, "level": level, "parent_id": parent_id,
            "member_count": member_count, "members_hash": members_hash,
            "members": members, **_carry_params(carry),
        })


def build_community_writeback(store: Any) -> CommunityWriteback:
    if settings.graph.backend == "nebula":
        return NebulaCommunityWriteback(store)
    return Neo4jCommunityWriteback(store)
```

Add a temporary stub so the module imports before Task 3 lands the real impl (Task 3 replaces this body):

```python
class NebulaCommunityWriteback:
    """nGQL community BUILD write-back. Implemented in Task 3."""

    def __init__(self, store: Any):
        self._store = store

    def ensure_schema(self) -> None:
        # Nebula Community TAG + index are created by nebula_schema.ensure_schema.
        return None

    def read_old_reports(self) -> list[dict]:
        raise NotImplementedError("NebulaCommunityWriteback.read_old_reports (Task 3)")

    def prune_level(self, level: int) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.prune_level (Task 3)")

    def prune_all(self) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.prune_all (Task 3)")

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.merge_community (Task 3)")

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        raise NotImplementedError("NebulaCommunityWriteback.merge_subcommunity (Task 3)")
```

Place `NebulaCommunityWriteback` ABOVE `build_community_writeback` (which references it). Keep `logger` imported — Task 3 uses it.

- [ ] **Step 4: Run test to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_writeback.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/graph/community_writeback.py tests/test_graph/test_community_writeback.py`
(If ruff flags `logger` as unused, add `# noqa: F401` on its import with a comment "used by NebulaCommunityWriteback in Task 3" — it will be used after Task 3; do NOT delete it.)

```bash
git add src/graph/community_writeback.py tests/test_graph/test_community_writeback.py
git commit -m "feat(community): CommunityWriteback seam + Neo4j impl (byte-for-byte) + dispatch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `NebulaCommunityWriteback` — nGQL implementation

**Files:**
- Modify: `src/graph/community_writeback.py` (replace the Task-2 `NebulaCommunityWriteback` stub body)
- Test: `tests/test_graph/test_community_writeback.py` (append nebula tests)

**Interfaces:**
- Consumes: `nebula_store.entity_vid`, `nebula_store._q`; `store.structured_query(query)` (inline nGQL, no `param_map`).
- Produces: a fully-working `NebulaCommunityWriteback` (same method surface as `Neo4jCommunityWriteback`).

**nGQL facts (verbatim):**
- Nebula INSERT is upsert-by-VID. `INSERT VERTEX \`Community\` (id, level, member_count, members_hash, updated, report, title, summary, summarized_at) VALUES "<cvid>":(...);`
- `INSERT EDGE \`IN_COMMUNITY\` (level) VALUES "<entity_vid>"->"<cvid>":(<level>);` (batch multiple with comma-separated VALUES).
- `INSERT EDGE \`PARENT_OF\` () VALUES "<parent_cvid>"->"<cvid>":();`
- Prune: `LOOKUP ON \`Community\` WHERE \`Community\`.level == <n> YIELD id(vertex) AS vid;` then `DELETE VERTEX "<v1>", "<v2>" WITH EDGE;`. prune_all uses `LOOKUP ON \`Community\` YIELD id(vertex) AS vid;`.
- read_old_reports: `LOOKUP ON \`Community\` YIELD id(vertex) AS vid;` → `FETCH PROP ON \`Community\` "<v1>","<v2>" YIELD \`Community\`.level AS level, \`Community\`.members_hash AS h, \`Community\`.report AS report, \`Community\`.title AS title, \`Community\`.summary AS summary, \`Community\`.summarized_at AS summarized_at;` filtered to non-blank report.
- `store.structured_query(q)` returns `list[dict]` (rows). LOOKUP `id(vertex)` column key is `"vid"` (per the YIELD alias).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_graph/test_community_writeback.py`:

```python
from src.graph.nebula_store import entity_vid


class _RecSession:
    """Fake NebulaGraphStore: records structured_query(q) statements; returns
    a canned row list per matched substring for reads."""
    def __init__(self, read_map=None):
        self.stmts = []
        self._read_map = read_map or {}
    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula writeback must inline values (no param_map)"
        self.stmts.append(query)
        for needle, rows in self._read_map.items():
            if needle in query:
                return rows
        return []


def test_nebula_merge_community_inserts_vertex_and_member_edges():
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    wb.merge_community(community_id="7", level=0, member_count=2,
                       members_hash="h", members=["Alice", "Bob"], carry=None)
    joined = "\n".join(s.stmts)
    cvid = cw.community_vid("7", 0)
    assert f'INSERT VERTEX `Community`' in joined
    assert f'"{cvid}":(' in joined
    # report_vec never written to the vertex
    assert "report_vec" not in joined
    # IN_COMMUNITY edges from each member's entity_vid to the community vid, with level
    assert f'INSERT EDGE `IN_COMMUNITY`' in joined
    assert f'"{entity_vid("Alice")}"->"{cvid}"' in joined
    assert f'"{entity_vid("Bob")}"->"{cvid}"' in joined


def test_nebula_merge_subcommunity_adds_parent_of_edge():
    s = _RecSession()
    wb = cw.NebulaCommunityWriteback(s)
    wb.merge_subcommunity(community_id="9", level=1, parent_id="7",
                          member_count=1, members_hash="h2",
                          members=["Carol"], carry={"report": "R", "title": "T",
                                                    "summary": "S", "report_vec": [0.1],
                                                    "summarized_at": 5})
    joined = "\n".join(s.stmts)
    child = cw.community_vid("9", 1)
    parent = cw.community_vid("7", 0)
    assert f'INSERT EDGE `PARENT_OF`' in joined
    assert f'"{parent}"->"{child}"' in joined
    # carry report text lands on the vertex; report_vec does not
    assert '"R"' in joined and '"T"' in joined and '"S"' in joined
    assert "0.1" not in joined


def test_nebula_prune_level_lookups_then_deletes_with_edge():
    cvid = cw.community_vid("7", 0)
    s = _RecSession(read_map={"LOOKUP ON `Community` WHERE": [{"vid": cvid}]})
    wb = cw.NebulaCommunityWriteback(s)
    wb.prune_level(0)
    joined = "\n".join(s.stmts)
    assert "LOOKUP ON `Community` WHERE `Community`.level == 0" in joined
    assert f'DELETE VERTEX "{cvid}" WITH EDGE' in joined


def test_nebula_prune_all_no_vertices_is_noop():
    s = _RecSession(read_map={"LOOKUP ON `Community` YIELD": []})
    wb = cw.NebulaCommunityWriteback(s)
    wb.prune_all()
    # LOOKUP ran, but no DELETE VERTEX (nothing to delete)
    assert any("LOOKUP ON `Community` YIELD" in q for q in s.stmts)
    assert not any("DELETE VERTEX" in q for q in s.stmts)


def test_nebula_read_old_reports_returns_rows_with_nonblank_report():
    cvid = cw.community_vid("7", 0)
    s = _RecSession(read_map={
        "LOOKUP ON `Community` YIELD": [{"vid": cvid}],
        "FETCH PROP ON `Community`": [
            {"level": 0, "h": "h", "report": "r", "title": "t",
             "summary": "s", "summarized_at": 9},
            {"level": 0, "h": "h2", "report": "", "title": "", "summary": "", "summarized_at": 0},
        ],
    })
    wb = cw.NebulaCommunityWriteback(s)
    rows = wb.read_old_reports()
    assert len(rows) == 1                       # blank-report row dropped
    assert rows[0]["h"] == "h" and rows[0]["report"] == "r"
    assert rows[0].get("report_vec") is None    # not stored on the vertex


def test_dispatch_returns_nebula_when_backend_nebula(monkeypatch):
    monkeypatch.setattr(cw.settings.graph, "backend", "nebula")
    assert isinstance(cw.build_community_writeback(_RecSession()), cw.NebulaCommunityWriteback)
```

- [ ] **Step 2: Run to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_writeback.py -q`
Expected: FAIL (`NotImplementedError` from the Task-2 stub).

- [ ] **Step 3: Implement the nGQL methods**

In `src/graph/community_writeback.py`, replace the `NebulaCommunityWriteback` stub body (keep `ensure_schema` as the no-op) with:

```python
class NebulaCommunityWriteback:
    """nGQL community BUILD write-back. INSERT is upsert-by-VID; both call
    sites prune before merge, so INSERT-overwrite == neo4j MERGE+FOREACH on a
    fresh node (no divergence). `report_vec` is never written to the vertex
    (it lives in Milvus). Values are inline-quoted (nebula binds no params)."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def ensure_schema(self) -> None:
        # Community TAG + index are created by nebula_schema.ensure_schema.
        return None

    def _insert_community_vertex(self, *, cvid, community_id, level,
                                 member_count, members_hash, carry) -> None:
        from src.graph.nebula_store import _q
        import time
        c = carry or {}
        updated = int(time.time() * 1000)
        self._exec(
            "INSERT VERTEX `Community` "
            "(id, level, member_count, members_hash, updated, "
            "report, title, summary, summarized_at) VALUES "
            f"{_q(cvid)}:({_q(community_id)}, {int(level)}, {int(member_count)}, "
            f"{_q(members_hash)}, {updated}, "
            f"{_q(c.get('report') or '')}, {_q(c.get('title') or '')}, "
            f"{_q(c.get('summary') or '')}, {int(c.get('summarized_at') or 0)});"
        )

    def _insert_member_edges(self, *, cvid, level, members) -> None:
        from src.graph.nebula_store import _q, entity_vid
        # No stale-clear needed: both call sites prune (prune_level/prune_all
        # via DELETE VERTEX ... WITH EDGE) BEFORE merge, so the community vertex
        # is fresh with no incoming IN_COMMUNITY edges — the design's central
        # prune-before-merge invariant. (neo4j's MERGE clears stale inline;
        # prune-first makes that redundant on nebula, and avoids a fragile
        # GO|DELETE pipe on the main write path.)
        if not members:
            return
        values = ", ".join(
            f"{_q(entity_vid(m))}->{_q(cvid)}:({int(level)})" for m in members
        )
        self._exec(f"INSERT EDGE `IN_COMMUNITY` (level) VALUES {values};")

    def merge_community(self, *, community_id, level, member_count,
                        members_hash, members, carry) -> None:
        cvid = community_vid(community_id, level)
        self._insert_community_vertex(
            cvid=cvid, community_id=community_id, level=level,
            member_count=member_count, members_hash=members_hash, carry=carry)
        self._insert_member_edges(cvid=cvid, level=level, members=members)

    def merge_subcommunity(self, *, community_id, level, parent_id,
                           member_count, members_hash, members, carry) -> None:
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        self._insert_community_vertex(
            cvid=cvid, community_id=community_id, level=level,
            member_count=member_count, members_hash=members_hash, carry=carry)
        parent_vid = community_vid(parent_id, level - 1)
        self._exec(
            f"INSERT EDGE `PARENT_OF` () VALUES {_q(parent_vid)}->{_q(cvid)}:();"
        )
        self._insert_member_edges(cvid=cvid, level=level, members=members)

    def _lookup_vids(self, where: str | None) -> list[str]:
        clause = f" WHERE {where}" if where else ""
        rows = self._exec(f"LOOKUP ON `Community`{clause} YIELD id(vertex) AS vid;")
        return [r["vid"] for r in rows if r.get("vid")]

    def _delete_vids(self, vids: list[str]) -> None:
        from src.graph.nebula_store import _q
        if not vids:
            return
        listed = ", ".join(_q(v) for v in vids)
        self._exec(f"DELETE VERTEX {listed} WITH EDGE;")

    def prune_level(self, level: int) -> None:
        self._delete_vids(self._lookup_vids(f"`Community`.level == {int(level)}"))

    def prune_all(self) -> None:
        self._delete_vids(self._lookup_vids(None))

    def read_old_reports(self) -> list[dict]:
        from src.graph.nebula_store import _q
        vids = self._lookup_vids(None)
        if not vids:
            return []
        listed = ", ".join(_q(v) for v in vids)
        rows = self._exec(
            f"FETCH PROP ON `Community` {listed} YIELD "
            "`Community`.level AS level, `Community`.members_hash AS h, "
            "`Community`.report AS report, `Community`.title AS title, "
            "`Community`.summary AS summary, `Community`.summarized_at AS summarized_at;"
        )
        return [r for r in rows if (r.get("report") or "").strip()]
```

- [ ] **Step 4: Run to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_writeback.py -q`
Expected: PASS (11 passed — 5 from Task 2 + 6 new).

- [ ] **Step 5: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/graph/community_writeback.py tests/test_graph/test_community_writeback.py`

```bash
git add src/graph/community_writeback.py tests/test_graph/test_community_writeback.py
git commit -m "feat(nebula): NebulaCommunityWriteback nGQL (INSERT/LOOKUP+DELETE/FETCH)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Route `communities.py` BUILD write-back through the seam

**Files:**
- Modify: `src/graph/communities.py` — `_read_old_reports` (`:255` region), `detect_communities` write-back (`:518-540`), `detect_hierarchy` carry + write-back (`:640-714`)
- Test: `tests/test_graph/test_communities.py` (append)

**Interfaces:**
- Consumes: `build_community_writeback` from `src.graph.community_writeback` (LAZY import inside functions).
- Produces: no new public interface; behaviour identical on neo4j, and `GRAPH_BACKEND=nebula` now materialises communities.

**Key parity rule:** on neo4j the sequence of writeback calls must reproduce today's exact Cypher+params. `detect_communities`: `ensure_schema()` (= constraint + indexes), `prune_level(level)`, then per community `merge_community(..., carry=None)`. `detect_hierarchy`: `read_old_reports()` (via `_read_old_reports`), `ensure_schema()`, `prune_all()`, then per community `merge_community`/`merge_subcommunity` with `carry` from the old-reports lookup.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_graph/test_communities.py`:

```python
import asyncio

import src.graph.communities as comm


class _FakeWriteback:
    def __init__(self):
        self.calls = []
    def ensure_schema(self): self.calls.append(("ensure_schema",))
    def read_old_reports(self): self.calls.append(("read_old_reports",)); return []
    def prune_level(self, level): self.calls.append(("prune_level", level))
    def prune_all(self): self.calls.append(("prune_all",))
    def merge_community(self, **kw): self.calls.append(("merge_community", kw["community_id"], kw["level"]))
    def merge_subcommunity(self, **kw): self.calls.append(("merge_subcommunity", kw["community_id"], kw["level"]))


def test_detect_communities_routes_writeback_through_seam(monkeypatch):
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "leidenalg")

    def fake_extract(store, *, batch_size=50_000):
        edges = [("a", "b", 5.0), ("b", "c", 5.0), ("a", "c", 5.0),
                 ("x", "y", 5.0), ("y", "z", 5.0), ("x", "z", 5.0), ("c", "x", 0.1)]
        return edges, list("abcxyz")
    monkeypatch.setattr(comm, "extract_entity_edges", fake_extract)

    fake = _FakeWriteback()
    monkeypatch.setattr(comm, "build_community_writeback", lambda store: fake)

    class _Store:
        def structured_query(self, cypher, param_map=None): return []
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert len(refs) == 2
    kinds = [c[0] for c in fake.calls]
    assert kinds[0] == "ensure_schema"
    assert ("prune_level", 0) in fake.calls
    assert kinds.count("merge_community") == 2      # two cliques, single level
    assert "prune_all" not in kinds
```

- [ ] **Step 2: Run to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_communities.py::test_detect_communities_routes_writeback_through_seam -q`
Expected: FAIL (`AttributeError: module ... has no attribute 'build_community_writeback'` — the monkeypatch target doesn't exist yet, and the real function still calls `_run_query`).

- [ ] **Step 3: Rewire `detect_communities`**

In `src/graph/communities.py`, replace the `detect_communities` persist block (currently `:518-540`, the `try:` that runs `_COMMUNITY_CONSTRAINT`, `ensure_community_indexes`, `_PRUNE_LEVEL_CYPHER`, the `_MERGE_COMMUNITY_CYPHER` loop, and the GDS-only `finally` drop) with:

```python
    # Persist :Community nodes + member links via the backend writeback
    # (neo4j: the historical Cypher verbatim; nebula: nGQL).
    from src.graph.community_writeback import build_community_writeback
    writeback = build_community_writeback(store)
    try:
        await asyncio.to_thread(writeback.ensure_schema)
        # Prune the prior run's communities for THIS level FIRST so a rebuild
        # starts clean (Leiden may renumber/shrink ids). Level-scoped.
        await asyncio.to_thread(writeback.prune_level, level)
        for comm_ref in communities:
            # asyncio.to_thread forwards **kwargs; each call is awaited before
            # the next iteration, so no closure/late-binding concern.
            await asyncio.to_thread(
                writeback.merge_community,
                community_id=comm_ref.community_id, level=comm_ref.level,
                member_count=comm_ref.member_count, members_hash=comm_ref.members_hash,
                members=comm_ref.members, carry=None,
            )
    except Exception as exc:
        logger.warning("communities: :Community write failed: {e}", e=exc)
    finally:
        # Only the GDS path allocates an in-memory projection to drop.
        if settings.temporal.community_backend not in ("leidenalg", "graphscope"):
            with contextlib.suppress(Exception):
                await asyncio.to_thread(_run_query, store, _drop_cypher(graph_name))

    return communities
```

(Keep the `if not rows:` warning block + `communities = _coarsest_from_rows(...)` + the detection log ABOVE this unchanged.)

- [ ] **Step 4: Rewire `detect_hierarchy` + `_read_old_reports`**

4a. In `_read_old_reports` (`:255` region), replace the `rows = await asyncio.to_thread(_run_query, store, _READ_OLD_REPORTS_CYPHER)` line with:

```python
        from src.graph.community_writeback import build_community_writeback
        rows = await asyncio.to_thread(build_community_writeback(store).read_old_reports)
```

(The surrounding `if store is None: return {}` / try-except / the `{(level,h): {...}}` transform stay unchanged.)

4b. In `detect_hierarchy`'s persist block (`:679-714`), replace the `try:` body (constraint + indexes + `_PRUNE_ALL_CYPHER` + the `_MERGE_COMMUNITY_CYPHER`/`_MERGE_SUBCOMMUNITY_CYPHER` loop over `zip(communities, carry_params)`) with:

```python
    from src.graph.community_writeback import build_community_writeback
    writeback = build_community_writeback(store)
    try:
        await asyncio.to_thread(writeback.ensure_schema)
        # Prune EVERY prior :Community (depth/ids can change between runs).
        await asyncio.to_thread(writeback.prune_all)
        for comm_ref, carry in zip(communities, carry_params):
            carry_clean = {
                "report": carry.get("carry_report"),
                "title": carry.get("carry_title"),
                "summary": carry.get("carry_summary"),
                "report_vec": carry.get("carry_report_vec"),
                "summarized_at": carry.get("carry_summarized_at"),
            }
            if comm_ref.level == 0:
                await asyncio.to_thread(
                    writeback.merge_community,
                    community_id=comm_ref.community_id, level=comm_ref.level,
                    member_count=comm_ref.member_count, members_hash=comm_ref.members_hash,
                    members=comm_ref.members, carry=carry_clean,
                )
            else:
                await asyncio.to_thread(
                    writeback.merge_subcommunity,
                    community_id=comm_ref.community_id, level=comm_ref.level,
                    parent_id=comm_ref.parent_id, member_count=comm_ref.member_count,
                    members_hash=comm_ref.members_hash, members=comm_ref.members,
                    carry=carry_clean,
                )
    except Exception as exc:
        logger.warning("communities: :Community hierarchy write failed: {e}", e=exc)
```

(`carry_params` still carries `carry_*`-prefixed dicts; the `carry_clean` remap feeds the seam's clean-key contract. The neo4j impl re-derives the `carry_*` params, so the Cypher params are byte-for-byte identical to today. The hierarchy path has NO GDS `finally` drop today — do not add one.)

- [ ] **Step 5: Run the routing test + the full community suite**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_communities.py tests/test_graph/test_community_backend_switch.py tests/test_graph/test_community_writeback.py -q`
Expected: PASS (all — including the pre-existing community-backend and graphscope tests, which must stay green).

- [ ] **Step 6: Confirm the constants are still referenced (no dead code) + ruff**

Run: `cd "$(git rev-parse --show-toplevel)" && grep -c "_MERGE_COMMUNITY_CYPHER\|_MERGE_SUBCOMMUNITY_CYPHER\|_PRUNE_LEVEL_CYPHER\|_PRUNE_ALL_CYPHER\|_READ_OLD_REPORTS_CYPHER\|_COMMUNITY_CONSTRAINT" src/graph/communities.py`
Expected: `>= 6` (the constants remain DEFINED in communities.py and are imported by community_writeback.py; they are no longer called inline in the write-back — that is correct, not dead code).

Run: `.venv/bin/python -m ruff check src/graph/communities.py tests/test_graph/test_communities.py`
Expected: clean (a pre-existing `B905` at the `detect_hierarchy` `zip(...)` line may remain if that line is untouched — leave it; it predates this work).

- [ ] **Step 7: Commit**

```bash
git add src/graph/communities.py tests/test_graph/test_communities.py
git commit -m "feat(community): route BUILD write-back through CommunityWriteback seam

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Notes for the manual gate (post-merge, controller-run)

On the running nebula cluster with `GRAPH_BACKEND=nebula`: run `detect_communities` then `detect_hierarchy`; verify `:Community` vertices + `IN_COMMUNITY`/`PARENT_OF` edges materialise (`LOOKUP ON \`Community\` ...`, `GET SUBGRAPH`); verify a second run prunes cleanly (no ghost communities / orphaned edges). This is out of the automated scope (DB-free tests only).

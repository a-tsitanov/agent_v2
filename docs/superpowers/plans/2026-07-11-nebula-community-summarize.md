# Nebula community-SUMMARIZE (nGQL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Under `GRAPH_BACKEND=nebula`, community summarization reads its context and persists the report on the `:Community` vertex via nGQL, behind a `CommunitySummarize` seam, with the neo4j path byte-for-byte unchanged.

**Architecture:** A `CommunitySummarize` Protocol (mirrors the merged `CommunityWriteback` seam) with `Neo4jCommunitySummarize` (wraps the 3 existing Cypher constants verbatim) and `NebulaCommunitySummarize` (nGQL). `community.py`'s `_gather_context` + `summarize_community_activity` build the seam via `build_community_summarize(store)` and call its methods in place of inline `store.structured_query(<cypher>)`. No schema change (the `Community` TAG already has report columns).

**Tech Stack:** Python 3.12, nebula3-python (inline nGQL), Neo4j Cypher, pytest.

## Global Constraints

- **Default neo4j SUMMARIZE path byte-for-byte unchanged.** With `GRAPH_BACKEND=neo4j`, the 3 ops issue IDENTICAL Cypher + params as today. Nebula reached only under `GRAPH_BACKEND=nebula`.
- `report_vec` is NOT written on the nebula `Community` vertex (Milvus owns it, via the already-dispatched `build_community_report_vector_store` path — UNTOUCHED here).
- Opt-in / strangler-fig. Local commits only (no push). Never stage `docs/bruno/collection.bru`. Unit tests DB-free (fake store recording statements).
- Nebula binds NO params: inline nGQL, values quoted via `nebula_store._q`; VIDs via `entity_vid` / `community_vid` (from `src.graph.community_writeback`).
- Mirror the merged `CommunityWriteback` seam shape — a parallel `CommunitySummarize` seam, NOT a merge into it.
- Fail-open unchanged: the existing try/except in `_gather_context` (`community.py:329-360`) and `summarize_community_activity` (`:459-475`) stay; seam methods may raise and are caught there.
- Interpreter `.venv/bin/python`; run pytest with `API_ENV=development`.

## File Structure

- **Create** `src/graph/community_summarize.py` — the 3 Cypher constants (canonical home), `CommunitySummarize` Protocol, `Neo4jCommunitySummarize`, `NebulaCommunitySummarize`, `build_community_summarize`.
- **Modify** `src/workflow/search/activities/community.py` — `_gather_context` + `summarize_community_activity` route through the seam; remove the 3 now-moved constants.
- **Create** `tests/test_graph/test_community_summarize.py` — neo4j parity + nebula nGQL + dispatch.
- **Modify** `tests/test_workflow/.../test_community*.py` (or the nearest existing community-activity test) — integration: routing through a fake seam. If no such test file exists, add the integration test to `tests/test_graph/test_community_summarize.py` using a fake store + monkeypatched `build_community_summarize`.

**Transitional duplication:** Task 1 DEFINES the 3 constants in `community_summarize.py` while `community.py` still keeps its own copies (untouched) — a deliberate transitional duplication so Task 1 stays purely additive. Task 3 removes `community.py`'s copies when it rewires to the seam. (Same pattern as the BUILD slice's Task 2→4.)

---

### Task 1: `CommunitySummarize` seam + Neo4j impl + dispatch (additive)

**Files:**
- Create: `src/graph/community_summarize.py`
- Test: `tests/test_graph/test_community_summarize.py`

**Interfaces:**
- Produces:
  - Constants `_MEMBER_CONTEXT_CYPHER`, `_CHILD_REPORTS_CYPHER`, `_WRITE_REPORT_CYPHER` (copied VERBATIM from `community.py:50-85`).
  - `CommunitySummarize` Protocol: `read_member_context(*, community_id, level) -> list[dict]`, `read_child_reports(*, community_id, level) -> list[dict]`, `write_report(*, community_id, level, report, title, summary, report_vec) -> None`.
  - `Neo4jCommunitySummarize(store)`, `NebulaCommunitySummarize(store)` (stub here, real in Task 2), `build_community_summarize(store) -> CommunitySummarize`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_graph/test_community_summarize.py`:

```python
from src.graph import community_summarize as cs


class _RecStore:
    def __init__(self, ret=None):
        self.calls = []
        self._ret = ret if ret is not None else []
    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._ret


def test_neo4j_read_member_context_issues_exact_cypher():
    store = _RecStore(ret=[{"name": "a", "description": "d", "rel_types": ["R"]}])
    wb = cs.Neo4jCommunitySummarize(store)
    rows = wb.read_member_context(community_id="7", level=0)
    assert store.calls == [(cs._MEMBER_CONTEXT_CYPHER, {"community_id": "7", "level": 0})]
    assert rows == [{"name": "a", "description": "d", "rel_types": ["R"]}]


def test_neo4j_read_child_reports_issues_exact_cypher():
    store = _RecStore(ret=[{"title": "t", "summary": "s"}])
    wb = cs.Neo4jCommunitySummarize(store)
    rows = wb.read_child_reports(community_id="7", level=1)
    assert store.calls == [(cs._CHILD_REPORTS_CYPHER, {"community_id": "7", "level": 1})]
    assert rows == [{"title": "t", "summary": "s"}]


def test_neo4j_write_report_issues_exact_cypher_and_params():
    store = _RecStore()
    wb = cs.Neo4jCommunitySummarize(store)
    wb.write_report(community_id="7", level=0, report="R", title="T",
                    summary="S", report_vec=[0.1])
    assert store.calls == [(cs._WRITE_REPORT_CYPHER, {
        "community_id": "7", "level": 0, "report": "R",
        "title": "T", "summary": "S", "report_vec": [0.1],
    })]


def test_dispatch_returns_neo4j_when_backend_not_nebula(monkeypatch):
    monkeypatch.setattr(cs.settings.graph, "backend", "neo4j")
    assert isinstance(cs.build_community_summarize(_RecStore()), cs.Neo4jCommunitySummarize)
```

- [ ] **Step 2: Run to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py -q`
Expected: FAIL (`ModuleNotFoundError: src.graph.community_summarize`).

- [ ] **Step 3: Create the module**

Copy the 3 constants VERBATIM from `src/workflow/search/activities/community.py:50-85` (`_MEMBER_CONTEXT_CYPHER`, `_CHILD_REPORTS_CYPHER`, `_WRITE_REPORT_CYPHER`) into the new module, then:

```python
"""Backend-dispatched community SUMMARIZE I/O (context reads + report write).

`Neo4jCommunitySummarize` wraps the existing Cypher constants verbatim
(default path, byte-for-byte). `NebulaCommunitySummarize` translates the same
ops to nGQL. `report_vec` is NOT written to the nebula vertex (Milvus owns it,
via the already-dispatched community report-vector store).
"""
from __future__ import annotations

from typing import Any, Protocol

from src.config import settings

# --- paste the 3 constants here, verbatim from community.py:50-85 ---
# _MEMBER_CONTEXT_CYPHER = """..."""
# _CHILD_REPORTS_CYPHER = """..."""
# _WRITE_REPORT_CYPHER = """..."""


class CommunitySummarize(Protocol):
    def read_member_context(self, *, community_id: str, level: int) -> list[dict]: ...
    def read_child_reports(self, *, community_id: str, level: int) -> list[dict]: ...
    def write_report(self, *, community_id: str, level: int, report: str,
                     title: str, summary: str, report_vec: list[float] | None) -> None: ...


class Neo4jCommunitySummarize:
    """Runs the historical Cypher constants verbatim — zero behaviour change."""

    def __init__(self, store: Any):
        self._store = store

    def _run(self, cypher: str, params: dict) -> list[dict]:
        return list(self._store.structured_query(cypher, param_map=params) or [])

    def read_member_context(self, *, community_id, level) -> list[dict]:
        return self._run(_MEMBER_CONTEXT_CYPHER, {"community_id": community_id, "level": level})

    def read_child_reports(self, *, community_id, level) -> list[dict]:
        return self._run(_CHILD_REPORTS_CYPHER, {"community_id": community_id, "level": level})

    def write_report(self, *, community_id, level, report, title, summary, report_vec) -> None:
        self._run(_WRITE_REPORT_CYPHER, {
            "community_id": community_id, "level": level, "report": report,
            "title": title, "summary": summary, "report_vec": report_vec,
        })


class NebulaCommunitySummarize:
    """nGQL community SUMMARIZE. Implemented in Task 2."""

    def __init__(self, store: Any):
        self._store = store

    def read_member_context(self, *, community_id, level) -> list[dict]:
        raise NotImplementedError("NebulaCommunitySummarize.read_member_context (Task 2)")

    def read_child_reports(self, *, community_id, level) -> list[dict]:
        raise NotImplementedError("NebulaCommunitySummarize.read_child_reports (Task 2)")

    def write_report(self, *, community_id, level, report, title, summary, report_vec) -> None:
        raise NotImplementedError("NebulaCommunitySummarize.write_report (Task 2)")


def build_community_summarize(store: Any) -> CommunitySummarize:
    if settings.graph.backend == "nebula":
        return NebulaCommunitySummarize(store)
    return Neo4jCommunitySummarize(store)
```

Do NOT modify `community.py` in this task (its own constant copies stay; transitional duplication resolved in Task 3).

- [ ] **Step 4: Run to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/graph/community_summarize.py tests/test_graph/test_community_summarize.py`

```bash
git add src/graph/community_summarize.py tests/test_graph/test_community_summarize.py
git commit -m "feat(community): CommunitySummarize seam + Neo4j impl (byte-for-byte) + dispatch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `NebulaCommunitySummarize` — nGQL implementation

**Files:**
- Modify: `src/graph/community_summarize.py` (replace the Task-1 stub body)
- Test: `tests/test_graph/test_community_summarize.py` (append nebula tests)

**Interfaces:**
- Consumes: `nebula_store._q`; `community_writeback.community_vid`; `nebula_store.entity_vid`; `store.structured_query(query)` (inline nGQL, no param_map).

**nGQL facts (verbatim):**
- Community VID = `community_vid(community_id, level)`. Entity VID = `entity_vid(name)`. Quote via `_q`.
- Members: `GO FROM "<cvid>" OVER \`IN_COMMUNITY\` REVERSELY YIELD src(edge) AS m;`
- Entity props: `FETCH PROP ON \`Entity\` "<m1>","<m2>" YIELD \`Entity\`.name AS name, \`Entity\`.description AS description;`
- Edges: `GO FROM "<m1>","<m2>" OVER \`RELATED\` BIDIRECT YIELD src(edge) AS s, dst(edge) AS d, \`RELATED\`.rel_type AS rt;`
- Children: `GO FROM "<cvid>" OVER \`PARENT_OF\` YIELD dst(edge) AS child;` then `FETCH PROP ON \`Community\` "<c1>" YIELD \`Community\`.title AS title, \`Community\`.summary AS summary, \`Community\`.report AS report, \`Community\`.member_count AS mc;`
- Write: `UPDATE VERTEX ON \`Community\` "<cvid>" SET report = <q>, title = <q>, summary = <q>, summarized_at = <int_ms>;` (PARTIAL update — preserves member_count/members_hash; `report_vec` NOT written).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_graph/test_community_summarize.py`:

```python
from src.graph.community_writeback import community_vid
from src.graph.nebula_store import entity_vid


class _RecNebula:
    """Fake nebula store: records nGQL; returns canned rows per substring."""
    def __init__(self, read_map=None):
        self.stmts = []
        self._read_map = read_map or {}
    def structured_query(self, query, param_map=None):
        assert not param_map, "nebula summarize must inline values"
        self.stmts.append(query)
        for needle, rows in self._read_map.items():
            if needle in query:
                return rows
        return []


def test_nebula_write_report_uses_update_vertex_no_report_vec():
    s = _RecNebula()
    wb = cs.NebulaCommunitySummarize(s)
    wb.write_report(community_id="7", level=0, report="R", title="T",
                    summary="S", report_vec=[0.1, 0.2])
    cvid = community_vid("7", 0)
    joined = "\n".join(s.stmts)
    assert f'UPDATE VERTEX ON `Community` "{cvid}"' in joined
    assert "INSERT VERTEX" not in joined              # partial update, not overwrite
    assert '"R"' in joined and '"T"' in joined and '"S"' in joined
    assert "summarized_at" in joined
    assert "report_vec" not in joined and "0.1" not in joined


def test_nebula_read_child_reports_filters_blank_and_sorts_by_member_count():
    cvid = community_vid("7", 1)
    ch_a, ch_b, ch_c = community_vid("a", 2), community_vid("b", 2), community_vid("c", 2)
    s = _RecNebula(read_map={
        'OVER `PARENT_OF`': [{"child": ch_a}, {"child": ch_b}, {"child": ch_c}],
        "FETCH PROP ON `Community`": [
            {"title": "A", "summary": "sa", "report": "ra", "mc": 5},
            {"title": "B", "summary": "sb", "report": "",   "mc": 9},   # blank report -> dropped
            {"title": "C", "summary": "sc", "report": "rc", "mc": 7},
        ],
    })
    wb = cs.NebulaCommunitySummarize(s)
    rows = wb.read_child_reports(community_id="7", level=1)
    assert rows == [{"title": "C", "summary": "sc"}, {"title": "A", "summary": "sa"}]  # mc desc, blank dropped


def test_nebula_read_member_context_intra_community_filter_and_cap():
    cvid = community_vid("7", 0)
    a, b, out = entity_vid("A"), entity_vid("B"), entity_vid("Outsider")
    s = _RecNebula(read_map={
        'OVER `IN_COMMUNITY` REVERSELY': [{"m": a}, {"m": b}],
        "FETCH PROP ON `Entity`": [
            {"name": "A", "description": "da"},
            {"name": "B", "description": "db"},
        ],
        'OVER `RELATED`': [
            {"s": a, "d": b, "rt": "KNOWS"},        # intra-community -> counts for A and B
            {"s": a, "d": out, "rt": "IGNORED"},    # edge to non-member -> excluded
        ],
    })
    wb = cs.NebulaCommunitySummarize(s)
    rows = wb.read_member_context(community_id="7", level=0)
    by_name = {r["name"]: r for r in rows}
    assert set(by_name) == {"A", "B"}
    assert rows == sorted(rows, key=lambda r: r["name"])   # ordered by name
    assert "KNOWS" in by_name["A"]["rel_types"] and "KNOWS" in by_name["B"]["rel_types"]
    assert "IGNORED" not in by_name["A"]["rel_types"]      # non-member edge excluded
    assert len(by_name["A"]["rel_types"]) <= 10
```

- [ ] **Step 2: Run to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py -q`
Expected: FAIL (`NotImplementedError`).

- [ ] **Step 3: Implement the nGQL methods**

Replace the `NebulaCommunitySummarize` stub body:

```python
class NebulaCommunitySummarize:
    """nGQL community SUMMARIZE. UPDATE VERTEX is a partial update (preserves
    BUILD's member_count/members_hash); report_vec is never written (Milvus).
    The intra-community edge filter for member context is done in Python."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def read_child_reports(self, *, community_id, level) -> list[dict]:
        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        child_rows = self._exec(f'GO FROM {_q(cvid)} OVER `PARENT_OF` YIELD dst(edge) AS child;')
        cvids = [r["child"] for r in child_rows if r.get("child")]
        if not cvids:
            return []
        listed = ", ".join(_q(v) for v in cvids)
        props = self._exec(
            f"FETCH PROP ON `Community` {listed} YIELD "
            "`Community`.title AS title, `Community`.summary AS summary, "
            "`Community`.report AS report, `Community`.member_count AS mc;"
        )
        kept = [r for r in props if (r.get("report") or "").strip()]
        kept.sort(key=lambda r: r.get("mc") or 0, reverse=True)
        return [{"title": r.get("title") or "", "summary": r.get("summary") or ""} for r in kept]

    def read_member_context(self, *, community_id, level) -> list[dict]:
        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        mrows = self._exec(f'GO FROM {_q(cvid)} OVER `IN_COMMUNITY` REVERSELY YIELD src(edge) AS m;')
        members = [r["m"] for r in mrows if r.get("m")]
        if not members:
            return []
        mset = set(members)
        listed = ", ".join(_q(v) for v in members)
        # YIELD id(vertex) so props can be keyed back to each member's VID.
        prop_rows = self._exec(
            f"FETCH PROP ON `Entity` {listed} YIELD id(vertex) AS vid, "
            "`Entity`.name AS name, `Entity`.description AS description;"
        )
        props = {r["vid"]: (r.get("name") or "", r.get("description") or "") for r in prop_rows}
        edges = self._exec(
            f"GO FROM {listed} OVER `RELATED` BIDIRECT YIELD "
            "src(edge) AS s, dst(edge) AS d, `RELATED`.rel_type AS rt;"
        )
        rel: dict[str, list[str]] = {v: [] for v in members}
        for e in edges:
            s_, d_, rt = e.get("s"), e.get("d"), e.get("rt")
            if s_ in mset and d_ in mset and rt:      # intra-community only
                for endpoint in (s_, d_):
                    if rt not in rel[endpoint]:
                        rel[endpoint].append(rt)
        out = []
        for v in members:
            name, desc = props.get(v, ("", ""))
            out.append({"name": name, "description": desc, "rel_types": rel[v][:10]})
        out.sort(key=lambda r: r["name"])
        return out

    def write_report(self, *, community_id, level, report, title, summary, report_vec) -> None:
        import time
        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        now = int(time.time() * 1000)
        # report_vec intentionally NOT written (Milvus owns it). UPDATE VERTEX
        # is a partial update, preserving member_count/members_hash from BUILD.
        self._exec(
            f'UPDATE VERTEX ON `Community` {_q(cvid)} SET '
            f"report = {_q(report)}, title = {_q(title)}, "
            f"summary = {_q(summary)}, summarized_at = {now};"
        )
```

- [ ] **Step 4: Run to verify it passes**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/graph/community_summarize.py tests/test_graph/test_community_summarize.py`

```bash
git add src/graph/community_summarize.py tests/test_graph/test_community_summarize.py
git commit -m "feat(nebula): NebulaCommunitySummarize nGQL (GO/FETCH reads, UPDATE VERTEX write)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Route `community.py` SUMMARIZE through the seam

**Files:**
- Modify: `src/workflow/search/activities/community.py` — `_gather_context` (`:322-360`), `summarize_community_activity` write (`:457-472`); remove the 3 moved constants (`:50-85`).
- Test: `tests/test_graph/test_community_summarize.py` (append integration) OR the nearest existing community-activity test.

**Interfaces:**
- Consumes: `build_community_summarize` from `src.graph.community_summarize`.

**Parity rule:** on neo4j the seam runs the same Cypher + params, so behaviour is identical.

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_graph/test_community_summarize.py`:

```python
import asyncio

import src.workflow.search.activities.community as cact


def test_gather_context_routes_member_read_through_seam(monkeypatch):
    captured = {}

    class _FakeSumm:
        def read_member_context(self, *, community_id, level):
            captured["member"] = (community_id, level)
            return [{"name": "a", "description": "d", "rel_types": ["R"]}]
        def read_child_reports(self, *, community_id, level):
            captured["child"] = (community_id, level)
            return []
        def write_report(self, **kw):
            captured["write"] = kw

    monkeypatch.setattr(cact, "build_community_summarize", lambda store: _FakeSumm())
    p = cact.SummarizeCommunityParams(community_id="7", level=0)
    body = asyncio.run(cact._gather_context(object(), p))
    assert captured["member"] == ("7", 0)
    assert isinstance(body, str) and body            # context string built from the rows
```

(If `SummarizeCommunityParams` needs more fields, construct it as the existing tests / dataclass definition require — read its definition in `community.py`.)

- [ ] **Step 2: Run to verify it fails**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py::test_gather_context_routes_member_read_through_seam -q`
Expected: FAIL (`AttributeError: ... has no attribute 'build_community_summarize'`).

- [ ] **Step 3: Rewire `_gather_context`**

At the top of `_gather_context`, add `summ = build_community_summarize(store)`. Replace the level>0 child fetch (`await asyncio.to_thread(store.structured_query, _CHILD_REPORTS_CYPHER, {...})`) with `await asyncio.to_thread(summ.read_child_reports, community_id=params.community_id, level=params.level)`, and the member fetch (`... store.structured_query, _MEMBER_CONTEXT_CYPHER, {...}`) with `await asyncio.to_thread(summ.read_member_context, community_id=params.community_id, level=params.level)`. Keep both try/except blocks and `_build_child_context`/`_build_member_context` unchanged.

- [ ] **Step 4: Rewire the write in `summarize_community_activity`**

Replace the `_WRITE_REPORT_CYPHER` call (`:459-471`):

```python
    persisted = False
    try:
        summ = build_community_summarize(store)
        await asyncio.to_thread(
            summ.write_report,
            community_id=params.community_id, level=params.level,
            report=json.dumps(report, ensure_ascii=False),
            title=title, summary=summary, report_vec=report_vec,
        )
        persisted = True
    except Exception as exc:
        ...  # unchanged warning
```

(The `report_vec` Milvus upsert block above is UNCHANGED.)

- [ ] **Step 5: Remove the moved constants + add the import**

Delete `_MEMBER_CONTEXT_CYPHER`, `_CHILD_REPORTS_CYPHER`, `_WRITE_REPORT_CYPHER` definitions from `community.py` (`:50-85`). Add `from src.graph.community_summarize import build_community_summarize` to the imports. Confirm no other reference to the 3 constants remains: `grep -n "_MEMBER_CONTEXT_CYPHER\|_CHILD_REPORTS_CYPHER\|_WRITE_REPORT_CYPHER" src/workflow/search/activities/community.py` → 0 lines.

- [ ] **Step 6: Run the integration test + the community-activity suite**

Run: `API_ENV=development .venv/bin/python -m pytest tests/test_graph/test_community_summarize.py tests/ -k "community" -q`
Expected: PASS (all community tests green — the neo4j path is unchanged).

- [ ] **Step 7: Ruff + commit**

Run: `.venv/bin/python -m ruff check src/workflow/search/activities/community.py tests/test_graph/test_community_summarize.py`

```bash
git add src/workflow/search/activities/community.py tests/test_graph/test_community_summarize.py
git commit -m "feat(community): route SUMMARIZE reads+write through CommunitySummarize seam

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Notes for the manual gate (post-merge, controller-run)

On the running nebula cluster with `GRAPH_BACKEND=nebula`, after BUILD materialises communities: drive `NebulaCommunitySummarize.write_report` then FETCH PROP the vertex — confirm report/title/summary/summarized_at land while member_count/members_hash are PRESERVED (UPDATE VERTEX, not overwrite); drive read_member_context on a small clique and read_child_reports on a 2-level pair.

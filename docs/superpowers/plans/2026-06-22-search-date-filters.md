# Search Date Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Filter search results by document date and insertion date, with both dates propagated into Milvus and Neo4j so the local + drift search modes filter on them.

**Architecture:** One stamping point (`parse_and_chunk`) writes two epoch-day ints (`doc_date_epoch`, `inserted_at_epoch`) into each chunk's `node.metadata`, which propagates to Milvus scalar fields and Neo4j `:Chunk` properties. At search time: vector retrieval pushes down `MetadataFilters`; graph (graph_search + graph_walk) results are post-filtered by the same metadata; `top_k` is over-fetched when a bound is set. Dates are client-provided at `/ingest`. Approach A from the spec (chunk-level dates; entities undated).

**Tech Stack:** Python 3.12, pydantic v2, FastAPI, Temporal (`temporalio`), llama-index (`MetadataFilters`), Milvus (HNSW), Neo4j, Postgres.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-22-search-date-filters-design.md`.
- Granularity is **DATE** (epoch-days int, UTC). No datetime.
- Canonical filterable fields: `doc_date_epoch`, `inserted_at_epoch` (int days since 1970-01-01).
- Document date is **client-provided** at `/ingest`; missing doc_date ⇒ chunk has no `doc_date_epoch` and is **excluded** by any document-date bound.
- Scope: filters apply to **local** and **drift** only; `global` keeps the reserved-warning behaviour.
- Determinism: epochs are computed at `/ingest` (outside the Temporal sandbox) and shipped via `IngestParams` — never compute dates inside `@workflow.run`.
- Run tests with `.venv/bin/python -m pytest`. Never commit/push without explicit user confirmation (project rule) — the "Commit" steps stage + commit locally only.

---

### Task 1: Pure date-filter helpers

**Files:**
- Create: `src/retrieval/date_filters.py`
- Test: `tests/test_retrieval/test_date_filters.py`

**Interfaces:**
- Produces:
  - `DOC_DATE_FIELD = "doc_date_epoch"`, `INSERTED_AT_FIELD = "inserted_at_epoch"` (str consts)
  - `iso_to_epoch_days(s: str) -> int` (raises `ValueError` on bad ISO)
  - `today_epoch_days() -> int`
  - `DateBounds` frozen dataclass: `doc_after, doc_before, ins_after, ins_before: int | None`; property `any_set: bool`
  - `bounds_from_iso(*, doc_after, doc_before, ins_after, ins_before: str | None) -> DateBounds` (raises `ValueError`)
  - `to_metadata_filters(b: DateBounds) -> MetadataFilters | None`
  - `node_metadata_in_range(md: dict, b: DateBounds) -> bool`
  - `filter_nodes(nodes: list, b: DateBounds) -> list` (drops out-of-range NodeWithScore; no-op when `not b.any_set`)
  - `overfetch_top_k(top_k: int, b: DateBounds, factor: int = 3) -> int`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_retrieval/test_date_filters.py
from __future__ import annotations

import pytest

from src.retrieval.date_filters import (
    DOC_DATE_FIELD, INSERTED_AT_FIELD, DateBounds, bounds_from_iso,
    filter_nodes, iso_to_epoch_days, node_metadata_in_range,
    overfetch_top_k, to_metadata_filters,
)


class _Node:
    def __init__(self, md): self.metadata = md
class _NWS:
    def __init__(self, md): self.node = _Node(md)


def test_iso_to_epoch_days_roundtrip():
    assert iso_to_epoch_days("1970-01-01") == 0
    assert iso_to_epoch_days("1970-01-02") == 1
    assert iso_to_epoch_days("2024-03-01") == 19783


def test_iso_to_epoch_days_rejects_bad():
    with pytest.raises(ValueError):
        iso_to_epoch_days("01-03-2024")


def test_bounds_from_iso_only_set_fields():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before=None,
                        ins_after=None, ins_before="2024-12-31")
    assert b.doc_after == iso_to_epoch_days("2024-01-01")
    assert b.doc_before is None
    assert b.ins_before == iso_to_epoch_days("2024-12-31")
    assert b.any_set is True
    assert DateBounds().any_set is False


def test_to_metadata_filters_builds_only_set_bounds():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before="2024-12-31",
                        ins_after=None, ins_before=None)
    mf = to_metadata_filters(b)
    keys = {(f.key, f.operator.value) for f in mf.filters}
    assert (DOC_DATE_FIELD, ">=") in keys
    assert (DOC_DATE_FIELD, "<=") in keys
    assert all(k[0] != INSERTED_AT_FIELD for k in keys)
    assert to_metadata_filters(DateBounds()) is None


def test_in_range_excludes_missing_and_out_of_range():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before="2024-12-31",
                        ins_after=None, ins_before=None)
    inside = {DOC_DATE_FIELD: iso_to_epoch_days("2024-06-01")}
    before = {DOC_DATE_FIELD: iso_to_epoch_days("2023-06-01")}
    missing = {"position": 0}
    assert node_metadata_in_range(inside, b) is True
    assert node_metadata_in_range(before, b) is False
    assert node_metadata_in_range(missing, b) is False  # missing field excluded
    assert node_metadata_in_range(missing, DateBounds()) is True  # no bound → keep


def test_filter_nodes_and_overfetch():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before=None,
                        ins_after=None, ins_before=None)
    nodes = [_NWS({DOC_DATE_FIELD: iso_to_epoch_days("2024-06-01")}),
             _NWS({DOC_DATE_FIELD: iso_to_epoch_days("2020-01-01")}),
             _NWS({"position": 1})]
    kept = filter_nodes(nodes, b)
    assert len(kept) == 1
    assert filter_nodes(nodes, DateBounds()) == nodes  # no-op
    assert overfetch_top_k(10, b) == 30
    assert overfetch_top_k(10, DateBounds()) == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_retrieval/test_date_filters.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.retrieval.date_filters'`

- [ ] **Step 3: Write the implementation**

```python
# src/retrieval/date_filters.py
"""Pure date-filter helpers for search (no infra).

Canonical filterable values are epoch-DAYS (int, UTC) stamped on each chunk's
``node.metadata`` at ingest. The same bounds drive a Milvus ``MetadataFilters``
push-down (vector) and a post-filter over graph/walk results — see the
2026-06-22-search-date-filters design.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from llama_index.core.vector_stores import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

DOC_DATE_FIELD = "doc_date_epoch"
INSERTED_AT_FIELD = "inserted_at_epoch"

_EPOCH = date(1970, 1, 1).toordinal()


def iso_to_epoch_days(s: str) -> int:
    """ISO ``YYYY-MM-DD`` → integer days since 1970-01-01. Raises ValueError."""
    return date.fromisoformat(s).toordinal() - _EPOCH


def today_epoch_days() -> int:
    return datetime.now(timezone.utc).date().toordinal() - _EPOCH


@dataclass(frozen=True)
class DateBounds:
    doc_after: int | None = None
    doc_before: int | None = None
    ins_after: int | None = None
    ins_before: int | None = None

    @property
    def any_set(self) -> bool:
        return any(b is not None for b in
                   (self.doc_after, self.doc_before, self.ins_after, self.ins_before))


def bounds_from_iso(
    *, doc_after: str | None = None, doc_before: str | None = None,
    ins_after: str | None = None, ins_before: str | None = None,
) -> DateBounds:
    """Convert optional ISO date strings → epoch-day bounds. Raises ValueError
    on a malformed date."""
    def _c(s: str | None) -> int | None:
        return iso_to_epoch_days(s) if s else None
    return DateBounds(_c(doc_after), _c(doc_before), _c(ins_after), _c(ins_before))


def to_metadata_filters(b: DateBounds) -> MetadataFilters | None:
    """Milvus push-down filter for whichever bounds are set (None if none)."""
    f: list[MetadataFilter] = []
    if b.doc_after is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_after, operator=FilterOperator.GTE))
    if b.doc_before is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_before, operator=FilterOperator.LTE))
    if b.ins_after is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_after, operator=FilterOperator.GTE))
    if b.ins_before is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_before, operator=FilterOperator.LTE))
    if not f:
        return None
    return MetadataFilters(filters=f, condition=FilterCondition.AND)


def _field_in_range(md: dict, field: str, lo: int | None, hi: int | None) -> bool:
    if lo is None and hi is None:
        return True
    v = md.get(field)
    if not isinstance(v, int):  # missing/non-int → excluded when a bound is set
        return False
    if lo is not None and v < lo:
        return False
    if hi is not None and v > hi:
        return False
    return True


def node_metadata_in_range(md: dict, b: DateBounds) -> bool:
    return (_field_in_range(md, DOC_DATE_FIELD, b.doc_after, b.doc_before)
            and _field_in_range(md, INSERTED_AT_FIELD, b.ins_after, b.ins_before))


def filter_nodes(nodes: list, b: DateBounds) -> list:
    """Drop NodeWithScore whose node.metadata dates fall outside bounds.
    No-op when no bound is set."""
    if not b.any_set:
        return list(nodes)
    return [n for n in nodes
            if node_metadata_in_range(getattr(n.node, "metadata", {}) or {}, b)]


def overfetch_top_k(top_k: int, b: DateBounds, factor: int = 3) -> int:
    """Over-fetch when filtering so post-filtered out-of-range hits don't
    starve the in-range result count."""
    return top_k * factor if b.any_set else top_k
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_retrieval/test_date_filters.py -q`
Expected: PASS (6 tests). If `FilterOperator.value` isn't `">="`/`"<="`, adjust the test's expected operator strings to the installed llama-index enum values (run `.venv/bin/python -c "from llama_index.core.vector_stores import FilterOperator; print(FilterOperator.GTE.value, FilterOperator.LTE.value)"`).

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/date_filters.py tests/test_retrieval/test_date_filters.py
git commit -m "feat(search): pure date-filter helpers (epoch-days, MetadataFilters, post-filter)"
```

---

### Task 2: Postgres `doc_date` column

**Files:**
- Modify: `scripts/setup_db.py` (the `_DOCUMENTS_DDL` block, ~line 34-54)
- Modify: `src/storage/postgres.py` (`insert_pending`, ~line 58-71)
- Test: `tests/test_storage/test_insert_pending_doc_date.py`

**Interfaces:**
- Produces: `AsyncPostgres.insert_pending(doc_id, path, department="", doc_type="", doc_date: str | None = None)` — writes `doc_date` (ISO `YYYY-MM-DD` or None) into `documents.doc_date`.

- [ ] **Step 1: Write the failing test** (asserts the SQL/signature carries doc_date; no live DB — patch the connection)

```python
# tests/test_storage/test_insert_pending_doc_date.py
from __future__ import annotations

import inspect

from src.storage.postgres import AsyncPostgres


def test_insert_pending_accepts_doc_date():
    sig = inspect.signature(AsyncPostgres.insert_pending)
    assert "doc_date" in sig.parameters


def test_insert_pending_sql_includes_doc_date():
    src = inspect.getsource(AsyncPostgres.insert_pending)
    assert "doc_date" in src  # column threaded into the INSERT


def test_documents_ddl_has_doc_date_column():
    import scripts.setup_db as sd
    assert "doc_date" in sd._DOCUMENTS_DDL
    assert "documents_doc_date_idx" in sd._DOCUMENTS_DDL
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_storage/test_insert_pending_doc_date.py -q`
Expected: FAIL (no `doc_date` param / not in DDL)

- [ ] **Step 3: Implement — DDL**

In `scripts/setup_db.py`, add the column + index inside `_DOCUMENTS_DDL` (after `summary` line, before `created_at`, and a new index after the department index):

```python
    summary      TEXT DEFAULT '',
    doc_date     DATE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status);

CREATE INDEX IF NOT EXISTS documents_department_idx
    ON documents (department);

CREATE INDEX IF NOT EXISTS documents_doc_date_idx
    ON documents (doc_date);
```

Note for existing DBs: the `CREATE TABLE IF NOT EXISTS` won't add a column to an already-created table. Add an idempotent migration next to the DDL (run in `setup_db`):

```python
_DOCUMENTS_MIGRATE = "ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date DATE;"
```
and execute it right after `cur.execute(_DOCUMENTS_DDL)` in the setup routine (find the existing `cur.execute(_DOCUMENTS_DDL)` call, ~line 156, and add `cur.execute(_DOCUMENTS_MIGRATE)` after it).

- [ ] **Step 4: Implement — insert_pending**

Replace `insert_pending` in `src/storage/postgres.py`:

```python
    async def insert_pending(
        self, doc_id: uuid.UUID, path: str,
        department: str = "", doc_type: str = "",
        doc_date: str | None = None,
    ) -> None:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO documents
                        (id, path, department, doc_type, doc_date, status)
                    VALUES (%s, %s, %s, %s, %s, 'pending')
                    """,
                    (str(doc_id), path, department, doc_type, doc_date),
                )
            await conn.commit()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_storage/test_insert_pending_doc_date.py -q`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add scripts/setup_db.py src/storage/postgres.py tests/test_storage/test_insert_pending_doc_date.py
git commit -m "feat(ingest): documents.doc_date column + insert_pending plumbing"
```

---

### Task 3: Ingest-side contracts (`IngestParams`, `Ctx`)

**Files:**
- Modify: `src/workflow/contracts.py` (`IngestParams` ~line 45, `Ctx` ~line 77)
- Test: `tests/test_workflow/test_contracts_date_fields.py`

**Interfaces:**
- Produces:
  - `IngestParams` gains `doc_date: str = ""`, `doc_date_epoch: int | None = None`, `inserted_at_epoch: int | None = None`
  - `Ctx` gains `doc_date_epoch: int | None = None`, `inserted_at_epoch: int | None = None`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_workflow/test_contracts_date_fields.py
from __future__ import annotations

from src.workflow.contracts import Ctx, IngestParams


def test_ingest_params_date_fields_default_empty():
    p = IngestParams(doc_id="d", path="/p")
    assert p.doc_date == ""
    assert p.doc_date_epoch is None
    assert p.inserted_at_epoch is None
    p2 = IngestParams(doc_id="d", path="/p", doc_date="2024-03-01",
                      doc_date_epoch=19783, inserted_at_epoch=20000)
    assert p2.doc_date_epoch == 19783


def test_ctx_date_fields_default_none():
    c = Ctx(doc_id="d", local_path="/p", cleanup_dir=None,
            workflow_run_id="r", doc_date_epoch=19783, inserted_at_epoch=20000)
    assert c.doc_date_epoch == 19783 and c.inserted_at_epoch == 20000
    c2 = Ctx(doc_id="d", local_path="/p", cleanup_dir=None, workflow_run_id="r")
    assert c2.doc_date_epoch is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_contracts_date_fields.py -q`
Expected: FAIL (unexpected keyword / attribute)

- [ ] **Step 3: Implement**

In `IngestParams` add after `force: bool = False`:

```python
    # Date filters (search). Client-provided document date + the ingest
    # timestamp, snapshotted at /ingest as epoch-DAYS (UTC) so the workflow
    # never computes dates (determinism). doc_date is the ISO string for the
    # Postgres write; the *_epoch ints are stamped onto chunks.
    doc_date: str = ""
    doc_date_epoch: int | None = None
    inserted_at_epoch: int | None = None
```

In `Ctx` add after `workflow_run_id: str`:

```python
    # Carried from IngestParams so parse_and_chunk can stamp chunks (search
    # date filters). Epoch-DAYS (UTC); None when the caller omitted doc_date.
    doc_date_epoch: int | None = None
    inserted_at_epoch: int | None = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_contracts_date_fields.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/workflow/contracts.py tests/test_workflow/test_contracts_date_fields.py
git commit -m "feat(ingest): date fields on IngestParams + Ctx"
```

---

### Task 4: `/ingest` accepts `document_date`

**Files:**
- Modify: `src/api/routes/ingest.py` (`upload_document` signature ~line 70, `insert_pending` call ~line 108, `IngestParams(...)` ~line 128)
- Test: `tests/test_api/test_ingest_doc_date.py`

**Interfaces:**
- Consumes: `IngestParams.doc_date/doc_date_epoch/inserted_at_epoch` (Task 3), `iso_to_epoch_days`/`today_epoch_days` (Task 1), `insert_pending(..., doc_date=...)` (Task 2).
- Produces: `/ingest` form field `document_date` (ISO `YYYY-MM-DD`, optional) → 422 on bad format; written to Postgres + shipped on IngestParams.

- [ ] **Step 1: Write the failing test** (extends the existing ingest test pattern — patch storage/pg/temporal client)

```python
# tests/test_api/test_ingest_doc_date.py
from __future__ import annotations

import inspect

from src.api.routes import ingest as ingest_mod


def test_upload_accepts_document_date_form_param():
    sig = inspect.signature(ingest_mod.upload_document)
    assert "document_date" in sig.parameters


def test_ingest_threads_doc_date_into_params_and_pg():
    src = inspect.getsource(ingest_mod.upload_document)
    # validated → epoch, shipped on IngestParams, written to Postgres
    assert "iso_to_epoch_days" in src
    assert "today_epoch_days" in src
    assert "doc_date=" in src            # insert_pending kwarg
    assert "doc_date_epoch=" in src      # IngestParams kwarg
    assert "inserted_at_epoch=" in src
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_api/test_ingest_doc_date.py -q`
Expected: FAIL

- [ ] **Step 3: Implement — imports + signature + validation + plumbing**

Add import near the top of `src/api/routes/ingest.py`:

```python
from src.retrieval.date_filters import iso_to_epoch_days, today_epoch_days
```

Add the form param to `upload_document` (after `force: bool = Form(default=False),`):

```python
    document_date: str | None = Form(default=None),
```

Right after `doc_id = uuid.uuid4()` (before the upload), validate + compute epochs:

```python
    # Document date is client-provided (ISO YYYY-MM-DD). Compute epochs here
    # (outside the Temporal sandbox) so the workflow never touches dates.
    doc_date_epoch: int | None = None
    if document_date:
        try:
            doc_date_epoch = iso_to_epoch_days(document_date)
        except ValueError as exc:
            raise HTTPException(422, "document_date must be ISO YYYY-MM-DD") from exc
    inserted_at_epoch = today_epoch_days()
```

Update the `insert_pending` call to pass `doc_date`:

```python
    await pg.insert_pending(
        doc_id, s3_uri, department=department, doc_type=doc_type,
        doc_date=document_date or None,
    )
```

Add the three fields to the `IngestParams(...)` construction (after `force=force,`):

```python
        doc_date=document_date or "",
        doc_date_epoch=doc_date_epoch,
        inserted_at_epoch=inserted_at_epoch,
```

- [ ] **Step 4: Run test to verify it passes + full ingest suite**

Run: `.venv/bin/python -m pytest tests/test_api/test_ingest_doc_date.py tests/test_api/test_ingest.py -q`
Expected: PASS (existing ingest tests still green)

- [ ] **Step 5: Commit**

```bash
git add src/api/routes/ingest.py tests/test_api/test_ingest_doc_date.py
git commit -m "feat(ingest): /ingest accepts document_date (validated → epoch, shipped + persisted)"
```

---

### Task 5: Stamp dates onto chunks (`fetch_source` Ctx + `parse_and_chunk`)

**Files:**
- Modify: `src/workflow/activities/fetch_source.py` (both `Ctx(...)` constructions, ~line 36 and ~line 61)
- Modify: `src/workflow/activities/parse_and_chunk.py` (metadata stamping loop, ~line 73-78)
- Test: `tests/test_workflow/test_parse_and_chunk_dates.py`

**Interfaces:**
- Consumes: `Ctx.doc_date_epoch/inserted_at_epoch` (Task 3), `DOC_DATE_FIELD`/`INSERTED_AT_FIELD` (Task 1).
- Produces: every chunk `node.metadata` carries `inserted_at_epoch` always (when set on ctx) and `doc_date_epoch` only when non-None.

- [ ] **Step 1: Write the failing test** (unit on the stamping helper — extract a pure stamper)

```python
# tests/test_workflow/test_parse_and_chunk_dates.py
from __future__ import annotations

from src.retrieval.date_filters import DOC_DATE_FIELD, INSERTED_AT_FIELD
from src.workflow.activities.parse_and_chunk import _stamp_dates


class _N:
    def __init__(self): self.metadata = {}


def test_stamp_dates_sets_both_when_present():
    n = _N()
    _stamp_dates([n], doc_date_epoch=19783, inserted_at_epoch=20000)
    assert n.metadata[DOC_DATE_FIELD] == 19783
    assert n.metadata[INSERTED_AT_FIELD] == 20000


def test_stamp_dates_omits_doc_date_when_none():
    n = _N()
    _stamp_dates([n], doc_date_epoch=None, inserted_at_epoch=20000)
    assert DOC_DATE_FIELD not in n.metadata
    assert n.metadata[INSERTED_AT_FIELD] == 20000


def test_stamp_dates_noop_when_inserted_none():
    n = _N()
    _stamp_dates([n], doc_date_epoch=None, inserted_at_epoch=None)
    assert n.metadata == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_parse_and_chunk_dates.py -q`
Expected: FAIL (no `_stamp_dates`)

- [ ] **Step 3: Implement — `_stamp_dates` + call it**

In `src/workflow/activities/parse_and_chunk.py`, add the helper near the top (module level) and import the field names:

```python
from src.retrieval.date_filters import DOC_DATE_FIELD, INSERTED_AT_FIELD


def _stamp_dates(nodes, *, doc_date_epoch: int | None, inserted_at_epoch: int | None) -> None:
    """Stamp search date-filter fields onto chunk metadata. doc_date is
    omitted when the caller didn't provide one (so a doc-date filter excludes
    these chunks); inserted_at is always present when known."""
    for n in nodes:
        md = getattr(n, "metadata", None)
        if md is None:
            md = n.metadata = {}
        if inserted_at_epoch is not None:
            md[INSERTED_AT_FIELD] = inserted_at_epoch
        if doc_date_epoch is not None:
            md[DOC_DATE_FIELD] = doc_date_epoch
```

Call it inside `parse_and_chunk`, right after the existing position/doc_id stamping loop (after the `md["doc_id"] = ctx.doc_id` loop, before the `_scrub` loop):

```python
    _stamp_dates(
        nodes,
        doc_date_epoch=ctx.doc_date_epoch,
        inserted_at_epoch=ctx.inserted_at_epoch,
    )
```

- [ ] **Step 4: Implement — carry dates through `Ctx` in `fetch_source`**

In `src/workflow/activities/fetch_source.py`, add to BOTH `Ctx(...)` constructions the two fields (the function already has `params`):

```python
        doc_date_epoch=params.doc_date_epoch,
        inserted_at_epoch=params.inserted_at_epoch,
```

- [ ] **Step 5: Run test + workflow suite**

Run: `.venv/bin/python -m pytest tests/test_workflow/test_parse_and_chunk_dates.py tests/test_workflow/test_parse_and_chunk.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/workflow/activities/parse_and_chunk.py src/workflow/activities/fetch_source.py tests/test_workflow/test_parse_and_chunk_dates.py
git commit -m "feat(ingest): stamp doc_date_epoch/inserted_at_epoch onto chunks (→ Milvus + Neo4j)"
```

---

### Task 6: Neo4j range indexes on chunk dates

**Files:**
- Modify: `src/graph/index.py` (add `ensure_chunk_date_indexes`, near `ensure_entity_lookup_indexes` ~line 85)
- Modify: `src/workflow/_search_deps.py` (`_build_graph_retriever_once` bootstrap, ~line 70-72 where `ensure_entity_lookup_indexes(gs)` is called)
- Modify: `src/workflow/activities/build_property_graph.py` (call ensure after upserts, near line 96-97 ensure calls)
- Test: `tests/test_graph/test_chunk_date_indexes.py`

**Interfaces:**
- Consumes: `DOC_DATE_FIELD`/`INSERTED_AT_FIELD` (Task 1).
- Produces: `ensure_chunk_date_indexes(store) -> bool` — fail-open DDL creating range indexes on `:Chunk(doc_date_epoch)` and `:Chunk(inserted_at_epoch)`.

> Note: PropertyGraphIndex labels chunk nodes `:Chunk` and persists `node.metadata` as properties. The post-filter (Task 8) does not require these indexes, but they keep any future Cypher date predicate scalable and are cheap/idempotent.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_graph/test_chunk_date_indexes.py
from __future__ import annotations

from src.graph.index import ensure_chunk_date_indexes


class _Store:
    def __init__(self): self.queries = []
    def structured_query(self, cypher, param_map=None):
        self.queries.append(cypher)
        return []


def test_creates_both_range_indexes():
    s = _Store()
    assert ensure_chunk_date_indexes(s) is True
    joined = "\n".join(s.queries)
    assert "doc_date_epoch" in joined
    assert "inserted_at_epoch" in joined
    assert joined.count("CREATE INDEX") == 2


def test_fail_open_on_error():
    class _Bad:
        def structured_query(self, *a, **k): raise RuntimeError("no")
    assert ensure_chunk_date_indexes(_Bad()) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_graph/test_chunk_date_indexes.py -q`
Expected: FAIL (no `ensure_chunk_date_indexes`)

- [ ] **Step 3: Implement**

In `src/graph/index.py`, add (mirroring `ensure_entity_lookup_indexes`):

```python
CHUNK_DOC_DATE_INDEX_CYPHER = (
    "CREATE INDEX chunk_doc_date IF NOT EXISTS "
    "FOR (c:Chunk) ON (c.doc_date_epoch)"
)
CHUNK_INSERTED_AT_INDEX_CYPHER = (
    "CREATE INDEX chunk_inserted_at IF NOT EXISTS "
    "FOR (c:Chunk) ON (c.inserted_at_epoch)"
)


def ensure_chunk_date_indexes(store) -> bool:
    """Idempotently create range indexes on chunk date fields (search date
    filters). Fail-open like ``ensure_entity_lookup_indexes``: errors are
    logged and swallowed. Returns True only if both succeeded."""
    ok = True
    for cypher in (CHUNK_DOC_DATE_INDEX_CYPHER, CHUNK_INSERTED_AT_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # broad by design — fail-open
            logger.warning("ensure_chunk_date_indexes failed: {e}", e=exc)
            ok = False
    return ok
```

- [ ] **Step 4: Wire into bootstrap + build path**

In `src/workflow/_search_deps.py`, add the import to the existing `from src.graph.index import (...)` block and call it next to `ensure_entity_lookup_indexes(gs)`:

```python
        ensure_entity_lookup_indexes(gs)
        ensure_chunk_date_indexes(gs)
```

In `src/workflow/activities/build_property_graph.py`, import `ensure_chunk_date_indexes` and call it next to the existing `ensure_*` calls (~line 96-97) so the index exists on the ingest side too:

```python
        ensure_chunk_date_indexes(graph_store)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_graph/test_chunk_date_indexes.py -q`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add src/graph/index.py src/workflow/_search_deps.py src/workflow/activities/build_property_graph.py tests/test_graph/test_chunk_date_indexes.py
git commit -m "feat(graph): Neo4j range indexes on :Chunk date fields (fail-open)"
```

---

### Task 7: Search-side contracts (request + workflow params)

**Files:**
- Modify: `src/models/search.py` (`SearchRequest`, ~line 50-53)
- Modify: `src/workflow/contracts.py` (`OrchestratorParams` ~line 433, `SubQueryParams` ~line 84, `RetrieveParams` ~line 361)
- Test: `tests/test_models/test_search_request_dates.py`

**Interfaces:**
- Produces:
  - `SearchRequest`: `created_after`/`created_before` change type `int | None` → `str | None` (ISO, insertion-date); new `doc_date_after`/`doc_date_before`: `str | None` (ISO, document-date).
  - `OrchestratorParams`, `SubQueryParams`, `RetrieveParams` each gain: `doc_date_after, doc_date_before, inserted_after, inserted_before: int | None = None` (epoch-days).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models/test_search_request_dates.py
from __future__ import annotations

from src.models.search import SearchRequest
from src.workflow.contracts import OrchestratorParams, RetrieveParams, SubQueryParams


def test_search_request_iso_date_fields():
    r = SearchRequest(query="q", created_after="2024-01-01",
                      doc_date_before="2024-12-31")
    assert r.created_after == "2024-01-01"
    assert r.doc_date_before == "2024-12-31"
    assert r.doc_date_after is None and r.created_before is None


def test_param_contracts_carry_epoch_bounds():
    for cls in (OrchestratorParams, RetrieveParams, SubQueryParams):
        fields = cls.model_fields
        for name in ("doc_date_after", "doc_date_before", "inserted_after", "inserted_before"):
            assert name in fields, f"{cls.__name__} missing {name}"
    rp = RetrieveParams(subquestion="s", doc_date_after=19783, inserted_before=20000)
    assert rp.doc_date_after == 19783 and rp.inserted_before == 20000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_models/test_search_request_dates.py -q`
Expected: FAIL

- [ ] **Step 3: Implement — SearchRequest**

In `src/models/search.py`, replace the two `created_*` fields and add two `doc_date_*` fields:

```python
    created_after: str | None = Field(
        default=None, description="Insertion-date lower bound (ISO YYYY-MM-DD); applied in local/drift.")
    created_before: str | None = Field(
        default=None, description="Insertion-date upper bound (ISO YYYY-MM-DD); applied in local/drift.")
    doc_date_after: str | None = Field(
        default=None, description="Document-date lower bound (ISO YYYY-MM-DD); applied in local/drift.")
    doc_date_before: str | None = Field(
        default=None, description="Document-date upper bound (ISO YYYY-MM-DD); applied in local/drift.")
```

- [ ] **Step 4: Implement — param contracts**

Add to `OrchestratorParams`, `SubQueryParams`, and `RetrieveParams` (each, after their existing fields):

```python
    # Search date filters (epoch-DAYS, UTC). None = no bound on that side.
    doc_date_after: int | None = None
    doc_date_before: int | None = None
    inserted_after: int | None = None
    inserted_before: int | None = None
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_models/test_search_request_dates.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/models/search.py src/workflow/contracts.py tests/test_models/test_search_request_dates.py
git commit -m "feat(search): ISO date filter fields on SearchRequest + epoch bounds on workflow params"
```

---

### Task 8: Thread bounds through search + apply filters

**Files:**
- Modify: `src/api/routes/search_v2.py` (`_RESERVED_FILTER_FIELDS` ~line 44, `_local_params` ~line 60)
- Modify: `src/workflow/search/orchestrator.py` (`SubQueryParams(...)` ~line 174; refinement `SubQueryParams(...)` ~line 253)
- Modify: `src/workflow/search/subquery_wf.py` (`RetrieveParams(...)` ~line 41)
- Modify: `src/workflow/_search_deps.py` (cache the vector index; add `get_vector_index()`)
- Modify: `src/workflow/search/activities/retrieve.py` (`retrieve_subquestion`: build filtered vector retriever, over-fetch, post-filter)
- Test: `tests/test_api/test_search_date_threading.py`, `tests/test_workflow/test_retrieve_date_filter.py`

**Interfaces:**
- Consumes: `bounds_from_iso`, `DateBounds`, `to_metadata_filters`, `filter_nodes`, `overfetch_top_k` (Task 1); the epoch-bound fields on the param contracts (Task 7); `SearchRequest` ISO fields (Task 7).
- Produces: `get_vector_index()` (cached `VectorStoreIndex`) in `_search_deps`; `_bounds_from_params(params) -> DateBounds` in `retrieve.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_api/test_search_date_threading.py
from __future__ import annotations

from src.api.routes.search_v2 import _RESERVED_FILTER_FIELDS, _local_params
from src.models.search import SearchRequest


def test_created_filters_no_longer_reserved():
    assert "created_after" not in _RESERVED_FILTER_FIELDS
    assert "created_before" not in _RESERVED_FILTER_FIELDS


def test_local_params_converts_iso_to_epoch_bounds():
    from src.retrieval.date_filters import iso_to_epoch_days
    req = SearchRequest(query="q", doc_date_after="2024-01-01",
                        created_before="2024-12-31")
    p = _local_params(req)
    assert p.doc_date_after == iso_to_epoch_days("2024-01-01")
    assert p.inserted_before == iso_to_epoch_days("2024-12-31")
    assert p.doc_date_before is None and p.inserted_after is None
```

```python
# tests/test_workflow/test_retrieve_date_filter.py
from __future__ import annotations

from src.retrieval.date_filters import DOC_DATE_FIELD, DateBounds, iso_to_epoch_days
from src.workflow.contracts import RetrieveParams
from src.workflow.search.activities.retrieve import _bounds_from_params


def test_bounds_from_params():
    rp = RetrieveParams(subquestion="s", doc_date_after=10, inserted_before=20)
    b = _bounds_from_params(rp)
    assert b == DateBounds(doc_after=10, doc_before=None,
                           ins_after=None, ins_before=20)
    assert _bounds_from_params(RetrieveParams(subquestion="s")).any_set is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_api/test_search_date_threading.py tests/test_workflow/test_retrieve_date_filter.py -q`
Expected: FAIL

- [ ] **Step 3: Implement — route**

In `src/api/routes/search_v2.py`: drop the two created fields from the reserved tuple:

```python
_RESERVED_FILTER_FIELDS = (
    "department", "user_id", "doc_type_filter",
)
```

Add import:

```python
from src.retrieval.date_filters import bounds_from_iso
```

In `_local_params`, convert the request ISO dates → epoch bounds and pass them on (validation: a bad ISO raises `ValueError` → wrap into 422):

```python
def _local_params(req: SearchRequest) -> OrchestratorParams:
    """Build the local plan-execute workflow input from the request."""
    _warn_reserved_filters(req)
    try:
        b = bounds_from_iso(
            doc_after=req.doc_date_after, doc_before=req.doc_date_before,
            ins_after=req.created_after, ins_before=req.created_before,
        )
    except ValueError as exc:
        raise HTTPException(422, "date filters must be ISO YYYY-MM-DD") from exc
    return OrchestratorParams(
        query=req.query,
        max_subqueries=settings.agent.max_subqueries,
        top_k=req.top_k,
        coverage_check_enabled=settings.agent.coverage_check_enabled,
        max_coverage_rounds=settings.agent.max_coverage_rounds,
        history=[
            ConversationTurnDict(role=t.role, content=t.content)
            for t in req.history
        ],
        contextualize_enabled=settings.agent.conversation_history_enabled,
        answer_template=req.answer_template or "",
        doc_date_after=b.doc_after, doc_date_before=b.doc_before,
        inserted_after=b.ins_after, inserted_before=b.ins_before,
    )
```

- [ ] **Step 4: Implement — orchestrator → SubQueryParams**

In `src/workflow/search/orchestrator.py`, both `SubQueryParams(...)` constructions (fan-out ~line 174 and the refinement gap ~line 253) must forward the bounds. Fan-out:

```python
                SubQueryParams(
                    subquestion=sub,
                    top_k=params.top_k,
                    doc_date_after=params.doc_date_after,
                    doc_date_before=params.doc_date_before,
                    inserted_after=params.inserted_after,
                    inserted_before=params.inserted_before,
                ),
```

Refinement (`SubQueryParams(subquestion=gap, top_k=params.top_k)`):

```python
                    SubQueryParams(
                        subquestion=gap, top_k=params.top_k,
                        doc_date_after=params.doc_date_after,
                        doc_date_before=params.doc_date_before,
                        inserted_after=params.inserted_after,
                        inserted_before=params.inserted_before,
                    ),
```

- [ ] **Step 5: Implement — SubQueryParams → RetrieveParams**

In `src/workflow/search/subquery_wf.py`, the `RetrieveParams(...)` construction:

```python
            RetrieveParams(
                subquestion=params.subquestion,
                top_k=params.top_k,
                doc_date_after=params.doc_date_after,
                doc_date_before=params.doc_date_before,
                inserted_after=params.inserted_after,
                inserted_before=params.inserted_before,
            ),
```

- [ ] **Step 6: Implement — cached vector index in `_search_deps`**

In `src/workflow/_search_deps.py`, refactor `_build_retriever_once` to also expose the index, and add `get_vector_index()`:

```python
async def _build_retriever_once():
    from src.ingestion.embeddings import build_embedding_model
    from src.retrieval.vector_index import build_vector_index, build_vector_store
    embed = build_embedding_model()
    store = build_vector_store()
    index = build_vector_index(store, embed)
    _state["_vector_index"] = index
    return index.as_retriever(similarity_top_k=10), embed


async def get_vector_index():
    """The cached VectorStoreIndex — callers build per-query retrievers with
    filters via index.as_retriever(filters=...)."""
    async with _lock:
        if _state.get("_vector_index") is None:
            ret, embed = await _build_retriever_once()
            _state["retriever"] = ret
            _state["_embed_model"] = embed
    return _state["_vector_index"]
```

(Ensure `_state` is initialised with `"_vector_index": None` alongside the other keys.)

- [ ] **Step 7: Implement — apply filters in `retrieve_subquestion`**

In `src/workflow/search/activities/retrieve.py`, add imports + helper + filter application. Imports:

```python
from src.retrieval.date_filters import (
    DateBounds, filter_nodes, overfetch_top_k, to_metadata_filters,
)
from src.workflow._search_deps import get_vector_index


def _bounds_from_params(p) -> DateBounds:
    return DateBounds(
        doc_after=p.doc_date_after, doc_before=p.doc_date_before,
        ins_after=p.inserted_after, ins_before=p.inserted_before,
    )
```

At the top of `retrieve_subquestion`, after computing `graph_retriever`, build the **filtered vector retriever** when bounds are set (replacing the plain `get_retriever()` for the vector path):

```python
    bounds = _bounds_from_params(params)
    if bounds.any_set:
        index = await get_vector_index()
        retriever = index.as_retriever(
            similarity_top_k=overfetch_top_k(params.top_k, bounds),
            filters=to_metadata_filters(bounds),
        )
    else:
        retriever = await get_retriever()
```

(Delete the original `retriever = await get_retriever()` line so it isn't built twice.)

Then, just before `sources = [node_to_serialized(n) for n in collected]`, post-filter graph/walk results (and re-cap to top_k):

```python
    # Graph (graph_search + graph_walk) results aren't push-down filtered —
    # post-filter the merged set by chunk-date metadata (vector hits already
    # satisfy the filter, so this is a no-op for them).
    collected = filter_nodes(collected, bounds)[: max(params.top_k, len(collected) if not bounds.any_set else params.top_k)]
```

Simplify the cap to: keep all when no bound, else cap to `top_k`:

```python
    collected = filter_nodes(collected, bounds)
    if bounds.any_set:
        collected = collected[: params.top_k]
```

- [ ] **Step 8: Run tests**

Run: `.venv/bin/python -m pytest tests/test_api/test_search_date_threading.py tests/test_workflow/test_retrieve_date_filter.py -q`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/api/routes/search_v2.py src/workflow/search/orchestrator.py src/workflow/search/subquery_wf.py src/workflow/_search_deps.py src/workflow/search/activities/retrieve.py tests/test_api/test_search_date_threading.py tests/test_workflow/test_retrieve_date_filter.py
git commit -m "feat(search): apply date filters in local retrieval (vector push-down + graph post-filter)"
```

---

### Task 9: Drift mode inherits the filter

**Files:**
- Modify: `src/api/routes/search_v2.py` (the drift route's local-leg param build — find where drift builds `OrchestratorParams`/calls `_local_params`)
- Test: `tests/test_api/test_drift_date_threading.py`

**Interfaces:**
- Consumes: `_local_params` (Task 8) — drift's local leg must reuse it so bounds flow into the local retrieval it seeds from.

- [ ] **Step 1: Inspect the drift route**

Read `search_drift` / `_drift_params` in `src/api/routes/search_v2.py` and `src/workflow/search/router_wf.py`. Confirm how `DriftSearchWorkflow` receives its local-leg params. The drift route MUST build the local-leg `OrchestratorParams` via `_local_params(req)` (which now carries bounds). If it constructs `OrchestratorParams` inline, switch it to `_local_params(req)` or copy the four bound kwargs.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_api/test_drift_date_threading.py
from __future__ import annotations

import inspect

from src.api.routes import search_v2


def test_drift_route_threads_date_bounds_into_local_leg():
    # The drift route must reuse _local_params (carries bounds) or set the
    # four epoch-bound kwargs on its local OrchestratorParams.
    src = inspect.getsource(search_v2)
    drift = src[src.index("async def search_drift"):]
    drift = drift[:drift.index("\n@router") if "\n@router" in drift else len(drift)]
    assert ("_local_params(" in drift) or ("doc_date_after=" in drift)
```

- [ ] **Step 3: Run test to verify it fails (or passes if drift already uses `_local_params`)**

Run: `.venv/bin/python -m pytest tests/test_api/test_drift_date_threading.py -q`
If it already passes (drift already calls `_local_params`), no code change needed — note that and skip to Step 5.

- [ ] **Step 4: Implement**

Make the drift route build its local-leg params via `_local_params(req)` (so the four bounds flow in). Concretely, where the drift route builds the local `OrchestratorParams`, replace the inline construction with `_local_params(req)` (the bounds are already in it).

- [ ] **Step 5: Run test + full search suite**

Run: `.venv/bin/python -m pytest tests/test_api/test_drift_date_threading.py "tests/test_workflow" -q`
Expected: PASS (pre-existing unrelated `test_search_community` failures may remain — confirm they're the `gamma` kwarg ones, not new).

- [ ] **Step 6: Commit**

```bash
git add src/api/routes/search_v2.py tests/test_api/test_drift_date_threading.py
git commit -m "feat(search): drift mode inherits date filters via local-leg params"
```

---

### Task 10: Docs + env reference

**Files:**
- Modify: `scripts/make_env.py` (no new env vars expected; only if a config knob is added — skip if none)
- Modify: `docs/scale_phase2_3.md` is unrelated; instead add a short note to API docs if present.
- Test: none (docs).

> Only needed if any operator-facing knob was added. This feature adds no env vars (dates are request/ingest params), so this task is typically a no-op — verify `.venv/bin/python -m scripts.make_env --check` still says OK and skip the commit if nothing changed.

- [ ] **Step 1: Run the drift guard + full regression**

Run: `.venv/bin/python -m pytest tests/test_retrieval/test_date_filters.py tests/test_workflow tests/test_api tests/test_models tests/test_storage tests/test_graph/test_chunk_date_indexes.py -q`
Expected: all new tests PASS; only the known pre-existing `test_search_community` failures (gamma kwarg) remain.

- [ ] **Step 2: env-check**

Run: `.venv/bin/python -m scripts.make_env --check`
Expected: `env check: OK`

---

## Self-Review

**Spec coverage:**
- doc_date column + insertion via created_at → Tasks 2, 4. ✓
- epoch-day canonical fields → Task 1. ✓
- IngestParams/Ctx date fields → Task 3. ✓
- /ingest document_date → Task 4. ✓
- single stamping point (parse_and_chunk) → Task 5. ✓
- Milvus propagation (automatic via metadata) → Task 5 (no index_vector change needed; verified small ints survive scrub). ✓
- Neo4j :Chunk properties + range indexes → Tasks 5 (write) + 6 (index). ✓
- SearchRequest ISO fields (created_* retyped + doc_date_*) → Task 7. ✓
- reserved-filter list update → Task 8. ✓
- vector push-down + graph post-filter + over-fetch → Tasks 1 (helpers) + 8 (wiring). ✓
- local + drift scope → Tasks 8 + 9; global untouched (still warns) ✓.
- error handling 422 → Tasks 4 + 8. ✓
- missing doc_date excluded → Task 1 (`_field_in_range`) + 5 (omit field). ✓
- testing (pure helpers) → Tasks 1, 5, 7, 8. ✓

**Placeholder scan:** none — every code step has full code.

**Type consistency:** epoch bounds are `int | None` on all three param contracts and on `DateBounds`; SearchRequest dates are `str | None` ISO; `bounds_from_iso` is the single ISO→epoch boundary; `_bounds_from_params` maps contract→`DateBounds`. Field names `doc_date_epoch`/`inserted_at_epoch` consistent across stamping (Task 5), helpers (Task 1), and indexes (Task 6).

**Open verification (call out during execution, not blockers):**
- Confirm `FilterOperator.GTE/LTE.value` strings for the test in Task 1.
- Confirm `MilvusVectorStore` honors `MetadataFilters` on dynamic int fields in the installed llama-index version (Task 8) — if it requires declaring scalar fields, add them where the store is built (`src/retrieval/vector_index.py`).
- Confirm the drift route's local-leg param construction site (Task 9 Step 1).

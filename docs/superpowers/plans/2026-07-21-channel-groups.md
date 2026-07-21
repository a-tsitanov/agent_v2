# Channel Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tag each document with its Telegram-folder group (news/analytics/digest/opinion/official/data) and use that label as a search filter, a rerank weight, and synthesis context.

**Architecture:** Mirror the existing `doc_date_epoch` date-filter machinery. The group string is stamped into each chunk's Milvus metadata (`doc_group`) at ingest, pushed down into the Milvus vector search as a `MetadataFilters`, mirrored as a graph/walk post-filter, applied as a per-group multiplier in the rerank activity, and prefixed onto each source's text at synthesis. Source of truth is the Telegram folder a channel sits in (read by `scripts/tg_ingest.py`).

**Tech Stack:** Python 3.12, FastAPI, Temporal, LlamaIndex (`MetadataFilters`), Milvus (dynamic fields), pydantic v2, pytest (`uv run --extra dev pytest`).

## Global Constraints

- Group enum is exactly: `news, analytics, digest, opinion, official, data` (lowercase). `""` = ungrouped.
- One group per doc (single string). Disjoint by construction; on multi-folder membership keep the first by `GROUP_PRIORITY`, log a WARNING.
- Never read `.env`/settings inside a `@workflow.run` body — resolve group params in `_local_params` (outside the sandbox), thread through contracts. Activities may read `settings` freely.
- Filter values pushed to Milvus are lowercase group strings; `""` is never sent as a filter value.
- No fixed-schema / no Milvus dim change — `doc_group` is a dynamic field (same mechanism as `doc_date_epoch`).
- Frequent commits: one per task minimum. Tests: `uv run --extra dev pytest`.

---

### Task 1: Group enum module

**Files:**
- Create: `src/retrieval/groups.py`
- Test: `tests/test_retrieval/test_groups.py`

**Interfaces:**
- Produces: `GROUPS: tuple[str, ...]`, `GROUP_SET: frozenset[str]`, `GROUP_PRIORITY: tuple[str, ...]`, `pick_priority(a: str, b: str) -> str` (returns whichever of two group names ranks earlier in `GROUP_PRIORITY`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_retrieval/test_groups.py
from src.retrieval.groups import GROUPS, GROUP_SET, GROUP_PRIORITY, pick_priority


def test_enum_is_the_six_groups():
    assert GROUPS == ("news", "analytics", "digest", "opinion", "official", "data")
    assert GROUP_SET == frozenset(GROUPS)
    assert "official" in GROUP_SET
    assert "sport" not in GROUP_SET


def test_pick_priority_returns_earlier_in_order():
    # order is news < analytics < digest < opinion < official < data
    assert pick_priority("official", "opinion") == "opinion"
    assert pick_priority("data", "news") == "news"
    assert pick_priority("news", "news") == "news"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_retrieval/test_groups.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.retrieval.groups'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/retrieval/groups.py
"""Canonical channel-group enum, imported by ingest/search/rerank/tg_ingest.

A document's group is a single lowercase string from GROUPS (or "" =
ungrouped). GROUP_PRIORITY is the tie-break order when a channel is found
in more than one group-folder (earlier wins).
"""
from __future__ import annotations

GROUPS: tuple[str, ...] = ("news", "analytics", "digest", "opinion", "official", "data")
GROUP_SET: frozenset[str] = frozenset(GROUPS)
GROUP_PRIORITY: tuple[str, ...] = GROUPS


def pick_priority(a: str, b: str) -> str:
    """Return whichever group name ranks earlier in GROUP_PRIORITY.
    Unknown names sort last (index = len)."""
    def rank(g: str) -> int:
        return GROUP_PRIORITY.index(g) if g in GROUP_SET else len(GROUP_PRIORITY)
    return a if rank(a) <= rank(b) else b
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_retrieval/test_groups.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/retrieval/groups.py tests/test_retrieval/test_groups.py
git commit -m "feat(groups): canonical channel-group enum"
```

---

### Task 2: Group metadata filter + combined-filter builder

**Files:**
- Create: `src/retrieval/group_filter.py`
- Modify: `src/retrieval/date_filters.py:59-72` (extract the date filter list so the combiner can reuse it)
- Test: `tests/test_retrieval/test_group_filter.py`

**Interfaces:**
- Consumes: `src.retrieval.groups.GROUP_SET`; `src.retrieval.date_filters.DateBounds`, `date_metadata_filters`.
- Produces:
  - `GROUP_FIELD = "doc_group"`
  - `GroupFilter(include: tuple[str, ...] = (), exclude: tuple[str, ...] = ())` with `.any_set: bool`
  - `group_metadata_filters(gf: GroupFilter) -> list[MetadataFilter]`
  - `node_group_ok(md: dict, gf: GroupFilter) -> bool`
  - `filter_nodes_by_group(nodes: list, gf: GroupFilter) -> list`
  - `combined_metadata_filters(b: DateBounds, gf: GroupFilter) -> MetadataFilters | None`
- Also produces (in `date_filters.py`): `date_metadata_filters(b: DateBounds) -> list[MetadataFilter]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_retrieval/test_group_filter.py
from types import SimpleNamespace

from llama_index.core.vector_stores import FilterOperator
from src.retrieval.date_filters import DateBounds
from src.retrieval.group_filter import (
    GROUP_FIELD,
    GroupFilter,
    combined_metadata_filters,
    filter_nodes_by_group,
    group_metadata_filters,
    node_group_ok,
)


def _node(group):
    return SimpleNamespace(node=SimpleNamespace(metadata={"doc_group": group}))


def test_include_builds_IN_filter():
    fs = group_metadata_filters(GroupFilter(include=("official", "data")))
    assert len(fs) == 1
    assert fs[0].key == GROUP_FIELD
    assert fs[0].operator == FilterOperator.IN
    assert fs[0].value == ["official", "data"]


def test_exclude_builds_NIN_filter():
    fs = group_metadata_filters(GroupFilter(exclude=("opinion",)))
    assert fs[0].operator == FilterOperator.NIN
    assert fs[0].value == ["opinion"]


def test_any_set_and_empty():
    assert GroupFilter().any_set is False
    assert group_metadata_filters(GroupFilter()) == []
    assert GroupFilter(include=("news",)).any_set is True


def test_node_group_ok_include_and_exclude():
    assert node_group_ok({"doc_group": "official"}, GroupFilter(include=("official",)))
    assert not node_group_ok({"doc_group": "opinion"}, GroupFilter(include=("official",)))
    assert not node_group_ok({"doc_group": "opinion"}, GroupFilter(exclude=("opinion",)))
    # missing group: excluded by an include-list, kept by a no-op filter
    assert not node_group_ok({}, GroupFilter(include=("official",)))
    assert node_group_ok({}, GroupFilter())


def test_filter_nodes_by_group_drops_out_of_set():
    nodes = [_node("official"), _node("opinion"), _node("data")]
    kept = filter_nodes_by_group(nodes, GroupFilter(include=("official", "data")))
    assert [n.node.metadata["doc_group"] for n in kept] == ["official", "data"]


def test_combined_filters_ANDs_date_and_group():
    mf = combined_metadata_filters(
        DateBounds(doc_after=50), GroupFilter(include=("official",)),
    )
    keys = {f.key for f in mf.filters}
    assert keys == {"doc_date_epoch", "doc_group"}


def test_combined_filters_none_when_nothing_set():
    assert combined_metadata_filters(DateBounds(), GroupFilter()) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_retrieval/test_group_filter.py -v`
Expected: FAIL with `ImportError`/`ModuleNotFoundError` (group_filter + `date_metadata_filters` missing).

- [ ] **Step 3: Refactor `date_filters.py` to expose the date filter list**

In `src/retrieval/date_filters.py`, replace the body of `to_metadata_filters` (lines 59-72) with a reusable list builder:

```python
def date_metadata_filters(b: DateBounds) -> list[MetadataFilter]:
    """The per-bound Milvus MetadataFilter list (empty when no bound set)."""
    f: list[MetadataFilter] = []
    if b.doc_after is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_after, operator=FilterOperator.GTE))
    if b.doc_before is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_before, operator=FilterOperator.LTE))
    if b.ins_after is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_after, operator=FilterOperator.GTE))
    if b.ins_before is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_before, operator=FilterOperator.LTE))
    return f


def to_metadata_filters(b: DateBounds) -> MetadataFilters | None:
    """Milvus push-down filter for whichever bounds are set (None if none)."""
    f = date_metadata_filters(b)
    if not f:
        return None
    return MetadataFilters(filters=f, condition=FilterCondition.AND)
```

- [ ] **Step 4: Write `group_filter.py`**

```python
# src/retrieval/group_filter.py
"""Channel-group search filter — the doc_group analogue of date_filters.

`doc_group` is stamped on each chunk's node.metadata at ingest. The same
GroupFilter drives a Milvus MetadataFilters push-down (vector) and a
post-filter over graph/walk results (which don't go through Milvus).
"""
from __future__ import annotations

from dataclasses import dataclass

from llama_index.core.vector_stores import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

from src.retrieval.date_filters import DateBounds, date_metadata_filters

GROUP_FIELD = "doc_group"


@dataclass(frozen=True)
class GroupFilter:
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @property
    def any_set(self) -> bool:
        return bool(self.include or self.exclude)


def group_metadata_filters(gf: GroupFilter) -> list[MetadataFilter]:
    f: list[MetadataFilter] = []
    if gf.include:
        f.append(MetadataFilter(key=GROUP_FIELD, value=list(gf.include), operator=FilterOperator.IN))
    if gf.exclude:
        f.append(MetadataFilter(key=GROUP_FIELD, value=list(gf.exclude), operator=FilterOperator.NIN))
    return f


def node_group_ok(md: dict, gf: GroupFilter) -> bool:
    if not gf.any_set:
        return True
    g = md.get(GROUP_FIELD)
    if gf.include and g not in gf.include:
        return False
    return not (gf.exclude and g in gf.exclude)


def filter_nodes_by_group(nodes: list, gf: GroupFilter) -> list:
    """Drop NodeWithScore whose node.metadata[doc_group] is out of the
    include-set / in the exclude-set. No-op when nothing set."""
    if not gf.any_set:
        return list(nodes)
    return [n for n in nodes
            if node_group_ok(getattr(n.node, "metadata", {}) or {}, gf)]


def combined_metadata_filters(b: DateBounds, gf: GroupFilter) -> MetadataFilters | None:
    """AND the date-bound filters with the group filter into ONE push-down
    (None when neither is set)."""
    filters = date_metadata_filters(b) + group_metadata_filters(gf)
    if not filters:
        return None
    return MetadataFilters(filters=filters, condition=FilterCondition.AND)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_retrieval/test_group_filter.py tests/test_retrieval/test_date_filters.py -v`
Expected: PASS (group_filter tests + existing date-filter tests still green after the refactor)

- [ ] **Step 6: Commit**

```bash
git add src/retrieval/group_filter.py src/retrieval/date_filters.py tests/test_retrieval/test_group_filter.py
git commit -m "feat(groups): GroupFilter + combined metadata-filter builder"
```

---

### Task 3: Ingest-side plumbing (group → chunk metadata)

**Files:**
- Modify: `src/workflow/contracts.py` (`IngestParams`, `Ctx`)
- Modify: `src/workflow/activities/fetch_source.py:36-43,63-70` (propagate into both `Ctx(...)` sites)
- Modify: `src/workflow/activities/parse_and_chunk.py:82-85` (stamp `md["doc_group"]`)
- Modify: `src/api/routes/ingest.py:59-67,156-176` (form param + validation + IngestParams)
- Test: `tests/test_workflow/test_parse_and_chunk_group.py`, `tests/test_api/test_ingest_group.py`

**Interfaces:**
- Consumes: `src.retrieval.groups.GROUP_SET`.
- Produces: `IngestParams.group: str`, `Ctx.group: str`; chunk `node.metadata["doc_group"]` set when group non-empty; `/ingest` `group` form field validated to `GROUP_SET ∪ {""}`.

- [ ] **Step 1: Write the failing test (metadata stamping is pure enough to test on Ctx→md logic)**

```python
# tests/test_workflow/test_parse_and_chunk_group.py
from src.workflow.contracts import Ctx, IngestParams


def test_contracts_carry_group_default_empty():
    assert IngestParams(doc_id="d", path="s3://x").group == ""
    assert Ctx(doc_id="d", local_path="/p", cleanup_dir=None,
               workflow_run_id="r").group == ""


def test_contracts_accept_group():
    p = IngestParams(doc_id="d", path="s3://x", group="official")
    assert p.group == "official"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_workflow/test_parse_and_chunk_group.py -v`
Expected: FAIL — `IngestParams`/`Ctx` reject unexpected `group` kwarg (frozen models) → `ValidationError`.

- [ ] **Step 3: Add `group` to the contracts**

In `src/workflow/contracts.py`, add to `IngestParams` (after `inserted_at_epoch`):

```python
    # Channel group (news/analytics/.../data or "" = ungrouped). Stamped
    # onto each chunk's doc_group metadata for search filter/rerank/synth.
    group: str = ""
```

And to `Ctx` (after `inserted_at_epoch`):

```python
    group: str = ""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_workflow/test_parse_and_chunk_group.py -v`
Expected: PASS

- [ ] **Step 5: Propagate group through `fetch_source` + stamp in `parse_and_chunk`**

In `src/workflow/activities/fetch_source.py`, add `group=params.group,` to BOTH `Ctx(...)` constructions (the legacy-path one at ~L36-43 and the s3 one at ~L63-70), immediately after `inserted_at_epoch=params.inserted_at_epoch,`.

In `src/workflow/activities/parse_and_chunk.py`, after the `inserted_at_epoch` block (currently L84-85), add:

```python
        if ctx.group:
            md["doc_group"] = ctx.group
```

- [ ] **Step 6: Add the `/ingest` form param + validation**

In `src/api/routes/ingest.py`:

Add the import near the top (with the other `src.retrieval` imports):

```python
from src.retrieval.groups import GROUP_SET
```

Add the form param to `upload_document`'s signature (after `queue`):

```python
    group: str = Form(default=""),
```

After the `queue` validation block (~L81), add:

```python
    # Optional channel group. Validate up front → 422 on an unknown name.
    if group and group not in GROUP_SET:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"unknown group {group!r}; allowed: {sorted(GROUP_SET)}",
        )
```

Add `group=group,` to the `IngestParams(...)` construction (~L156-176), after `force=force,`.

- [ ] **Step 7: Write the API validation test**

```python
# tests/test_api/test_ingest_group.py
import io

from fastapi.testclient import TestClient
from src.api.app import build_app  # adjust to the repo's app factory


def _client():
    return TestClient(build_app())


def _headers():
    from src.config import settings
    return {"X-API-Key": settings.api.keys[0]}  # adjust accessor to repo


def test_unknown_group_422():
    c = _client()
    r = c.post(
        "/api/v1/ingest",
        headers=_headers(),
        files={"file": ("t.txt", io.BytesIO(b"hi"), "text/plain")},
        data={"group": "sport"},
    )
    assert r.status_code == 422
    assert "unknown group" in r.text
```

> NOTE: match `build_app` / API-key accessor to the repo's actual test
> harness — grep an existing `tests/test_api/*` for the real import + key
> fixture and copy it verbatim. If MinIO/Temporal aren't available in the
> test env, assert only the 422 path (validation happens before any upload).

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_workflow/test_parse_and_chunk_group.py tests/test_api/test_ingest_group.py -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/workflow/contracts.py src/workflow/activities/fetch_source.py src/workflow/activities/parse_and_chunk.py src/api/routes/ingest.py tests/test_workflow/test_parse_and_chunk_group.py tests/test_api/test_ingest_group.py
git commit -m "feat(groups): thread group through ingest into chunk doc_group metadata"
```

---

### Task 4: Search-side param plumbing (API → contracts → workflows)

**Files:**
- Modify: `src/models/search.py:22-91` (`SearchRequest` fields + validator)
- Modify: `src/api/routes/search_v2.py:64-91` (`_local_params`)
- Modify: `src/workflow/contracts.py` (`OrchestratorParams`, `SubQueryParams`, `RetrieveParams`)
- Modify: `src/workflow/search/orchestrator.py:175-182,259-266` (both `SubQueryParams(...)` sites)
- Modify: `src/workflow/search/subquery_wf.py:43-50` (`RetrieveParams(...)`)
- Test: `tests/test_api/test_search_groups_params.py`

**Interfaces:**
- Consumes: `src.retrieval.groups.GROUP_SET`.
- Produces: `SearchRequest.groups: list[str] | None`, `SearchRequest.exclude_groups: list[str] | None`; `OrchestratorParams.groups/.exclude_groups: list[str]`, same on `SubQueryParams` and `RetrieveParams` (default `[]`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_api/test_search_groups_params.py
import pytest
from pydantic import ValidationError

from src.models.search import SearchRequest
from src.api.routes.search_v2 import _local_params


def test_valid_groups_accepted_and_threaded():
    req = SearchRequest(query="q", groups=["official", "data"])
    p = _local_params(req)
    assert p.groups == ["official", "data"]
    assert p.exclude_groups == []


def test_unknown_group_rejected():
    with pytest.raises(ValidationError):
        SearchRequest(query="q", groups=["sport"])


def test_include_and_exclude_mutually_exclusive():
    with pytest.raises(ValidationError):
        SearchRequest(query="q", groups=["official"], exclude_groups=["opinion"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_api/test_search_groups_params.py -v`
Expected: FAIL — `SearchRequest` has no `groups` field (ignored/rejected) and `_local_params` doesn't set it.

- [ ] **Step 3: Add fields + validator to `SearchRequest`**

In `src/models/search.py`, add to `SearchRequest` (after `answer_template`, ~L75):

```python
    groups: list[str] | None = Field(
        default=None,
        description="Include-list of channel groups; None/[] = all groups.",
    )
    exclude_groups: list[str] | None = Field(
        default=None,
        description="Exclude-list of channel groups (mutually exclusive with groups).",
    )
```

Add a validator (below the existing `_validate_iso_date`):

```python
    @field_validator("groups", "exclude_groups")
    @classmethod
    def _validate_groups(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        from src.retrieval.groups import GROUP_SET
        bad = [g for g in v if g not in GROUP_SET]
        if bad:
            raise ValueError(f"unknown group(s) {bad}; allowed: {sorted(GROUP_SET)}")
        return v

    @model_validator(mode="after")
    def _groups_xor_exclude(self):
        if self.groups and self.exclude_groups:
            raise ValueError("groups and exclude_groups are mutually exclusive")
        return self
```

Add `model_validator` to the pydantic import line (`from pydantic import BaseModel, Field, field_validator, model_validator`).

- [ ] **Step 4: Thread through `_local_params`**

In `src/api/routes/search_v2.py`, `_local_params`, add to the `OrchestratorParams(...)` return (after `inserted_before_epoch=b.ins_before,`):

```python
        groups=req.groups or [],
        exclude_groups=req.exclude_groups or [],
```

- [ ] **Step 5: Add fields to the three contracts**

In `src/workflow/contracts.py` add to `RetrieveParams` (after `inserted_before_epoch`), `SubQueryParams` (after `inserted_before_epoch`), and `OrchestratorParams` (after `inserted_before_epoch`):

```python
    # Channel-group filter (include / exclude lists; empty = all groups).
    groups: list[str] = Field(default_factory=list)
    exclude_groups: list[str] = Field(default_factory=list)
```

- [ ] **Step 6: Thread through the workflow construction sites**

In `src/workflow/search/orchestrator.py`, both `SubQueryParams(...)` constructions (~L175-182 and ~L259-266), add after `inserted_before_epoch=params.inserted_before_epoch,`:

```python
                    groups=params.groups,
                    exclude_groups=params.exclude_groups,
```

(match the indentation of each site).

In `src/workflow/search/subquery_wf.py`, the `RetrieveParams(...)` construction (~L43-50), add after `inserted_before_epoch=params.inserted_before_epoch,`:

```python
                groups=params.groups,
                exclude_groups=params.exclude_groups,
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_api/test_search_groups_params.py -v`
Expected: PASS (3 tests)

- [ ] **Step 8: Commit**

```bash
git add src/models/search.py src/api/routes/search_v2.py src/workflow/contracts.py src/workflow/search/orchestrator.py src/workflow/search/subquery_wf.py tests/test_api/test_search_groups_params.py
git commit -m "feat(groups): thread group filter through search API + workflow contracts"
```

---

### Task 5: Retrieve activity — group push-down + post-filter

**Files:**
- Modify: `src/workflow/search/activities/retrieve.py:24-29,115-216`
- Test: `tests/test_workflow/test_search_retrieve_groups.py`

**Interfaces:**
- Consumes: `RetrieveParams.groups/.exclude_groups`; `src.retrieval.group_filter.{GroupFilter, combined_metadata_filters, filter_nodes_by_group}`; existing `get_vector_retriever(top_k, filters=...)`.
- Produces: retrieval that (a) pushes the combined date+group filter into the Milvus vector search and (b) post-filters graph/walk nodes by group.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_workflow/test_search_retrieve_groups.py
import asyncio
from types import SimpleNamespace

import src.workflow.search.activities.retrieve as R
from src.retrieval.group_filter import GroupFilter, combined_metadata_filters
from src.retrieval.date_filters import DateBounds
from src.workflow.contracts import RetrieveParams


def test_group_filter_pushed_into_vector_retriever(monkeypatch):
    seen = {}

    async def _vret(top_k, filters=None):
        seen["top_k"] = top_k
        seen["filters"] = filters
        async def _retrieve(q): return []
        return SimpleNamespace(aretrieve=_retrieve)

    async def _greter():  # graph retriever unused here
        return None

    monkeypatch.setattr(R, "get_vector_retriever", _vret)
    monkeypatch.setattr(R, "get_graph_retriever", _greter)
    # Make the tool pipeline a no-op so only retriever construction matters.
    async def _dispatch(*a, **k):
        return SimpleNamespace(observation="{}", sources=[])
    monkeypatch.setattr(R.atomic_tools, "dispatch", _dispatch)

    params = RetrieveParams(subquestion="q", top_k=10, groups=["official"])
    asyncio.run(R.retrieve_subquestion(params))

    expected = combined_metadata_filters(DateBounds(), GroupFilter(include=("official",)))
    assert seen["filters"] == expected
    assert seen["top_k"] > 10  # over-fetched because a filter is set
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_workflow/test_search_retrieve_groups.py -v`
Expected: FAIL — `retrieve_subquestion` ignores `groups`; `seen["filters"]` is `None` (or the retriever isn't the vector one).

- [ ] **Step 3: Wire the group filter into `retrieve_subquestion`**

In `src/workflow/search/activities/retrieve.py`:

Extend the imports (L24-29 block) to add the group helpers and the combiner:

```python
from src.retrieval.date_filters import (
    DateBounds,
    filter_nodes,
    overfetch_top_k,
)
from src.retrieval.group_filter import (
    GroupFilter,
    combined_metadata_filters,
    filter_nodes_by_group,
)
```

(`to_metadata_filters` is no longer used directly here — replaced by `combined_metadata_filters`.)

Replace the retriever-selection block (currently L119-135) with:

```python
    bounds = DateBounds(
        doc_after=params.doc_date_after_epoch,
        doc_before=params.doc_date_before_epoch,
        ins_after=params.inserted_after_epoch,
        ins_before=params.inserted_before_epoch,
    )
    gf = GroupFilter(
        include=tuple(params.groups),
        exclude=tuple(params.exclude_groups),
    )
    if bounds.any_set or gf.any_set:
        # Push date AND group INTO the Milvus vector search (not just the
        # post-filter below): otherwise the vector top-k is chosen across
        # ALL docs and the post-filter starves the in-scope pool.
        retriever = await get_vector_retriever(
            overfetch_top_k(params.top_k, bounds) if bounds.any_set
            else params.top_k * 3,
            filters=combined_metadata_filters(bounds, gf),
        )
    else:
        retriever = await get_retriever()
```

Replace the post-filter block (currently L209-215) with:

```python
    if bounds.any_set or gf.any_set:
        before = len(collected)
        collected = filter_nodes(collected, bounds)
        collected = filter_nodes_by_group(collected, gf)
        activity.logger.info(
            "retrieve_subquestion  scope-filter kept %d/%d",
            len(collected), before,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_workflow/test_search_retrieve_groups.py tests/test_workflow/test_search_retrieve.py -v`
Expected: PASS (new group test + existing date-filter retrieve tests still green)

- [ ] **Step 5: Commit**

```bash
git add src/workflow/search/activities/retrieve.py tests/test_workflow/test_search_retrieve_groups.py
git commit -m "feat(groups): push group filter into vector search + graph post-filter"
```

---

### Task 6: Rerank group weights

**Files:**
- Modify: `src/config.py` (`AgentSettings.group_weights`, after `graph_similarity_top_k` ~L717)
- Modify: `src/workflow/_search_deps.py:207-232` (`get_reranker` builds a score-all instance)
- Modify: `src/workflow/search/activities/rerank.py`
- Test: `tests/test_workflow/test_rerank_group_weights.py`

**Interfaces:**
- Consumes: `settings.agent.group_weights: dict[str, float]`; `SerializedNode.score/.metadata`.
- Produces: `apply_group_weights(sources: list[SerializedNode], weights: dict[str, float]) -> list[SerializedNode]` (pure — multiplies each score by `weights.get(doc_group, 1.0)`, returns re-sorted desc); rerank activity scores the full pool, weights, then cuts to `top_n`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_workflow/test_rerank_group_weights.py
from src.workflow.contracts import SerializedNode
from src.workflow.search.activities.rerank import apply_group_weights


def _n(cid, score, group):
    return SerializedNode(chunk_id=cid, text=cid, score=score, metadata={"doc_group": group})


def test_weight_reorders_by_group():
    # opinion starts higher but is down-weighted; official is boosted.
    pool = [_n("op", 1.00, "opinion"), _n("of", 0.90, "official")]
    weights = {"opinion": 0.8, "official": 1.3}
    out = apply_group_weights(pool, weights)
    assert [n.chunk_id for n in out] == ["of", "op"]  # 0.90*1.3=1.17 > 1.00*0.8=0.80


def test_missing_group_weight_is_identity():
    pool = [_n("a", 0.5, ""), _n("b", 0.4, "news")]
    out = apply_group_weights(pool, {"official": 1.3})
    assert [n.chunk_id for n in out] == ["a", "b"]  # order unchanged (both ×1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_workflow/test_rerank_group_weights.py -v`
Expected: FAIL — `apply_group_weights` not defined.

- [ ] **Step 3: Add the config field**

In `src/config.py`, add to `AgentSettings` (after `graph_similarity_top_k`, ~L717):

```python
    # Per-group rerank multipliers (Search R5+). Applied to the cross-encoder
    # score before the top_n cut; a missing group (or "") → 1.0. Override via
    # AGENT_GROUP_WEIGHTS (JSON).
    group_weights: dict[str, float] = Field(default_factory=lambda: {
        "official": 1.30, "data": 1.25, "analytics": 1.10,
        "news": 1.00, "digest": 0.95, "opinion": 0.80,
    })
```

- [ ] **Step 4: Make `get_reranker` score the whole pool (no early cut)**

In `src/workflow/_search_deps.py`, add a module constant near the top of the file:

```python
# The unified rerank now owns the top_n cut (after group weighting), so the
# cached cross-encoder is built as a pure SCORER that never truncates below
# this cap. Pools are bounded (top_k × sub-questions + graph + walk); 256 is
# comfortably above any real pool.
_RERANK_SCORE_CAP = 256
```

In `get_reranker`, change the build call (currently ~L229-231) to always size the instance to the cap (ignore the small per-call `top_n` for cutting):

```python
                _state["reranker"] = build_reranker(top_n=_RERANK_SCORE_CAP)
```

(Leave the `top_n` parameter on `get_reranker` for signature compatibility; it is no longer used for the cut.) The local `from src.config import settings` inside `get_reranker` was only used for `settings.temporal.rerank_top_n` — **remove that now-unused import line** (the `from src.retrieval.reranker import build_reranker` line stays). This is an intended behaviour change: the cached reranker now returns up to `_RERANK_SCORE_CAP` scored nodes instead of pre-cutting to `rerank_top_n`; the activity owns the final cut.

- [ ] **Step 5: Apply weights in the rerank activity**

In `src/workflow/search/activities/rerank.py`, add the import and helper, and apply after scoring:

```python
from src.config import settings
```

Add the pure helper (below `prepare_rerank_pool`):

```python
def apply_group_weights(
    sources: list[SerializedNode], weights: dict[str, float],
) -> list[SerializedNode]:
    """Multiply each source's rerank score by its group weight (missing
    group / "" → 1.0) and return the pool re-sorted by weighted score desc.
    Pure — no model, unit-tested directly."""
    weighted = [
        s.model_copy(update={
            "score": s.score * weights.get(s.metadata.get("doc_group", ""), 1.0)
        })
        for s in sources
    ]
    return sorted(weighted, key=lambda s: s.score, reverse=True)
```

In `rerank_sources`, after `out = [node_to_serialized(n) for n in reranked]` (currently L67), and before the return, insert the weighting + cut:

```python
    out = apply_group_weights(out, settings.agent.group_weights)[: params.top_n]
```

Also cut the fail-open branch (reranker unavailable) — it currently returns `pool[: params.top_n]` unweighted; leave as-is (no scores to weight), which the plan's Non-goals already accept.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_workflow/test_rerank_group_weights.py -v`
Expected: PASS (2 tests)

- [ ] **Step 7: Commit**

```bash
git add src/config.py src/workflow/_search_deps.py src/workflow/search/activities/rerank.py tests/test_workflow/test_rerank_group_weights.py
git commit -m "feat(groups): per-group rerank weights (AGENT_GROUP_WEIGHTS)"
```

---

### Task 7: Synthesis group prefix

**Files:**
- Modify: `src/workflow/activities/synthesize_answer.py:18-27`
- Test: `tests/test_workflow/test_synthesize_group_prefix.py`

**Interfaces:**
- Consumes: `SynthesizeParams.accumulated` (each `SerializedNode` carries `metadata["doc_group"]`).
- Produces: `with_group_prefix(sn: SerializedNode) -> SerializedNode` (pure — prepends `"[group] "` to `text` when group non-empty; identity otherwise).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_workflow/test_synthesize_group_prefix.py
from src.workflow.contracts import SerializedNode
from src.workflow.activities.synthesize_answer import with_group_prefix


def test_prefixes_group():
    sn = SerializedNode(chunk_id="c", text="body", metadata={"doc_group": "official"})
    assert with_group_prefix(sn).text == "[official] body"


def test_no_group_is_identity():
    sn = SerializedNode(chunk_id="c", text="body", metadata={})
    assert with_group_prefix(sn).text == "body"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_workflow/test_synthesize_group_prefix.py -v`
Expected: FAIL — `with_group_prefix` not defined.

- [ ] **Step 3: Add the helper + use it**

In `src/workflow/activities/synthesize_answer.py`, add the import and helper:

```python
from src.workflow.contracts import SynthesizeParams, SynthesizeResult, SerializedNode


def with_group_prefix(sn: SerializedNode) -> SerializedNode:
    """Prepend the channel group so the synthesis LLM sees each source's
    type/trust. Identity when the source has no group."""
    g = (sn.metadata or {}).get("doc_group")
    if not g:
        return sn
    return sn.model_copy(update={"text": f"[{g}] {sn.text}"})
```

Change the node reconstruction line (currently L27) from:

```python
    nodes = [serialized_to_node(n) for n in params.accumulated]
```

to:

```python
    nodes = [serialized_to_node(with_group_prefix(n)) for n in params.accumulated]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_workflow/test_synthesize_group_prefix.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/workflow/activities/synthesize_answer.py tests/test_workflow/test_synthesize_group_prefix.py
git commit -m "feat(groups): prefix source text with channel group at synthesis"
```

---

### Task 8: tg_ingest — folder → group mapping

**Files:**
- Modify: `scripts/tg_ingest.py` (`resolve_group_map` new; thread `group_map` through `sync_round`; `post_ingest` gains `group`)
- Test: `tests/test_scripts/test_tg_ingest_groups.py`

**Interfaces:**
- Consumes: `src.retrieval.groups.{GROUP_SET, pick_priority}`.
- Produces: `resolve_group_map(folders, *, peer_id) -> dict[int, str]` (dialog_id → group folder-title; disjoint, `pick_priority` on conflict + WARNING); `post_ingest(..., group: str = "")` sends `data["group"]` when non-empty.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_scripts/test_tg_ingest_groups.py
from types import SimpleNamespace

import scripts.tg_ingest as T


def _folder(title, include):
    return SimpleNamespace(title=title, include_peers=include, pinned_peers=[], exclude_peers=[])


def test_resolve_group_map_maps_channel_to_folder_group():
    folders = [
        _folder("official", [SimpleNamespace(id=1)]),
        _folder("opinion", [SimpleNamespace(id=2)]),
        _folder("Random", [SimpleNamespace(id=3)]),  # not a group folder → ignored
    ]
    gm = T.resolve_group_map(folders, peer_id=lambda p: p.id)
    assert gm == {1: "official", 2: "opinion"}


def test_resolve_group_map_conflict_takes_priority(caplog):
    # channel 9 is in both opinion and official → priority order: opinion wins
    folders = [
        _folder("official", [SimpleNamespace(id=9)]),
        _folder("opinion", [SimpleNamespace(id=9)]),
    ]
    gm = T.resolve_group_map(folders, peer_id=lambda p: p.id)
    assert gm[9] == "opinion"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_scripts/test_tg_ingest_groups.py -v`
Expected: FAIL — `resolve_group_map` not defined.

- [ ] **Step 3: Add `resolve_group_map`**

In `scripts/tg_ingest.py`, add the import near the top:

```python
from src.retrieval.groups import GROUP_SET, pick_priority
```

Add below `resolve_folders`:

```python
def resolve_group_map(folders: list[Any], *, peer_id: Callable[[Any], int]) -> dict[int, str]:
    """Map dialog_id → channel group (folder title ∈ GROUP_SET).

    A group is the lowercase folder title. Disjoint: if a channel appears in
    >1 group-folder keep the higher-priority group (``pick_priority``) and log
    a WARNING. Folders whose title is not a known group are ignored here (they
    may still scope the sync via ``resolve_folders``). Category-flag folders
    («все каналы») don't enumerate peers, so channels included only by a flag
    stay ungrouped (``""``)."""
    out: dict[int, str] = {}
    for f in folders:
        title = _filter_title(f).strip().casefold()
        if title not in GROUP_SET:
            continue
        for p in [*getattr(f, "include_peers", []), *getattr(f, "pinned_peers", [])]:
            pid = peer_id(p)
            prev = out.get(pid)
            if prev and prev != title:
                winner = pick_priority(prev, title)
                logger.warning(
                    "tg_ingest: channel {c} in multiple group-folders "
                    "({a}, {b}) → keeping {w}", c=pid, a=prev, b=title, w=winner,
                )
                out[pid] = winner
            else:
                out[pid] = title
    return out
```

- [ ] **Step 4: Thread `group` through `post_ingest` + `sync_round`**

In `post_ingest` add a `group: str = ""` parameter (after `queue`) and include it in the form:

```python
    data: dict[str, str] = {"document_date": document_date}
    if queue:
        data["queue"] = queue
    if group:
        data["group"] = group
```

In `sync_round`, add a `group_map: dict[int, str]` parameter, and at the point each message is posted resolve the dialog's group and pass it:

```python
        group = group_map.get(dialog.id, "")
        ...
        ok = await post_ingest(http, api_base, api_key, filename, text,
                               document_date, queue, group=group)
```

At the caller that builds `dialogs` + `spec` (the sync driver that already calls `resolve_folders`), also call `resolve_group_map(folders, peer_id=...)` once per round and pass the result into `sync_round(..., group_map=group_map)`. (Same `folders` list; same `peer_id` used for `resolve_folders`.)

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_scripts/test_tg_ingest_groups.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add scripts/tg_ingest.py tests/test_scripts/test_tg_ingest_groups.py
git commit -m "feat(groups): tg_ingest tags each doc with its folder group"
```

---

### Task 9: Config docs + full-suite green + deploy note

**Files:**
- Modify: `.env` example block / `.env.example` if present (document `TG_INGEST_FOLDERS` = the six groups, and `AGENT_GROUP_WEIGHTS`)
- Verify: full test suite

- [ ] **Step 1: Document the env**

In the repo's env template (the committed `.env.example` if one exists; otherwise add a comment block in `docs/` — do NOT edit the gitignored real `.env`), record:

```
# Channel-group folders drive both ingest scope AND per-doc group tagging.
# Name your Telegram folders exactly: news,analytics,digest,opinion,official,data
TG_INGEST_FOLDERS=news,analytics,digest,opinion,official,data
# Optional rerank multipliers per group (JSON); omit to use built-in defaults.
# AGENT_GROUP_WEIGHTS={"official":1.3,"data":1.25,"analytics":1.1,"news":1.0,"digest":0.95,"opinion":0.8}
```

- [ ] **Step 2: Run the full suite**

Run: `uv run --extra dev pytest -q`
Expected: PASS (all pre-existing tests + the new group tests). Investigate and fix any regression before committing.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(groups): document TG_INGEST_FOLDERS groups + AGENT_GROUP_WEIGHTS"
```

- [ ] **Step 4: Deploy note (manual, not a code step)**

Per the deploy model, app services bake code at build time (no bind mounts):
rebuild + recreate the services that run this code — `api` (ingest/search
routes), `worker` (retrieve/rerank/synthesize/parse activities). The
`tg-ingest` service must also be rebuilt (it runs `scripts/tg_ingest.py`).
litellm/Milvus/Nebula need no change. After deploy, rename the Telegram
folders to the six group names so the sync tags new docs.

---

## Notes for the implementer

- **Re-ingest is already live.** The stack is re-populating from the Telegram
  feed post-wipe; once `api`/`worker`/`tg-ingest` are rebuilt with this
  feature, newly ingested chunks carry `doc_group`. No backfill needed.
- **`SearchRequest` app-factory / API-key fixture** in Task 3/4 tests: copy
  the exact import + fixture from an existing `tests/test_api/*.py` — don't
  guess `build_app`/key accessors.
- **`model_copy`** is pydantic v2 (`_Frozen`/`SerializedNode` are pydantic
  models) — used in Tasks 6 & 7 to clone frozen models with an updated field.

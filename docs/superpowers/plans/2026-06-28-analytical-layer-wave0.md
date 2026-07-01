# Analytical Layer — Wave 0 Implementation Plan (v1a + E1 + P1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Wave 0 of the analytical program — an online NL→computation Q&A layer over the knowledge graph (`v1a`), plus first-seen novelty stamping + "what's new" primitives (`E1`), plus knowledge-quality flags (`P1`) — all additive, no new infrastructure, no LLM change to ingest.

**Architecture:** A new `src/analytics/` package (a primitive *catalog* of safe parameterized read-only Cypher builders, a small-LLM *planner* that maps a question to 1–3 catalog calls, and deterministic *provenance*), driven by a new durable `AnalyticalQueryWorkflow` parallel to `SearchOrchestratorWorkflow`. The executor produces the numbers; the LLM only verbalizes them. `E1` adds a post-upsert `created_at` stamp (emulated `ON CREATE`) + a one-time backfill; `P1` and `E1`'s read side are extra catalog primitives.

**Tech Stack:** Python 3.12, FastAPI, Temporal (`temporalio`, pydantic data converter), Neo4j via `Neo4jPropertyGraphStore` (llama_index), LlamaIndex LLM via the project `LLMPool`, pydantic v2 / pydantic-settings, pytest (`asyncio_mode=auto`), ruff.

## Global Constraints

- **Determinism first.** Numbers come from the executor (Cypher rows), never from the LLM. The LLM only verbalizes. This is the layer's core invariant — every synthesis step and its test enforces it.
- **Cypher text-to-Cypher fallback ships OFF.** `ANALYTICS_CYPHER_FALLBACK_ENABLED` default `False`. Wave 0 is catalog-only; the fallback path is **not implemented** here (it is v1c / Wave 3). When the planner cannot map a question, return an honest "cannot compute" answer.
- **Fail-soft everywhere.** Every primitive: `store is None` → empty result; any exception → log at WARN, return empty. Never raise across a Temporal activity boundary. (Exact idiom: `src/graph/analysis.py`.)
- **Read-only.** Wave 0 analytics never writes to the graph. The only write in this plan is E1's `created_at` stamp (in the ingest write-path) and the one-time backfill script.
- **Blocking Neo4j off the event loop.** All store calls run via `await asyncio.to_thread(_run_query, store, cypher, params)`.
- **Canonical date form = epoch-days (int).** Days since 1970-01-01, via `src/retrieval/date_filters.py::iso_to_epoch_days` / `today_epoch_days`. Chunks store `doc_date_epoch` / `inserted_at_epoch`; E1's `created_at` uses the same form. Relationship `valid_from`/`valid_to` are ISO strings (`YYYY-MM-DD…`) or `None`.
- **Entity label is the literal string `"__Entity__"`** (no Python constant in the codebase). Identifier types are the fixed `ID_TYPES` list (Task 1). Relationship `polarity` ∈ `{"affirmed","negated","uncertain"}`, default `"affirmed"`. Entity mention-count property is **`mention_count`** (the *index* is named `entity_mention_count`).
- **Contracts are frozen pydantic** mirroring `src/workflow/contracts.py` (`ConfigDict(frozen=True)`, `Field(default_factory=...)` for collections, `X | None` unions).
- **Quality gates** (run before every commit): `uv run ruff check src/` · `uv run ruff format src/` · `uv run pytest -q`. ruff: line-length 100, target py312, Cyrillic allowed (`RUF001-003` ignored).
- **Test doubles** are hand-rolled dataclasses, not `unittest.mock`: `_FakeStore` (captures `cypher`/`param_map`, returns canned rows) and `_StubLLM` (async `achat` returning a canned `.message.content`). Copy them from `tests/test_retrieval/test_graph_walk_retriever.py` / `tests/test_retrieval/test_query_planner.py`.
- **Git:** commit locally after each task; **never push, never commit to `main`** (work stays on `worktree-anal`). Commit only when the task's tests are green.

---

## Codebase-grounded decisions (deviations from the specs — read first)

These were verified against the live code; they override the design specs where they differ:

1. **E1 `created_at` is NOT set inside the MERGE.** The entity/relationship upsert is `Neo4jPropertyGraphStore.upsert_nodes()/.upsert_relations()` — inside llama_index, not our code (`src/workflow/activities/build_property_graph.py:95-102`). **Emulate `ON CREATE`** with a dedicated post-upsert pass that sets `created_at`/`first_doc_id` **only `WHERE created_at IS NULL`**, scoped to this ingest's entity names + relation triples. A node created this pass has no stamp yet → gets stamped now; a re-mentioned old node already has one → keeps it. A one-time **sentinel backfill** (Task 19) must run before stamping is enabled, so pre-existing elements that get re-mentioned are not mis-stamped as new.
2. **No `EventOrAction` write path** — it's a normal entity *type* in the `EntityType` `Literal` (`src/graph/schema.py:25-52`). E1 needs **no `extract_kg` change**. New `EventOrAction` entities flow through normal extraction, get `created_at` stamped, and surface in `new_events`.
3. **Planner = plain `achat` + tolerant parse + pydantic validation**, not `.as_structured_llm()` (mirrors `src/retrieval/query_planner.py::decompose` and `src/workflow/search/activities/route.py::classify_route`). Robust to small-model output; fail-open.
4. **Chunk dates are epoch-days ints** (`doc_date_epoch`), not ISO. Chunk-based temporal primitives fetch raw epochs and bucket with a **pure helper** `epoch_days_to_period`. Edge-based temporal primitives use `r.valid_from` (ISO) with `substring(...,0,7)` in Cypher.
5. **`AnalyticsSettings` already exists** (`src/config.py:699-714`, env_prefix `ANALYTICS_`, currently ingest-metrics version tags). **Extend it** with the analytical-layer keys (same prefix, exposed as `settings.analytics`). `EventsSettings` / `SignalsSettings` are new.
6. **No Temporal Schedule yet.** Wave 0 has no offline materialization (that is v1b/Wave 1) — every Wave 0 primitive is online. The `/admin/graph/materialize` endpoint and `AnalyticsMaterializeWorkflow` are **out of scope** here.
7. **Indexes via idempotent `ensure_*` helpers** in `src/graph/index.py`, invoked from `build_property_graph` (`:109-111`); there is no migrations dir. E1's `created_at` index follows this pattern; the backfill is a standalone `scripts/` script.

---

## File Structure

**New package `src/analytics/`:**

```
src/analytics/
├── __init__.py
├── ids.py                 # ENTITY_LABEL, ID_TYPES, clamp_top_n, epoch_days_to_period (pure)
├── contracts.py           # frozen wire types: AnalyzeParams, PrimitiveCall, AnalysisPlan,
│                          #   StepResult, Provenance, AnalyticsOutcome
├── store_query.py         # _run_query (sync) + run_rows (async fail-soft, asyncio.to_thread)
├── catalog.py             # Primitive dataclass + CATALOG registry + render_catalog_for_planner()
├── planner.py             # parse_plan (pure) + plan_query (achat) + _SYSTEM prompt
├── provenance.py          # assemble_provenance (deterministic)
├── synthesis.py           # build_synthesis_prompt + extract_numbers + faithfulness_score (pure)
└── primitives/
    ├── __init__.py        # imports each module so registration side-effects run
    ├── aggregations.py    # Family 1
    ├── connections.py     # Family 2
    ├── dynamics.py        # Family 4 (named dynamics.py to avoid clash with other branches)
    ├── communities.py     # Family 3 online subset (communities reads + personalized_pagerank)
    ├── events.py          # E1 read side: new_events, entity_new_connections
    └── quality.py         # P1: contradictions, incomplete_entities, orphans, merge_candidates
```

**New elsewhere:**
```
src/workflow/analytics/
├── __init__.py
├── workflow.py            # AnalyticalQueryWorkflow
└── activities.py          # analytical_plan, execute_step, synthesize_analytical  (+ ANALYTICS_ACTIVITIES, ANALYTICS_LARGE_ACTIVITIES)
src/models/analyze.py      # AnalyzeRequest / AnalyzeResponse (API, ISO dates)
src/api/routes/analyze.py  # POST /api/v1/analyze
scripts/analyze.py         # CLI: run one analytical query
scripts/backfill_first_seen.py   # E1 one-time sentinel backfill
```

**Modified:**
```
src/config.py                         # extend AnalyticsSettings; add EventsSettings, SignalsSettings
src/graph/index.py                    # ensure_first_seen_indexes()
src/workflow/activities/build_property_graph.py   # call stamp_first_seen() + ensure_first_seen_indexes()
src/graph/first_seen.py  (new)        # stamp_first_seen() write helper (E1)
src/workflow/worker.py                # register AnalyticalQueryWorkflow + activities
src/api/main.py                       # include analyze.router
src/mcp/search_server.py              # add kb_analyze MCP-1 tool
```

**Tests** (mirror layout under `tests/test_analytics/`, plus `tests/eval/`):
```
tests/test_analytics/{conftest.py, test_ids.py, test_contracts.py, test_aggregations.py,
  test_connections.py, test_dynamics.py, test_communities.py, test_planner.py,
  test_provenance.py, test_synthesis.py, test_events.py, test_quality.py, test_catalog.py}
tests/test_workflow/test_analytics_workflow.py
tests/test_graph/test_first_seen_stamp.py
tests/eval/test_analytics_faithfulness.py
```

---

## Phase A — Foundations (`v1a`)

### Task 1: Constants & pure helpers (`ids.py`)

**Files:**
- Create: `src/analytics/__init__.py` (empty)
- Create: `src/analytics/ids.py`
- Test: `tests/test_analytics/test_ids.py`
- Create: `tests/test_analytics/__init__.py` (empty), `tests/test_analytics/conftest.py` (shared doubles, Task 5)

**Interfaces:**
- Produces: `ENTITY_LABEL: str = "__Entity__"`; `ID_TYPES: list[str]`; `clamp_top_n(n: int | None, *, default: int = 20, hard_max: int = 200) -> int`; `epoch_days_to_period(epoch: int, granularity: str = "month") -> str`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_ids.py
from src.analytics.ids import ID_TYPES, clamp_top_n, epoch_days_to_period


def test_id_types_are_the_identifier_entity_labels():
    assert "INN" in ID_TYPES and "PhoneNumber" in ID_TYPES
    assert "Person" not in ID_TYPES and "Organization" not in ID_TYPES
    assert len(ID_TYPES) == 12  # the 12 identifier types in schema.py


def test_clamp_top_n():
    assert clamp_top_n(None) == 20
    assert clamp_top_n(5) == 5
    assert clamp_top_n(0) == 20          # non-positive → default
    assert clamp_top_n(99999) == 200     # hard cap


def test_epoch_days_to_period():
    # 2024-03-15 = date(2024,3,15).toordinal() - date(1970,1,1).toordinal() = 19797
    assert epoch_days_to_period(19797, "month") == "2024-03"
    assert epoch_days_to_period(19797, "year") == "2024"
    assert epoch_days_to_period(19797, "quarter") == "2024-Q1"
    assert epoch_days_to_period(0, "month") == "1970-01"  # epoch origin
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analytics/test_ids.py -q`
Expected: FAIL (`ModuleNotFoundError: src.analytics.ids`).

- [ ] **Step 3: Write minimal implementation**

```python
# src/analytics/ids.py
"""Constants + pure helpers for the analytical layer (no I/O, no LLM)."""

from __future__ import annotations

from datetime import date, timedelta

# The entity label is a literal string everywhere in the graph (no constant
# exists in src/graph/schema.py — it is established by the extractor/store).
ENTITY_LABEL = "__Entity__"

# The 12 identifier entity types (the identifier block of EntityType in
# src/graph/schema.py:25-52). Many aggregates exclude these by default.
ID_TYPES: list[str] = [
    "Email",
    "PhoneNumber",
    "PostalAddress",
    "DocumentDate",
    "Amount",
    "ContractNumber",
    "OrderNumber",
    "InvoiceNumber",
    "INN",
    "OGRN",
    "BIC",
    "BankAccount",
]

_EPOCH = date(1970, 1, 1)


def clamp_top_n(n: int | None, *, default: int = 20, hard_max: int = 200) -> int:
    """Clamp a requested row cap into ``[1, hard_max]``; ``None``/<=0 → default."""
    if not n or n <= 0:
        return default
    return min(int(n), hard_max)


def epoch_days_to_period(epoch: int, granularity: str = "month") -> str:
    """Bucket an epoch-day integer into a period label.

    granularity: ``year`` → ``"2024"`` · ``quarter`` → ``"2024-Q1"`` ·
    ``month`` (default) → ``"2024-03"``.
    """
    d = _EPOCH + timedelta(days=int(epoch))
    if granularity == "year":
        return f"{d.year:04d}"
    if granularity == "quarter":
        return f"{d.year:04d}-Q{(d.month - 1) // 3 + 1}"
    return f"{d.year:04d}-{d.month:02d}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_analytics/test_ids.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/ids.py && uv run ruff format src/analytics/ids.py tests/test_analytics/test_ids.py
git add src/analytics/__init__.py src/analytics/ids.py tests/test_analytics/__init__.py tests/test_analytics/test_ids.py
git commit -m "feat(analytics): ids + pure helpers (ID_TYPES, clamp_top_n, epoch_days_to_period)"
```

---

### Task 2: Wire types / contracts (`contracts.py`)

**Files:**
- Create: `src/analytics/contracts.py`
- Test: `tests/test_analytics/test_contracts.py`

**Interfaces:**
- Produces (all `frozen=True` pydantic):
  - `PrimitiveCall(primitive: str, params: dict[str, Any] = {})`
  - `AnalysisPlan(route: Literal["catalog","cypher"]="catalog", steps: list[PrimitiveCall]=[], reason: str="")`
  - `StepResult(primitive: str, params: dict, cypher: str, rows: list[dict], row_count: int, source_chunks: list[str]=[], truncated: bool=False)`
  - `Provenance(route: str, plan_reason: str, steps: list[StepResult], elapsed_ms: int=0)`
  - `AnalyzeParams(query: str, top_n: int=20, date_from_epoch: int|None=None, date_to_epoch: int|None=None)` — Temporal workflow input
  - `AnalyticsOutcome(query: str, answer: str, provenance: Provenance, latency_ms: int=0)` — Temporal workflow output

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_contracts.py
import pytest
from pydantic import ValidationError

from src.analytics.contracts import (
    AnalysisPlan,
    AnalyticsOutcome,
    AnalyzeParams,
    PrimitiveCall,
    Provenance,
    StepResult,
)


def test_plan_defaults_and_frozen():
    plan = AnalysisPlan(steps=[PrimitiveCall(primitive="count_entities", params={"type": "Organization"})])
    assert plan.route == "catalog"
    assert plan.steps[0].primitive == "count_entities"
    with pytest.raises(ValidationError):
        plan.route = "cypher"  # frozen


def test_outcome_roundtrips_provenance():
    sr = StepResult(
        primitive="count_entities", params={}, cypher="MATCH ...",
        rows=[{"n": 3}], row_count=1,
    )
    prov = Provenance(route="catalog", plan_reason="r", steps=[sr], elapsed_ms=12)
    out = AnalyticsOutcome(query="q", answer="a", provenance=prov, latency_ms=20)
    assert out.provenance.steps[0].rows == [{"n": 3}]


def test_analyze_params_defaults():
    p = AnalyzeParams(query="q")
    assert p.top_n == 20 and p.date_from_epoch is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analytics/test_contracts.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write minimal implementation**

```python
# src/analytics/contracts.py
"""Frozen wire types for the analytical layer.

Mirrors the style of src/workflow/contracts.py (frozen BaseModel,
Field(default_factory=...) for collections). Serialized by Temporal's
pydantic data converter.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class PrimitiveCall(_Frozen):
    primitive: str
    params: dict[str, Any] = Field(default_factory=dict)


class AnalysisPlan(_Frozen):
    route: Literal["catalog", "cypher"] = "catalog"
    steps: list[PrimitiveCall] = Field(default_factory=list)
    reason: str = ""


class StepResult(_Frozen):
    primitive: str
    params: dict[str, Any] = Field(default_factory=dict)
    cypher: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    source_chunks: list[str] = Field(default_factory=list)
    truncated: bool = False


class Provenance(_Frozen):
    route: str = "catalog"
    plan_reason: str = ""
    steps: list[StepResult] = Field(default_factory=list)
    elapsed_ms: int = 0


class AnalyzeParams(_Frozen):
    """AnalyticalQueryWorkflow input (epoch-day bounds, like OrchestratorParams)."""

    query: str
    top_n: int = 20
    date_from_epoch: int | None = None
    date_to_epoch: int | None = None


class AnalyticsOutcome(_Frozen):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int = 0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_analytics/test_contracts.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/contracts.py && uv run ruff format src/analytics/contracts.py tests/test_analytics/test_contracts.py
git add src/analytics/contracts.py tests/test_analytics/test_contracts.py
git commit -m "feat(analytics): frozen wire contracts (plan/step/provenance/params/outcome)"
```

---

### Task 3: Config — extend `AnalyticsSettings`, add `EventsSettings`/`SignalsSettings`

**Files:**
- Modify: `src/config.py` (extend existing `AnalyticsSettings` ~`:699-714`; add two classes; add two `@cached_property` on `Settings` ~`:833-914`)
- Test: `tests/test_analytics/test_config.py`

**Interfaces:**
- Produces: `settings.analytics.default_top_n: int`, `.max_steps: int`, `.cypher_fallback_enabled: bool`; `settings.events.first_seen_enabled: bool`, `.new_window_days: int`, `.backfill_sentinel: int`; `settings.signals.orphan_min_degree: int`, `.expected_attrs: dict[str, list[str]]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_config.py
from src.config import settings


def test_analytics_layer_settings_defaults():
    assert settings.analytics.default_top_n == 20
    assert settings.analytics.max_steps == 3
    assert settings.analytics.cypher_fallback_enabled is False  # ships OFF


def test_events_settings_defaults():
    assert settings.events.first_seen_enabled is False  # OFF until backfill run
    assert settings.events.new_window_days == 14
    assert settings.events.backfill_sentinel == 0


def test_signals_settings_defaults():
    assert settings.signals.orphan_min_degree == 1
    assert "Organization" in settings.signals.expected_attrs
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_analytics/test_config.py -q`
Expected: FAIL (`AttributeError: ... 'default_top_n'` / no `events`).

- [ ] **Step 3: Implement — extend `AnalyticsSettings`, add classes**

In `src/config.py`, add the new fields to the **existing** `AnalyticsSettings` class (keep its current `default_version_tag`/`env_name`; same `env_prefix="ANALYTICS_"`):

```python
class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ANALYTICS_", env_file=".env", extra="ignore",
    )
    # --- existing ingest-metrics fields: keep as-is ---
    default_version_tag: str = "unspecified"
    env_name: str = "dev-local"
    # --- analytical-query layer (Wave 0 v1a) ---
    default_top_n: int = 20
    max_steps: int = 3                       # max primitive calls per plan
    cypher_fallback_enabled: bool = False    # text-to-Cypher fallback (v1c; ships OFF)
```

Add two new classes near the other sub-settings:

```python
class EventsSettings(BaseSettings):
    """first_seen / event-detection config (Wave 0 E1)."""

    model_config = SettingsConfigDict(
        env_prefix="EVENTS_", env_file=".env", extra="ignore",
    )
    first_seen_enabled: bool = False    # enable ON-CREATE stamping (flip AFTER backfill)
    new_window_days: int = 14           # default window for new_events
    backfill_sentinel: int = 0          # epoch-day stamp for pre-existing elements


class SignalsSettings(BaseSettings):
    """Knowledge-quality / actionable-signal config (Wave 0 P1)."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALS_", env_file=".env", extra="ignore",
    )
    orphan_min_degree: int = 1
    # per-type expected identifier attributes for completeness scoring
    expected_attrs: dict[str, list[str]] = {
        "Organization": ["INN", "OGRN", "PostalAddress", "PhoneNumber"],
        "Person": ["PhoneNumber", "Email"],
    }
```

Add the `@cached_property` accessors on `Settings` (next to `def analytics`):

```python
    @cached_property
    def events(self) -> EventsSettings:
        return EventsSettings()

    @cached_property
    def signals(self) -> SignalsSettings:
        return SignalsSettings()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_analytics/test_config.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/config.py && uv run ruff format src/config.py tests/test_analytics/test_config.py
git add src/config.py tests/test_analytics/test_config.py
git commit -m "feat(analytics): config — analytics-layer/events/signals settings"
```

---

## Phase B — Primitives (`v1a` Families 1/2/4 + communities)

### Task 4: Store query helper + Primitive/catalog skeleton + shared test doubles

**Files:**
- Create: `src/analytics/store_query.py`, `src/analytics/catalog.py`, `src/analytics/primitives/__init__.py`
- Create: `tests/test_analytics/conftest.py` (shared `_FakeStore`)
- Test: `tests/test_analytics/test_catalog.py`

**Interfaces:**
- Produces:
  - `store_query._run_query(store, cypher: str, params: dict | None) -> list[dict]` (sync)
  - `store_query.run_rows(store, cypher: str, params: dict | None) -> list[dict]` (async, fail-soft)
  - `catalog.PrimitiveResult(cypher: str, params: dict, rows: list[dict], source_chunks: list[str], truncated: bool)` (dataclass)
  - `catalog.Primitive(name, fn, param_model, description, tier)` (dataclass) — `fn: Callable[..., Awaitable[PrimitiveResult]]`
  - `catalog.register(primitive)` and `catalog.CATALOG: dict[str, Primitive]`
  - `catalog.render_catalog_for_planner() -> str`
- Consumes: nothing (this is the registry every primitive task plugs into via `register(...)`).

- [ ] **Step 1: Write `conftest.py` shared `_FakeStore`** (copied from `tests/test_retrieval/test_graph_walk_retriever.py:21-32`)

```python
# tests/test_analytics/conftest.py
from __future__ import annotations

from dataclasses import dataclass, field


class _FakeStore:
    """Captures the last Cypher + params and returns canned rows."""

    def __init__(self, rows=None, *, by_call=None):
        self._rows = rows or []
        self._by_call = list(by_call) if by_call else None  # rows per sequential call
        self.calls: list[tuple[str, dict]] = []
        self.last_cypher = None
        self.last_params = None

    def structured_query(self, cypher, param_map=None):
        self.last_cypher = cypher
        self.last_params = param_map or {}
        self.calls.append((cypher, self.last_params))
        if self._by_call is not None:
            i = len(self.calls) - 1
            return self._by_call[i] if i < len(self._by_call) else []
        return self._rows


@dataclass
class _StubLLM:
    """Minimal async chat LLM returning a canned reply (or raising)."""

    reply: str = ""
    raises: bool = False
    calls: list = field(default_factory=list)

    async def achat(self, messages):
        self.calls.append(messages)
        if self.raises:
            raise RuntimeError("llm down")

        class _Msg:
            content = self.reply

        class _Resp:
            message = _Msg()

        return _Resp()
```

- [ ] **Step 2: Write the failing test**

```python
# tests/test_analytics/test_catalog.py
import pytest

from src.analytics import store_query
from src.analytics.catalog import CATALOG, Primitive, PrimitiveResult, register, render_catalog_for_planner
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_run_rows_failsoft_on_none_store():
    assert await store_query.run_rows(None, "MATCH (n) RETURN n", {}) == []


@pytest.mark.asyncio
async def test_run_rows_passes_cypher_and_params():
    store = _FakeStore(rows=[{"n": 1}])
    rows = await store_query.run_rows(store, "MATCH (n) RETURN n", {"x": 1})
    assert rows == [{"n": 1}]
    assert store.last_params == {"x": 1}


@pytest.mark.asyncio
async def test_run_rows_failsoft_on_error():
    class _Boom:
        def structured_query(self, *a, **k):
            raise RuntimeError("db down")

    assert await store_query.run_rows(_Boom(), "X", {}) == []


def test_register_and_render():
    from pydantic import BaseModel

    class _P(BaseModel):
        pass

    async def _fn(store, **kw):
        return PrimitiveResult(cypher="C", params={}, rows=[], source_chunks=[], truncated=False)

    register(Primitive(name="_demo", fn=_fn, param_model=_P, description="demo desc", tier="online"))
    assert "_demo" in CATALOG
    assert "demo desc" in render_catalog_for_planner()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_analytics/test_catalog.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 4: Implement `store_query.py`**

```python
# src/analytics/store_query.py
"""Read-only Neo4j query execution for analytics primitives (fail-soft)."""

from __future__ import annotations

import asyncio
from typing import Any

from src.logging import get_logger  # project logger; match src/graph/analysis.py import

logger = get_logger(__name__)


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    """Sync execution; returns raw rows. Mirrors src/graph/analysis.py::_run_query."""
    return list(store.structured_query(cypher, param_map=params or {}))


async def run_rows(store: Any | None, cypher: str, params: dict | None = None) -> list[dict]:
    """Run ``cypher`` off the event loop; ``[]`` on no-store or any error."""
    if store is None:
        return []
    try:
        return await asyncio.to_thread(_run_query, store, cypher, params)
    except Exception as exc:  # noqa: BLE001 — fail-soft like analysis.py
        logger.warning("analytics query failed: {e}", e=exc)
        return []
```

> Implementer note: confirm the logger import used by `src/graph/analysis.py` and copy it verbatim (the project uses a `loguru`-style `logger.warning("... {e}", e=exc)`). If it imports `from loguru import logger`, use that instead of `get_logger`.

- [ ] **Step 5: Implement `catalog.py`**

```python
# src/analytics/catalog.py
"""Primitive registry: the single source of truth the planner sees."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel


@dataclass
class PrimitiveResult:
    cypher: str
    params: dict[str, Any]
    rows: list[dict[str, Any]]
    source_chunks: list[str] = field(default_factory=list)
    truncated: bool = False


@dataclass(frozen=True)
class Primitive:
    name: str
    fn: Callable[..., Awaitable[PrimitiveResult]]
    param_model: type[BaseModel]
    description: str
    tier: str = "online"  # "online" | "offline-mat"


CATALOG: dict[str, Primitive] = {}


def register(primitive: Primitive) -> Primitive:
    CATALOG[primitive.name] = primitive
    return primitive


def render_catalog_for_planner() -> str:
    """Human-readable catalog (name + description + params) for the planner prompt."""
    lines: list[str] = []
    for name in sorted(CATALOG):
        p = CATALOG[name]
        fields = ", ".join(p.param_model.model_fields) or "(none)"
        lines.append(f"- {name}({fields}): {p.description}")
    return "\n".join(lines)
```

- [ ] **Step 6: Implement `primitives/__init__.py`** (imports run registration side-effects; fill in as primitive modules land)

```python
# src/analytics/primitives/__init__.py
"""Importing this package registers every primitive into catalog.CATALOG."""

from src.analytics.primitives import aggregations  # noqa: F401
from src.analytics.primitives import connections  # noqa: F401
from src.analytics.primitives import dynamics  # noqa: F401
from src.analytics.primitives import communities  # noqa: F401
from src.analytics.primitives import events  # noqa: F401
from src.analytics.primitives import quality  # noqa: F401
```

> Implementer note: add each import line only as that module is created (Tasks 5–9, 21, 22–23), so the package imports cleanly between tasks. The final state imports all six.

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_analytics/test_catalog.py -q`
Expected: PASS (4 tests). (Comment out the `primitives/__init__.py` import-all until modules exist, or create empty stubs.)

- [ ] **Step 8: Lint + commit**

```bash
uv run ruff check src/analytics/ && uv run ruff format src/analytics/ tests/test_analytics/
git add src/analytics/store_query.py src/analytics/catalog.py src/analytics/primitives/__init__.py tests/test_analytics/conftest.py tests/test_analytics/test_catalog.py
git commit -m "feat(analytics): store_query (fail-soft) + Primitive/CATALOG registry"
```

---

### Task 5: Family 1 — Aggregations & rankings (`aggregations.py`)

**Files:**
- Create: `src/analytics/primitives/aggregations.py`
- Test: `tests/test_analytics/test_aggregations.py`

**Interfaces:**
- Consumes: `store_query.run_rows`, `catalog.{Primitive,PrimitiveResult,register}`, `ids.{ID_TYPES,ENTITY_LABEL,clamp_top_n}`.
- Produces (registered primitives, each `async def fn(store, **params) -> PrimitiveResult`):
  `count_entities`, `count_relationships`, `distribution_by_type`, `distribution_by_relation_type`, `distribution_by_polarity`, `top_entities_by_mentions`, `top_entities_by_degree`.

- [ ] **Step 1: Write the failing tests** (one per primitive — assert generated Cypher shape + params + fail-soft)

```python
# tests/test_analytics/test_aggregations.py
import pytest

from src.analytics.primitives import aggregations as agg
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_count_entities_excludes_identifiers_by_default():
    store = _FakeStore(rows=[{"n": 7}])
    res = await agg.count_entities(store, type="Organization")
    assert res.rows == [{"n": 7}]
    assert "count(e)" in res.cypher
    assert res.params["type"] == "Organization"
    # identifier exclusion present
    assert "ID_TYPES" in res.cypher or "$id_types" in res.cypher
    assert res.params["id_types"]  # passed in


@pytest.mark.asyncio
async def test_count_entities_failsoft():
    res = await agg.count_entities(None)
    assert res.rows == []


@pytest.mark.asyncio
async def test_count_relationships_filters_rel_type_and_polarity():
    store = _FakeStore(rows=[{"n": 3}])
    res = await agg.count_relationships(store, rel_type="OWNS", polarity="negated")
    assert res.params["rel_type"] == "OWNS" and res.params["polarity"] == "negated"


@pytest.mark.asyncio
async def test_top_entities_by_mentions_clamps_and_orders():
    store = _FakeStore(rows=[{"name": "X", "mentions": 9}])
    res = await agg.top_entities_by_mentions(store, top_n=99999)
    assert res.params["top_n"] == 200  # clamp
    assert "mention_count" in res.cypher and "ORDER BY" in res.cypher


@pytest.mark.asyncio
async def test_distribution_by_type_shape():
    store = _FakeStore(rows=[{"type": "Person", "n": 5}])
    res = await agg.distribution_by_type(store)
    assert res.rows[0]["type"] == "Person"


@pytest.mark.asyncio
async def test_top_entities_by_degree_excludes_negated():
    store = _FakeStore(rows=[{"name": "X", "degree": 4}])
    res = await agg.top_entities_by_degree(store)
    assert "negated" in res.cypher
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_analytics/test_aggregations.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement** (full module)

```python
# src/analytics/primitives/aggregations.py
"""Family 1 — aggregations & rankings (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CountEntitiesParams(_Params):
    type: str | None = None
    exclude_identifiers: bool = True


async def count_entities(store: Any | None, *, type: str | None = None, exclude_identifiers: bool = True) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($type IS NULL OR $type IN labels(e)) "
        "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "RETURN count(e) AS n"
    )
    params = {"type": type, "exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class CountRelationshipsParams(_Params):
    rel_type: str | None = None
    polarity: str | None = None


async def count_relationships(store: Any | None, *, rel_type: str | None = None, polarity: str | None = None) -> PrimitiveResult:
    cypher = (
        "MATCH (:__Entity__)-[r]->(:__Entity__) "
        "WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
        "AND ($polarity IS NULL OR r.polarity = $polarity) "
        "RETURN count(r) AS n"
    )
    params = {"rel_type": rel_type, "polarity": polarity}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class DistributionByTypeParams(_Params):
    exclude_identifiers: bool = False


async def distribution_by_type(store: Any | None, *, exclude_identifiers: bool = False) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "WITH [l IN labels(e) WHERE l <> '__Entity__'][0] AS type "
        "RETURN type, count(*) AS n ORDER BY n DESC"
    )
    params = {"exclude_ids": exclude_identifiers, "id_types": ID_TYPES}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class _NoParams(_Params):
    pass


async def distribution_by_relation_type(store: Any | None) -> PrimitiveResult:
    cypher = "MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC"
    rows = await run_rows(store, cypher, {})
    return PrimitiveResult(cypher=cypher, params={}, rows=rows)


class DistributionByPolarityParams(_Params):
    rel_type: str | None = None


async def distribution_by_polarity(store: Any | None, *, rel_type: str | None = None) -> PrimitiveResult:
    cypher = (
        "MATCH ()-[r]->() WHERE ($rel_type IS NULL OR type(r) = $rel_type) "
        "RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC"
    )
    params = {"rel_type": rel_type}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class TopByMentionsParams(_Params):
    type: str | None = None
    top_n: int = 20
    exclude_identifiers: bool = True


async def top_entities_by_mentions(store: Any | None, *, type: str | None = None, top_n: int = 20, exclude_identifiers: bool = True) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) "
        "WHERE ($type IS NULL OR $type IN labels(e)) "
        "AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $id_types)) "
        "AND e.mention_count IS NOT NULL "
        "RETURN e.name AS name, e.mention_count AS mentions "
        "ORDER BY e.mention_count DESC LIMIT $top_n"
    )
    params = {"type": type, "exclude_ids": exclude_identifiers, "id_types": ID_TYPES, "top_n": top_n}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=len(rows) >= top_n)


class TopByDegreeParams(_Params):
    type: str | None = None
    top_n: int = 20


async def top_entities_by_degree(store: Any | None, *, type: str | None = None, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) WHERE ($type IS NULL OR $type IN labels(e)) "
        "OPTIONAL MATCH (e)-[r]-(:__Entity__) WHERE r.polarity <> 'negated' "
        "WITH e, count(r) AS degree "
        "RETURN e.name AS name, degree ORDER BY degree DESC LIMIT $top_n"
    )
    params = {"type": type, "top_n": top_n}
    rows = await run_rows(store, cypher, params)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows, truncated=len(rows) >= top_n)


register(Primitive("count_entities", count_entities, CountEntitiesParams,
                   "Count entities, optionally by type; excludes identifier nodes by default."))
register(Primitive("count_relationships", count_relationships, CountRelationshipsParams,
                   "Count relationships, optionally by relation type and/or polarity."))
register(Primitive("distribution_by_type", distribution_by_type, DistributionByTypeParams,
                   "Histogram of entities by type."))
register(Primitive("distribution_by_relation_type", distribution_by_relation_type, _NoParams,
                   "Histogram of relationships by relation type."))
register(Primitive("distribution_by_polarity", distribution_by_polarity, DistributionByPolarityParams,
                   "Share of affirmed/negated/uncertain relationships (contentiousness)."))
register(Primitive("top_entities_by_mentions", top_entities_by_mentions, TopByMentionsParams,
                   "Top entities by mention frequency (importance by how often discussed)."))
register(Primitive("top_entities_by_degree", top_entities_by_degree, TopByDegreeParams,
                   "Top entities by connection count (degree), ignoring negated edges."))
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_analytics/test_aggregations.py -q`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/aggregations.py && uv run ruff format src/analytics/primitives/aggregations.py tests/test_analytics/test_aggregations.py
git add src/analytics/primitives/aggregations.py tests/test_analytics/test_aggregations.py
git commit -m "feat(analytics): Family 1 aggregation/ranking primitives"
```

---

### Task 6: Family 2 — Connections & co-occurrence (`connections.py`)

**Files:**
- Create: `src/analytics/primitives/connections.py`
- Test: `tests/test_analytics/test_connections.py`

**Interfaces:**
- Produces registered primitives: `entity_dossier` (flagship, multi-query), `neighbors_by_relation`, `cooccurrence`, `common_connections`, `connection_path`, `shared_identifier_entities`, `identifier_lookup`. Each returns `PrimitiveResult`; `entity_dossier` runs several sub-queries and returns a single `rows=[{core, connections, identifiers, communities}]`, with `source_chunks` aggregated.

- [ ] **Step 1: Write failing tests** (assert sub-query shapes, name param, identifier separation, fail-soft)

```python
# tests/test_analytics/test_connections.py
import pytest

from src.analytics.primitives import connections as conn
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_entity_dossier_assembles_sections():
    store = _FakeStore(by_call=[
        [{"name": "Ромашка", "description": "d", "labels": ["__Entity__", "Organization"], "mention_count": 4}],  # core
        [{"rel": "OWNS", "name": "ООО Лютик", "ntype": "Organization", "w": 2.0}],                                 # neighbors
        [{"id_type": "INN", "value": "7701234567"}],                                                               # identifiers
        [{"level": 0, "title": "Поставки"}],                                                                       # communities
    ])
    res = await conn.entity_dossier(store, name="Ромашка")
    row = res.rows[0]
    assert row["core"]["name"] == "Ромашка"
    assert row["connections"][0]["rel"] == "OWNS"
    assert row["identifiers"][0]["id_type"] == "INN"
    assert row["communities"][0]["title"] == "Поставки"
    assert res.params["name"] == "Ромашка"


@pytest.mark.asyncio
async def test_shared_identifier_entities_min_owners():
    store = _FakeStore(rows=[{"value": "7701234567", "id_type": "INN", "owners": ["A", "B"]}])
    res = await conn.shared_identifier_entities(store, min_owners=2)
    assert res.params["min_owners"] == 2
    assert "size(owners) >= $min_owners" in res.cypher


@pytest.mark.asyncio
async def test_connection_path_clamps_hops_inline():
    store = _FakeStore(rows=[{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}])
    res = await conn.connection_path(store, source="A", target="B", max_hops=99)
    # hops clamped into the Cypher literal (shortestPath bound), max 12
    assert "*..12" in res.cypher or res.params.get("max_hops") == 12


@pytest.mark.asyncio
async def test_cooccurrence_via_shared_chunks():
    store = _FakeStore(rows=[{"name": "B", "shared": 3}])
    res = await conn.cooccurrence(store, name="A")
    assert ":MENTIONS" in res.cypher


@pytest.mark.asyncio
async def test_failsoft():
    assert (await conn.entity_dossier(None, name="X")).rows == []
    assert (await conn.identifier_lookup(None, value="x")).rows == []
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/test_analytics/test_connections.py -q` → FAIL.

- [ ] **Step 3: Implement** (full module)

```python
# src/analytics/primitives/connections.py
"""Family 2 — connections & co-occurrence (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_CORE = (
    "MATCH (e:__Entity__ {name:$name}) "
    "RETURN e.name AS name, e.description AS description, labels(e) AS labels, "
    "e.mention_count AS mention_count"
)
_NEIGHBORS = (
    "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
    "WHERE r.polarity <> 'negated' AND NONE(l IN labels(n) WHERE l IN $id_types) "
    "RETURN type(r) AS rel, n.name AS name, "
    "[l IN labels(n) WHERE l <> '__Entity__'][0] AS ntype, r.weight AS w "
    "ORDER BY r.weight DESC LIMIT $top_n"
)
_IDENTIFIERS = (
    "MATCH (e:__Entity__ {name:$name})-[]-(id:__Entity__) "
    "WHERE any(l IN labels(id) WHERE l IN $id_types) "
    "RETURN [l IN labels(id) WHERE l IN $id_types][0] AS id_type, id.name AS value"
)
_COMMUNITIES = (
    "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
    "RETURN c.level AS level, c.title AS title"
)


class EntityDossierParams(_Params):
    name: str
    top_n: int = 25


async def entity_dossier(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    params = {"name": name, "top_n": top_n, "id_types": ID_TYPES}
    if store is None:
        return PrimitiveResult(cypher=_CORE, params=params, rows=[])
    core = await run_rows(store, _CORE, params)
    if not core:
        return PrimitiveResult(cypher=_CORE, params=params, rows=[])
    neighbors = await run_rows(store, _NEIGHBORS, params)
    identifiers = await run_rows(store, _IDENTIFIERS, params)
    communities = await run_rows(store, _COMMUNITIES, params)
    row = {
        "core": core[0],
        "connections": neighbors,
        "identifiers": identifiers,
        "communities": communities,
    }
    cypher = " ;; ".join([_CORE, _NEIGHBORS, _IDENTIFIERS, _COMMUNITIES])
    return PrimitiveResult(cypher=cypher, params=params, rows=[row])


class NeighborsByRelationParams(_Params):
    name: str
    rel_type: str
    polarity: str | None = None
    top_n: int = 25


async def neighbors_by_relation(store: Any | None, *, name: str, rel_type: str, polarity: str | None = None, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
        "WHERE type(r)=$rel_type AND ($polarity IS NULL OR r.polarity=$polarity) "
        "RETURN n.name AS name, r.weight AS w, r.valid_from AS valid_from, r.valid_to AS valid_to "
        "ORDER BY r.weight DESC LIMIT $top_n"
    )
    params = {"name": name, "rel_type": rel_type, "polarity": polarity, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class CooccurrenceParams(_Params):
    name: str
    top_n: int = 25


async def cooccurrence(store: Any | None, *, name: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->(other:__Entity__) "
        "WHERE other <> e "
        "RETURN other.name AS name, count(DISTINCT c) AS shared ORDER BY shared DESC LIMIT $top_n"
    )
    params = {"name": name, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class CommonConnectionsParams(_Params):
    a: str
    b: str
    top_n: int = 25


async def common_connections(store: Any | None, *, a: str, b: str, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (x:__Entity__ {name:$a})-[r1]-(m:__Entity__)-[r2]-(y:__Entity__ {name:$b}) "
        "WHERE r1.polarity<>'negated' AND r2.polarity<>'negated' "
        "RETURN m.name AS name, [l IN labels(m) WHERE l<>'__Entity__'][0] AS type, "
        "collect(DISTINCT type(r1))+collect(DISTINCT type(r2)) AS via LIMIT $top_n"
    )
    params = {"a": a, "b": b, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class ConnectionPathParams(_Params):
    source: str
    target: str
    max_hops: int = 6


async def connection_path(store: Any | None, *, source: str, target: str, max_hops: int = 6) -> PrimitiveResult:
    hops = max(1, min(int(max_hops or 6), 12))  # clamp; inlined into pattern (cannot parameterize var-length)
    cypher = (
        "MATCH (a:__Entity__ {name:$source}),(b:__Entity__ {name:$target}) "
        f"MATCH p = shortestPath((a)-[*..{hops}]-(b)) "
        "RETURN [n IN nodes(p)|n.name] AS path, [r IN relationships(p)|type(r)] AS rels, length(p) AS hops"
    )
    params = {"source": source, "target": target, "max_hops": hops}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class SharedIdentifierParams(_Params):
    id_type: str | None = None
    min_owners: int = 2
    top_n: int = 25


async def shared_identifier_entities(store: Any | None, *, id_type: str | None = None, min_owners: int = 2, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    cypher = (
        "MATCH (id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $id_types) "
        "AND ($id_type IS NULL OR $id_type IN labels(id)) "
        "MATCH (id)-[]-(owner:__Entity__) WHERE NONE(l IN labels(owner) WHERE l IN $id_types) "
        "WITH id, [l IN labels(id) WHERE l IN $id_types][0] AS id_type, collect(DISTINCT owner.name) AS owners "
        "WHERE size(owners) >= $min_owners "
        "RETURN id.name AS value, id_type, owners ORDER BY size(owners) DESC LIMIT $top_n"
    )
    params = {"id_type": id_type, "min_owners": int(min_owners), "top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class IdentifierLookupParams(_Params):
    value: str


async def identifier_lookup(store: Any | None, *, value: str) -> PrimitiveResult:
    cypher = (
        "MATCH (id:__Entity__ {name:$value})-[r]-(e:__Entity__) "
        "WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "RETURN e.name AS name, labels(e) AS labels, type(r) AS rel"
    )
    params = {"value": value, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("entity_dossier", entity_dossier, EntityDossierParams,
                   "Full portrait of one entity: core, top neighbors, attached identifiers, communities."))
register(Primitive("neighbors_by_relation", neighbors_by_relation, NeighborsByRelationParams,
                   "Entities linked to a named entity by a specific relation type."))
register(Primitive("cooccurrence", cooccurrence, CooccurrenceParams,
                   "Entities most often mentioned together with a named entity (shared chunks)."))
register(Primitive("common_connections", common_connections, CommonConnectionsParams,
                   "What/who two named entities share (common neighbors)."))
register(Primitive("connection_path", connection_path, ConnectionPathParams,
                   "Shortest path (chain of relations) between two named entities."))
register(Primitive("shared_identifier_entities", shared_identifier_entities, SharedIdentifierParams,
                   "Distinct entities sharing one identifier (phone/INN/account) — affiliation/dedup/risk."))
register(Primitive("identifier_lookup", identifier_lookup, IdentifierLookupParams,
                   "Who owns this identifier value (phone/INN/email)."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/connections.py && uv run ruff format src/analytics/primitives/connections.py tests/test_analytics/test_connections.py
git add src/analytics/primitives/connections.py tests/test_analytics/test_connections.py
git commit -m "feat(analytics): Family 2 connection/co-occurrence primitives (incl. entity_dossier)"
```

---

### Task 7: Family 4 — Temporal dynamics (`dynamics.py`)

**Files:**
- Create: `src/analytics/primitives/dynamics.py`
- Test: `tests/test_analytics/test_dynamics.py`

**Interfaces:**
- Produces: `relationship_timeline`, `whats_changed`, `topic_trend`, `polarity_evolution`, `entity_activity`. Edge-based ones use ISO `valid_from`/`valid_to` (`substring`); chunk-based ones (`topic_trend`, `entity_activity`) fetch `doc_date_epoch` rows and bucket via `ids.epoch_days_to_period`.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_dynamics.py
import pytest

from src.analytics.primitives import dynamics as dyn
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_topic_trend_buckets_epoch_days_in_python():
    # two chunks: 2024-03-15 (19797) and 2024-03-20 (19802) → same month bucket
    store = _FakeStore(rows=[{"epoch": 19797, "n": 2}, {"epoch": 19802, "n": 1}])
    res = await dyn.topic_trend(store, topic="Поставки", granularity="month")
    periods = {r["period"]: r["mentions"] for r in res.rows}
    assert periods == {"2024-03": 3}
    assert "doc_date_epoch" in res.cypher


@pytest.mark.asyncio
async def test_relationship_timeline_uses_iso_substring():
    store = _FakeStore(rows=[{"period": "2024-03", "rel": "OWNS", "name": "X", "polarity": "affirmed"}])
    res = await dyn.relationship_timeline(store, name="Ромашка")
    assert "substring(r.valid_from" in res.cypher


@pytest.mark.asyncio
async def test_whats_changed_marks_appeared_vs_ended():
    store = _FakeStore(rows=[{"name": "A", "rel": "OWNS", "other": "B", "change": "appeared"}])
    res = await dyn.whats_changed(store, date_from="2024-01-01", date_to="2024-12-31")
    assert res.params["from"] == "2024-01-01"


@pytest.mark.asyncio
async def test_failsoft():
    assert (await dyn.topic_trend(None, topic="x")).rows == []
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/primitives/dynamics.py
"""Family 4 — temporal dynamics (online, read-only)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n, epoch_days_to_period
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RelationshipTimelineParams(_Params):
    name: str
    rel_type: str | None = None


async def relationship_timeline(store: Any | None, *, name: str, rel_type: str | None = None) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) "
        "WHERE r.valid_from IS NOT NULL AND ($rel_type IS NULL OR type(r)=$rel_type) "
        "RETURN substring(r.valid_from,0,7) AS period, type(r) AS rel, n.name AS name, r.polarity AS polarity "
        "ORDER BY period"
    )
    params = {"name": name, "rel_type": rel_type}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class WhatsChangedParams(_Params):
    date_from: str
    date_to: str
    entity: str | None = None
    top_n: int = 50


async def whats_changed(store: Any | None, *, date_from: str, date_to: str, entity: str | None = None, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    cypher = (
        "MATCH (e:__Entity__)-[r]-(n:__Entity__) "
        "WHERE ($entity IS NULL OR e.name=$entity) AND "
        "((r.valid_from >= $from AND r.valid_from <= $to) OR (r.valid_to >= $from AND r.valid_to <= $to)) "
        "RETURN e.name AS name, type(r) AS rel, n.name AS other, r.polarity AS polarity, "
        "r.valid_from AS valid_from, r.valid_to AS valid_to, "
        "CASE WHEN r.valid_from>=$from THEN 'appeared' ELSE 'ended' END AS change "
        "ORDER BY coalesce(r.valid_from,r.valid_to) LIMIT $top_n"
    )
    params = {"from": date_from, "to": date_to, "entity": entity, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class TopicTrendParams(_Params):
    topic: str
    granularity: str = "month"


async def topic_trend(store: Any | None, *, topic: str, granularity: str = "month") -> PrimitiveResult:
    cypher = (
        "MATCH (t:__Entity__ {name:$topic})<-[:MENTIONS]-(c:Chunk) "
        "WHERE c.doc_date_epoch IS NOT NULL "
        "RETURN c.doc_date_epoch AS epoch, count(DISTINCT c) AS n"
    )
    params = {"topic": topic}
    raw = await run_rows(store, cypher, params)
    buckets: dict[str, int] = defaultdict(int)
    for r in raw:
        buckets[epoch_days_to_period(r["epoch"], granularity)] += int(r["n"])
    rows = [{"period": p, "mentions": buckets[p]} for p in sorted(buckets)]
    return PrimitiveResult(cypher=cypher, params=params, rows=rows)


class PolarityEvolutionParams(_Params):
    name: str | None = None
    rel_type: str | None = None


async def polarity_evolution(store: Any | None, *, name: str | None = None, rel_type: str | None = None) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__)-[r]-(:__Entity__) "
        "WHERE r.valid_from IS NOT NULL AND ($name IS NULL OR e.name=$name) "
        "AND ($rel_type IS NULL OR type(r)=$rel_type) "
        "RETURN substring(r.valid_from,0,7) AS period, r.polarity AS polarity, count(*) AS n "
        "ORDER BY period"
    )
    params = {"name": name, "rel_type": rel_type}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class EntityActivityParams(_Params):
    name: str
    granularity: str = "month"


async def entity_activity(store: Any | None, *, name: str, granularity: str = "month") -> PrimitiveResult:
    res = await topic_trend(store, topic=name, granularity=granularity)
    return PrimitiveResult(cypher=res.cypher, params={"name": name}, rows=res.rows)


register(Primitive("relationship_timeline", relationship_timeline, RelationshipTimelineParams,
                   "How an entity's relations changed over time (by edge valid_from)."))
register(Primitive("whats_changed", whats_changed, WhatsChangedParams,
                   "Relations that appeared or ended in a date window."))
register(Primitive("topic_trend", topic_trend, TopicTrendParams,
                   "Mention frequency of a topic/entity over time (by chunk date)."))
register(Primitive("polarity_evolution", polarity_evolution, PolarityEvolutionParams,
                   "How affirmed/negated/uncertain shares shifted over time."))
register(Primitive("entity_activity", entity_activity, EntityActivityParams,
                   "When an entity was active/discussed over time (mention bursts)."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/dynamics.py && uv run ruff format src/analytics/primitives/dynamics.py tests/test_analytics/test_dynamics.py
git add src/analytics/primitives/dynamics.py tests/test_analytics/test_dynamics.py
git commit -m "feat(analytics): Family 4 temporal-dynamics primitives"
```

---

### Task 8: Family 3 online subset — communities + personalized_pagerank (`communities.py`)

**Files:**
- Create: `src/analytics/primitives/communities.py`
- Test: `tests/test_analytics/test_communities.py`

**Interfaces:**
- Produces: `community_overview`, `entity_communities`, `personalized_pagerank`. The first two read materialized `:Community` data (already built). `personalized_pagerank` **wraps the existing `src/graph/analysis.py::personalized_pagerank`** (seed-biased, online) — do not reimplement GDS here.
- Out of scope (Wave 1 / v1b): `top_central_entities`, `link_prediction` (need offline materialization).

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_communities.py
import pytest

from src.analytics.primitives import communities as com
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_community_overview_reads_level():
    store = _FakeStore(rows=[{"title": "Поставки", "summary": "...", "member_count": 12}])
    res = await com.community_overview(store, level=0)
    assert res.params["level"] == 0
    assert "c:Community" in res.cypher and "member_count" in res.cypher


@pytest.mark.asyncio
async def test_entity_communities_by_name():
    store = _FakeStore(rows=[{"level": 0, "title": "Поставки", "summary": "s"}])
    res = await com.entity_communities(store, name="Ромашка")
    assert ":IN_COMMUNITY" in res.cypher


@pytest.mark.asyncio
async def test_personalized_pagerank_wraps_analysis(monkeypatch):
    async def _fake_ppr(store, seeds, *, top_n=20):
        return [{"name": "X", "score": 0.4}]

    monkeypatch.setattr(com, "_analysis_ppr", _fake_ppr)
    res = await com.personalized_pagerank(object(), seeds=["A"], top_n=5)
    assert res.rows == [{"name": "X", "score": 0.4}]
    assert res.params["seeds"] == ["A"]


@pytest.mark.asyncio
async def test_personalized_pagerank_failsoft_no_seeds():
    res = await com.personalized_pagerank(None, seeds=[])
    assert res.rows == []
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/primitives/communities.py
"""Family 3 (online subset) — communities reads + seed-biased pagerank."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.graph.analysis import personalized_pagerank as _analysis_ppr


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CommunityOverviewParams(_Params):
    level: int = 0
    top_n: int = 20


async def community_overview(store: Any | None, *, level: int = 0, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (c:Community {level:$level}) "
        "RETURN c.title AS title, c.summary AS summary, c.member_count AS member_count "
        "ORDER BY c.member_count DESC LIMIT $top_n"
    )
    params = {"level": int(level), "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class EntityCommunitiesParams(_Params):
    name: str


async def entity_communities(store: Any | None, *, name: str) -> PrimitiveResult:
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community) "
        "RETURN c.level AS level, c.title AS title, c.summary AS summary"
    )
    params = {"name": name}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class PersonalizedPagerankParams(_Params):
    seeds: list[str]
    top_n: int = 20


async def personalized_pagerank(store: Any | None, *, seeds: list[str], top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    params = {"seeds": list(seeds or []), "top_n": top_n}
    if store is None or not seeds:
        return PrimitiveResult(cypher="gds.pageRank.stream(sourceNodes=$seeds)", params=params, rows=[])
    rows = await _analysis_ppr(store, list(seeds), top_n=top_n)
    return PrimitiveResult(cypher="gds.pageRank.stream(sourceNodes=$seeds)", params=params, rows=rows)


register(Primitive("community_overview", community_overview, CommunityOverviewParams,
                   "The large thematic clusters at a level (title/summary/size)."))
register(Primitive("entity_communities", entity_communities, EntityCommunitiesParams,
                   "Which thematic clusters a named entity belongs to."))
register(Primitive("personalized_pagerank", personalized_pagerank, PersonalizedPagerankParams,
                   "Entities most central relative to given seed entities (seed-biased PageRank)."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/communities.py && uv run ruff format src/analytics/primitives/communities.py tests/test_analytics/test_communities.py
git add src/analytics/primitives/communities.py tests/test_analytics/test_communities.py
git commit -m "feat(analytics): Family 3 online subset (communities + personalized_pagerank wrap)"
```

---

## Phase C — Planner · Provenance · Synthesis (`v1a`)

### Task 9: Planner (`planner.py`)

**Files:**
- Create: `src/analytics/planner.py`
- Test: `tests/test_analytics/test_planner.py`

**Interfaces:**
- Consumes: `catalog.{CATALOG, render_catalog_for_planner}`, `contracts.{AnalysisPlan, PrimitiveCall}`.
- Produces:
  - `parse_plan(raw: str, *, max_steps: int) -> AnalysisPlan` — pure, tolerant JSON parse + per-step validation against each primitive's `param_model`; unknown primitive / bad params → step dropped; any failure → `AnalysisPlan(steps=[], reason=...)`.
  - `async plan_query(question: str, llm, *, max_steps: int) -> AnalysisPlan` — builds messages, `await llm.achat(...)`, `parse_plan(...)`; fail-open to empty plan on LLM error.
  - `_SYSTEM: str` prompt.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_planner.py
import pytest

from src.analytics import planner
from src.analytics.primitives import aggregations  # noqa: F401 — registers primitives
from tests.test_analytics.conftest import _StubLLM


def test_parse_plan_valid_json():
    raw = '{"route":"catalog","steps":[{"primitive":"count_entities","params":{"type":"Organization"}}],"reason":"r"}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"
    assert plan.steps[0].params["type"] == "Organization"


def test_parse_plan_drops_unknown_primitive():
    raw = '{"route":"catalog","steps":[{"primitive":"no_such","params":{}}],"reason":"r"}'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps == []


def test_parse_plan_tolerates_prose_around_json():
    raw = 'Sure! Here:\n{"route":"catalog","steps":[{"primitive":"count_entities","params":{}}]}\nHope it helps'
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"


def test_parse_plan_caps_steps():
    raw = '{"steps":[{"primitive":"count_entities"},{"primitive":"distribution_by_type"},{"primitive":"distribution_by_relation_type"},{"primitive":"count_relationships"}]}'
    plan = planner.parse_plan(raw, max_steps=2)
    assert len(plan.steps) == 2


def test_parse_plan_garbage_returns_empty():
    assert planner.parse_plan("not json at all", max_steps=3).steps == []


def test_parse_plan_drops_step_with_bad_params():
    # count_relationships has no 'type' field but extra='ignore' → ok; use a required-field violation instead
    raw = '{"steps":[{"primitive":"entity_dossier","params":{}}]}'  # entity_dossier requires name
    plan = planner.parse_plan(raw, max_steps=3)
    assert plan.steps == []  # missing required 'name' → dropped


@pytest.mark.asyncio
async def test_plan_query_failopen_on_llm_error():
    plan = await planner.plan_query("q", _StubLLM(raises=True), max_steps=3)
    assert plan.steps == [] and "llm" in plan.reason.lower()


@pytest.mark.asyncio
async def test_plan_query_happy_path():
    llm = _StubLLM(reply='{"steps":[{"primitive":"count_entities","params":{}}],"reason":"ok"}')
    plan = await planner.plan_query("how many entities", llm, max_steps=3)
    assert plan.steps[0].primitive == "count_entities"
```

> Note: `entity_dossier` must be registered for the bad-params test. Add `from src.analytics.primitives import connections  # noqa` to the test imports.

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/planner.py
"""NL → AnalysisPlan. Plain achat + tolerant parse + strict pydantic validation.

Mirrors src/retrieval/query_planner.py: no structured/function-calling; defensive
parsing; fail-open. The number-producing work happens later in the executor.
"""

from __future__ import annotations

import json
import re
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.analytics.catalog import CATALOG, render_catalog_for_planner
from src.analytics.contracts import AnalysisPlan, PrimitiveCall
from src.logging import get_logger

logger = get_logger(__name__)

_SYSTEM = (
    "You are the planner for an analytical layer over a knowledge graph. "
    "Map the user's question to 1-3 calls from the CATALOG below. "
    "Reply with ONLY a JSON object: "
    '{"route":"catalog","steps":[{"primitive":"<name>","params":{...}}],"reason":"<why>"}. '
    "Use only catalog primitive names and only their listed params. If nothing fits, "
    'reply {"route":"catalog","steps":[],"reason":"no matching primitive"}.\n\nCATALOG:\n'
)

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(raw: str) -> dict[str, Any] | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    # try whole string, then the first {...} block
    for candidate in (raw, (_JSON_RE.search(raw) or _Empty()).group_or_none()):
        if not candidate:
            continue
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return obj
        except Exception:  # noqa: BLE001
            continue
    return None


class _Empty:
    def group_or_none(self) -> None:
        return None


def _augment(match: re.Match[str] | None) -> str | None:
    return match.group(0) if match else None


def parse_plan(raw: str, *, max_steps: int) -> AnalysisPlan:
    """Pure, tolerant parse + per-step validation. Never raises."""
    try:
        obj = None
        raw = (raw or "").strip()
        if raw:
            try:
                obj = json.loads(raw)
            except Exception:  # noqa: BLE001
                m = _JSON_RE.search(raw)
                obj = json.loads(m.group(0)) if m else None
        if not isinstance(obj, dict):
            return AnalysisPlan(steps=[], reason="planner output not JSON")

        route = obj.get("route", "catalog")
        if route not in ("catalog", "cypher"):
            route = "catalog"
        reason = str(obj.get("reason", ""))

        validated: list[PrimitiveCall] = []
        for step in obj.get("steps", []) or []:
            if not isinstance(step, dict):
                continue
            name = step.get("primitive")
            prim = CATALOG.get(name)
            if prim is None:
                continue  # unknown primitive → drop
            params = step.get("params", {}) or {}
            if not isinstance(params, dict):
                continue
            try:
                model = prim.param_model(**params)  # validates required + types
            except Exception:  # noqa: BLE001
                continue  # bad params → drop
            validated.append(PrimitiveCall(primitive=name, params=model.model_dump()))
            if len(validated) >= max_steps:
                break

        return AnalysisPlan(route=route, steps=validated, reason=reason)
    except Exception as exc:  # noqa: BLE001
        logger.warning("parse_plan failed: {e}", e=exc)
        return AnalysisPlan(steps=[], reason="parse error")


async def plan_query(question: str, llm: Any, *, max_steps: int) -> AnalysisPlan:
    messages = [
        ChatMessage(role=MessageRole.SYSTEM, content=_SYSTEM + render_catalog_for_planner()),
        ChatMessage(role=MessageRole.USER, content=question),
    ]
    try:
        resp = await llm.achat(messages)
        raw = resp.message.content or ""
    except Exception as exc:  # noqa: BLE001
        logger.warning("plan_query LLM failed: {e}", e=exc)
        return AnalysisPlan(steps=[], reason="llm error — could not plan")
    return parse_plan(raw, max_steps=max_steps)
```

> Implementer note: the dead `_extract_json`/`_Empty`/`_augment` helpers above were a false start — delete them; `parse_plan` is self-contained. Confirm the `ChatMessage` import path matches `src/retrieval/query_planner.py` (it imports from `llama_index.core.llms` or `llama_index.core.base.llms.types`) — copy that project's exact import.

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/planner.py && uv run ruff format src/analytics/planner.py tests/test_analytics/test_planner.py
git add src/analytics/planner.py tests/test_analytics/test_planner.py
git commit -m "feat(analytics): planner — NL→AnalysisPlan (tolerant parse + strict validation)"
```

---

### Task 10: Provenance (`provenance.py`)

**Files:**
- Create: `src/analytics/provenance.py`
- Test: `tests/test_analytics/test_provenance.py`

**Interfaces:**
- Produces: `assemble_provenance(plan: AnalysisPlan, steps: list[StepResult], elapsed_ms: int) -> Provenance` (deterministic; copies plan reason/route, attaches steps).
- Produces: `step_from_primitive(call: PrimitiveCall, result: PrimitiveResult) -> StepResult` — maps a `PrimitiveResult` (Task 4) to a `StepResult`, computing `row_count`, harvesting `source_chunks`.

- [ ] **Step 1: Write failing test**

```python
# tests/test_analytics/test_provenance.py
from src.analytics.catalog import PrimitiveResult
from src.analytics.contracts import AnalysisPlan, PrimitiveCall
from src.analytics.provenance import assemble_provenance, step_from_primitive


def test_step_from_primitive_counts_rows():
    call = PrimitiveCall(primitive="count_entities", params={"type": "Organization"})
    pr = PrimitiveResult(cypher="MATCH ...", params={"type": "Organization"}, rows=[{"n": 3}], source_chunks=["c1"], truncated=False)
    sr = step_from_primitive(call, pr)
    assert sr.primitive == "count_entities" and sr.row_count == 1
    assert sr.cypher == "MATCH ..." and sr.source_chunks == ["c1"]


def test_assemble_provenance_carries_plan_meta():
    plan = AnalysisPlan(route="catalog", steps=[], reason="why")
    prov = assemble_provenance(plan, steps=[], elapsed_ms=42)
    assert prov.route == "catalog" and prov.plan_reason == "why" and prov.elapsed_ms == 42
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/provenance.py
"""Deterministic provenance assembly (no LLM)."""

from __future__ import annotations

from src.analytics.catalog import PrimitiveResult
from src.analytics.contracts import AnalysisPlan, PrimitiveCall, Provenance, StepResult


def step_from_primitive(call: PrimitiveCall, result: PrimitiveResult) -> StepResult:
    return StepResult(
        primitive=call.primitive,
        params=call.params,
        cypher=result.cypher,
        rows=result.rows,
        row_count=len(result.rows),
        source_chunks=list(result.source_chunks),
        truncated=result.truncated,
    )


def assemble_provenance(plan: AnalysisPlan, steps: list[StepResult], elapsed_ms: int) -> Provenance:
    return Provenance(
        route=plan.route,
        plan_reason=plan.reason,
        steps=list(steps),
        elapsed_ms=elapsed_ms,
    )
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/provenance.py && uv run ruff format src/analytics/provenance.py tests/test_analytics/test_provenance.py
git add src/analytics/provenance.py tests/test_analytics/test_provenance.py
git commit -m "feat(analytics): deterministic provenance assembly"
```

---

### Task 11: Synthesis prompt + faithfulness checker (`synthesis.py`)

**Files:**
- Create: `src/analytics/synthesis.py`
- Test: `tests/test_analytics/test_synthesis.py`

**Interfaces:**
- Produces:
  - `build_synthesis_prompt(query: str, steps: list[StepResult]) -> list[ChatMessage]` — instructs the LLM to answer using ONLY the numbers/values in the provided rows.
  - `extract_numbers(text: str) -> set[str]` (pure) — numeric tokens in a string.
  - `numbers_in_rows(steps: list[StepResult]) -> set[str]` (pure).
  - `faithfulness_score(answer: str, steps: list[StepResult]) -> float` (pure) — fraction of answer numbers present in rows (1.0 if answer has no numbers).

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_synthesis.py
from src.analytics.contracts import StepResult
from src.analytics.synthesis import build_synthesis_prompt, extract_numbers, faithfulness_score


def test_extract_numbers():
    assert extract_numbers("There are 7 orgs and 12% growth") == {"7", "12"}


def test_faithfulness_all_present():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    assert faithfulness_score("There are 7 organizations.", steps) == 1.0


def test_faithfulness_detects_hallucinated_number():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    # answer invents "42"
    assert faithfulness_score("There are 7 orgs and 42 people.", steps) == 0.5


def test_faithfulness_no_numbers_is_one():
    steps = [StepResult(primitive="x", rows=[{"n": 7}], row_count=1)]
    assert faithfulness_score("No quantitative claim here.", steps) == 1.0


def test_build_prompt_includes_rows_and_only_rows_rule():
    steps = [StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1)]
    msgs = build_synthesis_prompt("how many?", steps)
    joined = " ".join(m.content for m in msgs)
    assert "7" in joined and "only" in joined.lower()
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/synthesis.py
"""Synthesis prompt + numeric-faithfulness checker.

The LLM verbalizes; it must not introduce numbers absent from the rows. The
faithfulness checker is a pure function used by the eval (Task 18) and can be
used as a runtime guard later.
"""

from __future__ import annotations

import json
import re

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.analytics.contracts import StepResult

_NUM_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def _norm(tok: str) -> str:
    return tok.replace(",", ".").rstrip("0").rstrip(".") if "." in tok or "," in tok else tok


def extract_numbers(text: str) -> set[str]:
    return {_norm(m.group(0)) for m in _NUM_RE.finditer(text or "")}


def numbers_in_rows(steps: list[StepResult]) -> set[str]:
    nums: set[str] = set()
    for s in steps:
        for row in s.rows:
            nums |= extract_numbers(json.dumps(row, ensure_ascii=False, default=str))
    return nums


def faithfulness_score(answer: str, steps: list[StepResult]) -> float:
    ans = extract_numbers(answer)
    if not ans:
        return 1.0
    allowed = numbers_in_rows(steps)
    hits = sum(1 for n in ans if n in allowed)
    return hits / len(ans)


def build_synthesis_prompt(query: str, steps: list[StepResult]) -> list[ChatMessage]:
    blocks = []
    for s in steps:
        blocks.append(
            f"primitive: {s.primitive}\nparams: {json.dumps(s.params, ensure_ascii=False, default=str)}\n"
            f"rows: {json.dumps(s.rows, ensure_ascii=False, default=str)}"
        )
    evidence = "\n\n".join(blocks) or "(no results)"
    system = (
        "You answer analytical questions about a knowledge graph. You are given the "
        "exact computed RESULTS. Use ONLY the numbers and values present in those rows. "
        "Do NOT invent or estimate any number that is not in the rows. If the rows are "
        "empty, say the question could not be computed. Answer concisely in the user's language."
    )
    user = f"Question: {query}\n\nRESULTS:\n{evidence}"
    return [
        ChatMessage(role=MessageRole.SYSTEM, content=system),
        ChatMessage(role=MessageRole.USER, content=user),
    ]
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/synthesis.py && uv run ruff format src/analytics/synthesis.py tests/test_analytics/test_synthesis.py
git add src/analytics/synthesis.py tests/test_analytics/test_synthesis.py
git commit -m "feat(analytics): synthesis prompt + numeric-faithfulness checker"
```

---

## Phase D — Workflow & worker (`v1a`)

### Task 12: Activities (`activities.py`)

**Files:**
- Create: `src/workflow/analytics/__init__.py`, `src/workflow/analytics/activities.py`
- Test: `tests/test_workflow/test_analytics_activities.py`

**Interfaces:**
- Consumes: `analytics.planner.plan_query`, `analytics.catalog.CATALOG`, `analytics.provenance.step_from_primitive`, `analytics.synthesis.build_synthesis_prompt`, `get_llm_pool`, `build_neo4j_graph_store`.
- Produces (Temporal activities + new contract types in `src/analytics/contracts.py`):
  - contracts: `PlanInput(query: str, max_steps: int)`, `ExecInput(call: PrimitiveCall, top_n: int, date_from_epoch: int|None, date_to_epoch: int|None)`, `SynthInput(query: str, steps: list[StepResult])`, `SynthResult(text: str)`
  - `@activity.defn analytical_plan(p: PlanInput) -> AnalysisPlan`
  - `@activity.defn execute_step(p: ExecInput) -> StepResult`
  - `@activity.defn synthesize_analytical(p: SynthInput) -> SynthResult`
  - module lists: `ANALYTICS_ACTIVITIES = [analytical_plan, execute_step]`, `ANALYTICS_LARGE_ACTIVITIES = [synthesize_analytical]`

- [ ] **Step 1: Add the four small contract types to `src/analytics/contracts.py`** (frozen), then **write failing tests**:

```python
# tests/test_workflow/test_analytics_activities.py
import pytest

from src.analytics.contracts import ExecInput, PlanInput, PrimitiveCall, StepResult, SynthInput
from src.workflow.analytics import activities as act
from src.analytics.primitives import aggregations  # noqa: F401 — register


@pytest.mark.asyncio
async def test_execute_step_runs_primitive(monkeypatch):
    # stub the store builder + the primitive's store call
    monkeypatch.setattr(act, "build_neo4j_graph_store", lambda: _Store())
    p = ExecInput(call=PrimitiveCall(primitive="count_entities", params={"type": "Organization"}),
                  top_n=20, date_from_epoch=None, date_to_epoch=None)
    sr = await act.execute_step(p)
    assert sr.primitive == "count_entities"
    assert sr.row_count == 1 and sr.rows == [{"n": 5}]


@pytest.mark.asyncio
async def test_execute_step_unknown_primitive_returns_empty(monkeypatch):
    monkeypatch.setattr(act, "build_neo4j_graph_store", lambda: _Store())
    p = ExecInput(call=PrimitiveCall(primitive="nope", params={}), top_n=20,
                  date_from_epoch=None, date_to_epoch=None)
    sr = await act.execute_step(p)
    assert sr.rows == [] and sr.row_count == 0


class _Store:
    def structured_query(self, cypher, param_map=None):
        return [{"n": 5}]
```

> The `analytical_plan` and `synthesize_analytical` activities are covered indirectly by the workflow test (Task 13) with stubbed LLMs; a direct unit test for `analytical_plan` may stub `get_llm_pool`.

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement** (and add contracts)

Add to `src/analytics/contracts.py`:

```python
class PlanInput(_Frozen):
    query: str
    max_steps: int = 3


class ExecInput(_Frozen):
    call: PrimitiveCall
    top_n: int = 20
    date_from_epoch: int | None = None
    date_to_epoch: int | None = None


class SynthInput(_Frozen):
    query: str
    steps: list[StepResult] = Field(default_factory=list)


class SynthResult(_Frozen):
    text: str = ""
```

Create `src/workflow/analytics/activities.py`:

```python
# src/workflow/analytics/activities.py
"""Temporal activities for the analytical layer."""

from __future__ import annotations

from temporalio import activity

from src.analytics.catalog import CATALOG, PrimitiveResult
from src.analytics.contracts import (
    AnalysisPlan,
    ExecInput,
    PlanInput,
    StepResult,
    SynthInput,
    SynthResult,
)
from src.analytics.planner import plan_query
from src.analytics.provenance import step_from_primitive
from src.analytics.synthesis import build_synthesis_prompt
from src.graph.store import build_neo4j_graph_store
from src.retrieval.llm_pool import get_llm_pool


@activity.defn
async def analytical_plan(p: PlanInput) -> AnalysisPlan:
    llm = get_llm_pool().get("plan")
    return await plan_query(p.query, llm, max_steps=p.max_steps)


@activity.defn
async def execute_step(p: ExecInput) -> StepResult:
    prim = CATALOG.get(p.call.primitive)
    if prim is None:
        return StepResult(primitive=p.call.primitive, params=p.call.params, rows=[], row_count=0)
    store = build_neo4j_graph_store()
    params = dict(p.call.params)
    # inject default top_n when the primitive accepts it and the planner omitted it
    if "top_n" in prim.param_model.model_fields and "top_n" not in params:
        params["top_n"] = p.top_n
    try:
        result: PrimitiveResult = await prim.fn(store, **params)
    except TypeError:
        # planner produced params the fn doesn't accept → fail-soft empty
        result = PrimitiveResult(cypher="", params=params, rows=[])
    return step_from_primitive(p.call.model_copy(update={"params": params}), result)


@activity.defn
async def synthesize_analytical(p: SynthInput) -> SynthResult:
    if not any(s.rows for s in p.steps):
        return SynthResult(text="Не удалось вычислить ответ по доступным аналитическим примитивам.")
    llm = get_llm_pool().get("synthesis")
    resp = await llm.achat(build_synthesis_prompt(p.query, p.steps))
    return SynthResult(text=(resp.message.content or "").strip())


ANALYTICS_ACTIVITIES = [analytical_plan, execute_step]
ANALYTICS_LARGE_ACTIVITIES = [synthesize_analytical]
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/contracts.py src/workflow/analytics/ && uv run ruff format src/analytics/contracts.py src/workflow/analytics/ tests/test_workflow/test_analytics_activities.py
git add src/analytics/contracts.py src/workflow/analytics/__init__.py src/workflow/analytics/activities.py tests/test_workflow/test_analytics_activities.py
git commit -m "feat(analytics): Temporal activities (plan/execute_step/synthesize)"
```

---

### Task 13: Workflow (`workflow.py`)

**Files:**
- Create: `src/workflow/analytics/workflow.py`
- Test: `tests/test_workflow/test_analytics_workflow.py`

**Interfaces:**
- Consumes: `AnalyzeParams`, the three activities by name, `assemble_provenance`.
- Produces: `@workflow.defn class AnalyticalQueryWorkflow: @workflow.run async def run(self, params: AnalyzeParams) -> AnalyticsOutcome` + `@workflow.query get_state`.

- [ ] **Step 1: Write the failing test** (Temporal test environment with stubbed activities; mirror an existing workflow test in `tests/test_workflow/`)

```python
# tests/test_workflow/test_analytics_workflow.py
import uuid

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from src.analytics.contracts import AnalysisPlan, AnalyzeParams, PrimitiveCall, StepResult, SynthResult
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow


@activity.defn(name="analytical_plan")
async def _plan(p) -> AnalysisPlan:
    return AnalysisPlan(route="catalog", steps=[PrimitiveCall(primitive="count_entities", params={})], reason="r")


@activity.defn(name="execute_step")
async def _exec(p) -> StepResult:
    return StepResult(primitive="count_entities", rows=[{"n": 7}], row_count=1, cypher="MATCH ...")


@activity.defn(name="synthesize_analytical")
async def _synth(p) -> SynthResult:
    return SynthResult(text="There are 7 entities.")


@pytest.mark.asyncio
async def test_analytical_workflow_plan_execute_synthesize():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-analytics",
            workflows=[AnalyticalQueryWorkflow],
            activities=[_plan, _exec, _synth],
        ):
            out = await env.client.execute_workflow(
                AnalyticalQueryWorkflow.run,
                AnalyzeParams(query="how many entities"),
                id=f"t-{uuid.uuid4().hex}",
                task_queue="test-analytics",
            )
    assert out.answer == "There are 7 entities."
    assert out.provenance.steps[0].rows == [{"n": 7}]
    assert out.provenance.plan_reason == "r"
```

> Implementer note: confirm the Temporal test idiom used by existing tests in `tests/test_workflow/` (the project may use `WorkflowEnvironment.start_local()` or a shared fixture). Match it; skip the test if Temporal isn't available, as sibling workflow tests do.

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/workflow/analytics/workflow.py
"""AnalyticalQueryWorkflow — plan → execute primitives → synthesize + provenance."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.analytics.contracts import (
        AnalysisPlan,
        AnalyticsOutcome,
        AnalyzeParams,
        ExecInput,
        PlanInput,
        StepResult,
        SynthInput,
        SynthResult,
    )
    from src.analytics.provenance import assemble_provenance
    from src.config import settings

# Match the search workflow's policy/timeouts (src/workflow/search/_retry.py).
_FAST_RETRY = RetryPolicy(maximum_attempts=3, initial_interval=timedelta(seconds=1),
                          maximum_interval=timedelta(seconds=30))
_LLM_S2C = timedelta(hours=3)
_LLM_START = timedelta(hours=1)
_DB_START = timedelta(minutes=5)


@workflow.defn
class AnalyticalQueryWorkflow:
    def __init__(self) -> None:
        self._state: dict = {"phase": "init"}

    @workflow.query
    def get_state(self) -> dict:
        return dict(self._state)

    @workflow.run
    async def run(self, params: AnalyzeParams) -> AnalyticsOutcome:
        started = workflow.now()
        self._state["phase"] = "plan"
        plan: AnalysisPlan = await workflow.execute_activity(
            "analytical_plan",
            PlanInput(query=params.query, max_steps=settings.analytics.max_steps),
            result_type=AnalysisPlan,
            start_to_close_timeout=_LLM_START,
            schedule_to_close_timeout=_LLM_S2C,
            retry_policy=_FAST_RETRY,
        )

        self._state["phase"] = "execute"
        steps: list[StepResult] = []
        for call in plan.steps[: settings.analytics.max_steps]:
            sr: StepResult = await workflow.execute_activity(
                "execute_step",
                ExecInput(call=call, top_n=params.top_n,
                          date_from_epoch=params.date_from_epoch,
                          date_to_epoch=params.date_to_epoch),
                result_type=StepResult,
                start_to_close_timeout=_DB_START,
                schedule_to_close_timeout=_LLM_S2C,
                retry_policy=_FAST_RETRY,
            )
            steps.append(sr)

        self._state["phase"] = "synthesize"
        synth: SynthResult = await workflow.execute_activity(
            "synthesize_analytical",
            SynthInput(query=params.query, steps=steps),
            result_type=SynthResult,
            task_queue=settings.temporal.large_task_queue,
            start_to_close_timeout=_LLM_START,
            schedule_to_close_timeout=_LLM_S2C,
            retry_policy=_FAST_RETRY,
        )

        elapsed = int((workflow.now() - started).total_seconds() * 1000)
        self._state["phase"] = "done"
        return AnalyticsOutcome(
            query=params.query,
            answer=synth.text,
            provenance=assemble_provenance(plan, steps, elapsed),
            latency_ms=elapsed,
        )
```

> Implementer note: copy the exact `RetryPolicy`/timeout constants from `src/workflow/search/_retry.py` (`FAST_RETRY`, `LLM_START_TO_CLOSE`, `LLM_SCHEDULE_TO_CLOSE`) and import them rather than redefining, to stay consistent. Confirm the `with workflow.unsafe.imports_passed_through()` idiom matches the search workflow.

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/workflow/analytics/workflow.py && uv run ruff format src/workflow/analytics/workflow.py tests/test_workflow/test_analytics_workflow.py
git add src/workflow/analytics/workflow.py tests/test_workflow/test_analytics_workflow.py
git commit -m "feat(analytics): AnalyticalQueryWorkflow (plan→execute→synthesize+provenance)"
```

---

### Task 14: Worker registration

**Files:**
- Modify: `src/workflow/worker.py` (the `search` group: add the workflow + `ANALYTICS_ACTIVITIES`; the `large` group: add `ANALYTICS_LARGE_ACTIVITIES`)
- Test: `tests/test_workflow/test_worker_registration.py`

**Interfaces:**
- Consumes: `AnalyticalQueryWorkflow`, `ANALYTICS_ACTIVITIES`, `ANALYTICS_LARGE_ACTIVITIES`.

- [ ] **Step 1: Write the failing test** (assert the worker build for `search`/`large` includes the new names; use the existing `_build_worker`)

```python
# tests/test_workflow/test_worker_registration.py
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow
from src.workflow.analytics.activities import analytical_plan, execute_step, synthesize_analytical
from src.workflow import worker as w


def test_search_group_registers_analytics():
    # Inspect the static registration tables rather than building a live Worker.
    assert AnalyticalQueryWorkflow in w.SEARCH_WORKFLOWS
    assert analytical_plan in (w.SEARCH_ACTIVITIES + w.SEARCH_V2_ACTIVITIES + w.ANALYTICS_ACTIVITIES)
    assert execute_step in w.ANALYTICS_ACTIVITIES
    assert synthesize_analytical in w.ANALYTICS_LARGE_ACTIVITIES
```

> Implementer note: the exact symbols (`SEARCH_WORKFLOWS` list vs inline list in `_build_worker`) depend on `worker.py`'s structure. If workflows are listed inline in `_build_worker`, refactor the `search` group's `workflows=[...]` into a module-level `SEARCH_WORKFLOWS` list first (small, safe), then append `AnalyticalQueryWorkflow`. Adjust the test to whatever names you expose.

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement** — in `src/workflow/worker.py`:
  1. import `from src.workflow.analytics.workflow import AnalyticalQueryWorkflow` and `from src.workflow.analytics.activities import ANALYTICS_ACTIVITIES, ANALYTICS_LARGE_ACTIVITIES`.
  2. In the `search` group: add `AnalyticalQueryWorkflow` to its `workflows=[...]`, and add `+ ANALYTICS_ACTIVITIES` to its `activities=...`.
  3. In the `large` group: add `+ ANALYTICS_LARGE_ACTIVITIES` to its `activities=[synthesize_answer]`.

```python
# search group (illustrative — match existing structure):
workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow, GlobalSearchWorkflow,
           DriftSearchWorkflow, AutoSearchWorkflow, AnalyticalQueryWorkflow],
activities=SEARCH_ACTIVITIES + SEARCH_V2_ACTIVITIES + ANALYTICS_ACTIVITIES,
# large group:
activities=[synthesize_answer] + ANALYTICS_LARGE_ACTIVITIES,
```

- [ ] **Step 4: Run tests** → PASS (+ full suite to confirm no import cycle: `uv run pytest -q`). **Step 5: Lint + commit**

```bash
uv run ruff check src/workflow/worker.py && uv run ruff format src/workflow/worker.py tests/test_workflow/test_worker_registration.py
git add src/workflow/worker.py tests/test_workflow/test_worker_registration.py
git commit -m "feat(analytics): register AnalyticalQueryWorkflow + activities in worker"
```

---

## Phase E — Surfaces (`v1a`)

### Task 15: HTTP endpoint `POST /api/v1/analyze`

**Files:**
- Create: `src/models/analyze.py`, `src/api/routes/analyze.py`
- Modify: `src/api/main.py` (include router)
- Test: `tests/test_api/test_analyze_route.py`

**Interfaces:**
- Produces: `AnalyzeRequest(query: str, date_from: str|None, date_to: str|None, top_n: int=20)`, `AnalyzeResponse(query, answer, provenance: Provenance, latency_ms)`; route `analyze` that maps request→`AnalyzeParams` (ISO→epoch via `iso_to_epoch_days`), starts `AnalyticalQueryWorkflow`, awaits result, maps to response.

- [ ] **Step 1: Write the failing test** (mirror an existing route test that stubs the Temporal client)

```python
# tests/test_api/test_analyze_route.py
from src.models.analyze import AnalyzeRequest
from src.api.routes.analyze import _to_params


def test_request_to_params_converts_dates():
    req = AnalyzeRequest(query="q", date_from="2024-01-01", date_to="2024-12-31", top_n=10)
    p = _to_params(req)
    assert p.query == "q" and p.top_n == 10
    assert p.date_from_epoch == 19723  # date(2024,1,1).toordinal() - 719163
    assert p.date_to_epoch is not None
```

> A full HTTP round-trip test requires the Temporal client; mirror how `tests/test_api/` stubs `get_temporal_client` for `search_v2`. At minimum unit-test `_to_params`. Add an integration test only if the search route has one to copy.

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/models/analyze.py
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from src.analytics.contracts import Provenance
from src.retrieval.date_filters import iso_to_epoch_days


class AnalyzeRequest(BaseModel):
    query: str
    date_from: str | None = None
    date_to: str | None = None
    top_n: int = 20

    @field_validator("date_from", "date_to")
    @classmethod
    def _valid_iso(cls, v: str | None) -> str | None:
        if v is None:
            return v
        iso_to_epoch_days(v)  # raises ValueError → 422
        return v


class AnalyzeResponse(BaseModel):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int = 0
```

```python
# src/api/routes/analyze.py
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from temporalio.common import WorkflowIDReusePolicy

from src.analytics.contracts import AnalyzeParams
from src.api.auth import require_api_key
from src.config import settings
from src.models.analyze import AnalyzeRequest, AnalyzeResponse
from src.retrieval.date_filters import iso_to_epoch_days
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow
from src.workflow.client import get_temporal_client

router = APIRouter(tags=["analytics"])


def _to_params(req: AnalyzeRequest) -> AnalyzeParams:
    return AnalyzeParams(
        query=req.query,
        top_n=req.top_n,
        date_from_epoch=iso_to_epoch_days(req.date_from) if req.date_from else None,
        date_to_epoch=iso_to_epoch_days(req.date_to) if req.date_to else None,
    )


@router.post("/analyze", response_model=AnalyzeResponse, dependencies=[Depends(require_api_key)],
             summary="Plan→compute→synthesize analytical Q&A over the knowledge graph")
async def analyze(req: AnalyzeRequest) -> AnalyzeResponse:
    request_id = uuid.uuid4().hex
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            AnalyticalQueryWorkflow.run,
            _to_params(req),
            id=f"analyze-{request_id}",
            task_queue=settings.temporal.search_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        outcome = await handle.result()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Analyze failed: {exc}") from exc
    return AnalyzeResponse(query=outcome.query, answer=outcome.answer,
                           provenance=outcome.provenance, latency_ms=outcome.latency_ms)
```

In `src/api/main.py`, add next to the other includes:
```python
from src.api.routes import analyze as analyze_routes
app.include_router(analyze_routes.router, prefix="/api/v1")
```

> Implementer note: confirm `_EPOCH`/ordinal math — `date(2024,1,1).toordinal() == 738886`; `date(1970,1,1).toordinal() == 719163`; difference `19723`. Verify `iso_to_epoch_days("2024-01-01") == 19723` in a REPL before locking the test's literal.

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/models/analyze.py src/api/routes/analyze.py src/api/main.py && uv run ruff format src/models/analyze.py src/api/routes/analyze.py src/api/main.py tests/test_api/test_analyze_route.py
git add src/models/analyze.py src/api/routes/analyze.py src/api/main.py tests/test_api/test_analyze_route.py
git commit -m "feat(analytics): POST /api/v1/analyze endpoint"
```

---

### Task 16: MCP-1 tool `kb_analyze`

**Files:**
- Modify: `src/mcp/search_server.py` (add one `@mcp.tool`)
- Test: manual smoke (MCP tools have no unit test in-repo); add a thin import test.

**Interfaces:** mirrors `kb_search` — starts `AnalyticalQueryWorkflow`, returns `{query, answer, provenance, latency_ms}`.

- [ ] **Step 1: Implement** (add to `src/mcp/search_server.py`)

```python
@mcp.tool(timeout=1800)
async def kb_analyze(query: str, ctx: Context, top_n: int = 20) -> dict[str, Any]:
    """Analytical Q&A: computes counts/rankings/connections/centrality/temporal facts
    over the knowledge graph and returns an answer plus a deterministic provenance
    chain (which primitives ran, the rows, source chunks). USE FOR quantitative /
    structural / "how many / who is most central / how connected / what changed"
    questions. For 'what do the documents say', use kb_search instead."""
    from src.analytics.contracts import AnalyzeParams
    from src.workflow.analytics.workflow import AnalyticalQueryWorkflow

    handle = await (await get_temporal_client()).start_workflow(
        AnalyticalQueryWorkflow.run,
        AnalyzeParams(query=query, top_n=top_n),
        id=f"mcp-analyze-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await handle.result()
    return {
        "query": outcome.query,
        "answer": outcome.answer,
        "provenance": outcome.provenance.model_dump(),
        "latency_ms": outcome.latency_ms,
    }
```

- [ ] **Step 2: Smoke import test**

```python
# tests/test_mcp/test_kb_analyze_registered.py
def test_kb_analyze_is_registered():
    import src.mcp.search_server as s
    # FastMCP stores tools; confirm the symbol exists / is decorated
    assert hasattr(s, "kb_analyze")
```

- [ ] **Step 3: Run** `uv run pytest tests/test_mcp/test_kb_analyze_registered.py -q` → PASS. **Step 4: Lint + commit**

```bash
uv run ruff check src/mcp/search_server.py && uv run ruff format src/mcp/search_server.py
git add src/mcp/search_server.py tests/test_mcp/test_kb_analyze_registered.py
git commit -m "feat(analytics): kb_analyze MCP-1 tool"
```

> MCP-2 atomic primitive tools are deferred (optional, low value for Wave 0).

---

### Task 17: CLI `scripts/analyze.py`

**Files:**
- Create: `scripts/analyze.py`
- Test: covered by the workflow/route tests; CLI is a thin runner (no unit test, mirrors `scripts/check_ingestion.py`).

**Interfaces:** `python -m scripts.analyze "<question>" [--top-n N]` → prints answer + provenance JSON.

- [ ] **Step 1: Implement**

```python
# scripts/analyze.py
"""Run one analytical query against the knowledge graph.

Usage::

    python -m scripts.analyze "Сколько организаций в графе?"
    python -m scripts.analyze "Кто чаще всего упоминается с Ромашкой?" --top-n 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from temporalio.common import WorkflowIDReusePolicy  # noqa: E402

from src.analytics.contracts import AnalyzeParams  # noqa: E402
from src.config import settings  # noqa: E402
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow  # noqa: E402
from src.workflow.client import get_temporal_client  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()

    client = await get_temporal_client()
    handle = await client.start_workflow(
        AnalyticalQueryWorkflow.run,
        AnalyzeParams(query=args.query, top_n=args.top_n),
        id=f"cli-analyze-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await handle.result()
    print(outcome.answer)
    print("\n--- provenance ---")
    print(json.dumps(outcome.provenance.model_dump(), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Lint + commit**

```bash
uv run ruff check scripts/analyze.py && uv run ruff format scripts/analyze.py
git add scripts/analyze.py
git commit -m "feat(analytics): CLI scripts/analyze.py"
```

---

## Phase F — Numeric-faithfulness eval (`v1a`)

### Task 18: Eval harness for numeric faithfulness

**Files:**
- Create: `tests/eval/test_analytics_faithfulness.py`, `tests/eval/golden_analytics/cases.json`
- Test: the file itself is the eval.

**Interfaces:** reuses `synthesis.faithfulness_score`. Two layers: (a) **pure** deterministic tests of `faithfulness_score` over golden (answer, rows) pairs — always run; (b) an **end-to-end** test gated on a live LLM/stack (skip if unavailable), mirroring `tests/eval/test_answer_quality.py`.

- [ ] **Step 1: Create golden cases**

```json
// tests/eval/golden_analytics/cases.json
[
  {"id": "count-ok", "rows": [{"n": 7}], "answer": "В графе 7 организаций.", "min_faithfulness": 1.0},
  {"id": "halluc", "rows": [{"n": 7}], "answer": "7 организаций и 42 человека.", "max_faithfulness": 0.6},
  {"id": "ranking-ok", "rows": [{"name": "A", "mentions": 9}, {"name": "B", "mentions": 4}],
   "answer": "Чаще всего упоминается A (9), затем B (4).", "min_faithfulness": 1.0}
]
```

- [ ] **Step 2: Write the eval test**

```python
# tests/eval/test_analytics_faithfulness.py
import json
from pathlib import Path

from src.analytics.contracts import StepResult
from src.analytics.synthesis import faithfulness_score

_CASES = json.loads((Path(__file__).parent / "golden_analytics" / "cases.json").read_text(encoding="utf-8"))


def _steps(rows):
    return [StepResult(primitive="x", rows=rows, row_count=len(rows))]


def test_faithfulness_golden_cases():
    for c in _CASES:
        score = faithfulness_score(c["answer"], _steps(c["rows"]))
        if "min_faithfulness" in c:
            assert score >= c["min_faithfulness"], f"{c['id']}: {score}"
        if "max_faithfulness" in c:
            assert score <= c["max_faithfulness"], f"{c['id']}: {score}"
```

- [ ] **Step 3: Run** `uv run pytest tests/eval/test_analytics_faithfulness.py -q` → PASS. **Step 4: Lint + commit**

```bash
uv run ruff check tests/eval/test_analytics_faithfulness.py && uv run ruff format tests/eval/test_analytics_faithfulness.py
git add tests/eval/test_analytics_faithfulness.py tests/eval/golden_analytics/cases.json
git commit -m "test(analytics): numeric-faithfulness eval (golden cases)"
```

---

## Phase G — E1: first_seen novelty

### Task 19: `created_at` index + sentinel backfill

**Files:**
- Modify: `src/graph/index.py` (add `ensure_first_seen_indexes(store) -> bool`)
- Modify: `src/workflow/activities/build_property_graph.py` (call it in the `ensure_*` block ~`:109-111`)
- Create: `scripts/backfill_first_seen.py`
- Test: `tests/test_graph/test_first_seen_index.py`

**Interfaces:** `ensure_first_seen_indexes(store)` creates a RANGE index on `__Entity__.created_at` and a relationship index on `created_at`. Backfill stamps `settings.events.backfill_sentinel` on all elements with `created_at IS NULL`, batched.

- [ ] **Step 1: Write failing test** (assert idempotent helper issues the index DDL)

```python
# tests/test_graph/test_first_seen_index.py
from src.graph.index import ensure_first_seen_indexes


class _Rec:
    def __init__(self):
        self.queries = []

    def structured_query(self, cypher, param_map=None):
        self.queries.append(cypher)
        return []


def test_ensure_first_seen_indexes_creates_entity_and_rel_index():
    store = _Rec()
    assert ensure_first_seen_indexes(store) is True
    joined = " ".join(store.queries)
    assert "created_at" in joined
    assert "IF NOT EXISTS" in joined
    assert any("FOR (e:__Entity__)" in q for q in store.queries)
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement** — add to `src/graph/index.py` (mirror `ensure_chunk_date_indexes`):

```python
ENTITY_CREATED_AT_INDEX_CYPHER = (
    "CREATE INDEX entity_created_at IF NOT EXISTS FOR (e:__Entity__) ON (e.created_at)"
)
REL_CREATED_AT_INDEX_CYPHER = (
    "CREATE INDEX rel_created_at IF NOT EXISTS FOR ()-[r]-() ON (r.created_at)"
)


def ensure_first_seen_indexes(store) -> bool:
    """Idempotently create created_at indexes on entities and relationships (E1)."""
    ok = True
    for cypher in (ENTITY_CREATED_AT_INDEX_CYPHER, REL_CREATED_AT_INDEX_CYPHER):
        try:
            store.structured_query(cypher)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ensure_first_seen_indexes failed: {e}", e=exc)
            ok = False
    return ok
```

Call it in `build_property_graph.py` alongside the other `ensure_*` calls (~`:109-111`).

Create `scripts/backfill_first_seen.py`:

```python
# scripts/backfill_first_seen.py
"""One-time E1 backfill: stamp a sentinel created_at on pre-existing graph
elements so they are never mis-flagged as "new". Run BEFORE enabling
EVENTS_FIRST_SEEN_ENABLED.

Usage::  python -m scripts.backfill_first_seen
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings  # noqa: E402
from src.graph.index import ensure_first_seen_indexes  # noqa: E402
from src.graph.store import build_neo4j_graph_store  # noqa: E402

_ENT = (
    "MATCH (e:__Entity__) WHERE e.created_at IS NULL "
    "CALL { WITH e SET e.created_at = $sentinel } IN TRANSACTIONS OF 5000 ROWS"
)
_REL = (
    "MATCH ()-[r]->() WHERE r.created_at IS NULL "
    "CALL { WITH r SET r.created_at = $sentinel } IN TRANSACTIONS OF 5000 ROWS"
)


async def main() -> None:
    store = build_neo4j_graph_store()
    ensure_first_seen_indexes(store)
    sentinel = settings.events.backfill_sentinel
    for cypher in (_ENT, _REL):
        store.structured_query(cypher, param_map={"sentinel": sentinel})
    print(f"backfill complete (sentinel={sentinel})")


if __name__ == "__main__":
    asyncio.run(main())
```

> Implementer note: `CALL { ... } IN TRANSACTIONS` requires an auto-commit/`session.run` context — confirm `Neo4jPropertyGraphStore.structured_query` runs in an implicit transaction that permits `CALL IN TRANSACTIONS`. If not, fall back to a plain `MATCH ... SET` (acceptable for modest graphs) or drive batches in Python with `SKIP/LIMIT`.

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/graph/index.py scripts/backfill_first_seen.py && uv run ruff format src/graph/index.py scripts/backfill_first_seen.py tests/test_graph/test_first_seen_index.py
git add src/graph/index.py src/workflow/activities/build_property_graph.py scripts/backfill_first_seen.py tests/test_graph/test_first_seen_index.py
git commit -m "feat(events): created_at indexes + sentinel backfill (E1)"
```

---

### Task 20: `ON CREATE` emulated stamping in the ingest write-path

**Files:**
- Create: `src/graph/first_seen.py` (`stamp_first_seen(...)`)
- Modify: `src/workflow/activities/build_property_graph.py` (call after `upsert_relations`, gated by `settings.events.first_seen_enabled`)
- Test: `tests/test_graph/test_first_seen_stamp.py`

**Interfaces:**
- Produces: `stamp_first_seen(store, *, entity_names: list[str], relations: list[tuple[str,str,str]], ingest_epoch: int, doc_id: str) -> None` — sets `created_at`/`first_doc_id` **only WHERE created_at IS NULL**.

- [ ] **Step 1: Write the failing test** (the core anti-re-report guarantee: stamp uses `WHERE created_at IS NULL`)

```python
# tests/test_graph/test_first_seen_stamp.py
import pytest

from src.graph.first_seen import stamp_first_seen


class _Rec:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        return []


def test_stamp_only_sets_where_created_at_is_null():
    store = _Rec()
    stamp_first_seen(store, entity_names=["A", "B"],
                     relations=[("A", "OWNS", "B")], ingest_epoch=19797, doc_id="d1")
    joined = " ".join(c[0] for c in store.calls)
    assert "created_at IS NULL" in joined          # ON CREATE semantics
    assert "SET e.created_at" in joined or "SET r.created_at" in joined
    # entity names + ts + doc passed
    ent_call = next(c for c in store.calls if "e.created_at" in c[0])
    assert ent_call[1]["names"] == ["A", "B"]
    assert ent_call[1]["ts"] == 19797 and ent_call[1]["doc_id"] == "d1"


def test_stamp_noop_on_empty():
    store = _Rec()
    stamp_first_seen(store, entity_names=[], relations=[], ingest_epoch=1, doc_id="d")
    assert store.calls == []
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/graph/first_seen.py
"""E1 — emulate ON CREATE stamping (created_at/first_doc_id) post-upsert.

The entity/relationship MERGE lives inside llama_index, so we cannot set
ON CREATE there. Instead, after upsert, stamp only elements that have no
created_at yet, scoped to this ingest's elements. Combined with the one-time
sentinel backfill (scripts/backfill_first_seen.py), this means: a node created
this pass gets stamped now; a re-mentioned old node keeps its original stamp.
"""

from __future__ import annotations

from typing import Any

from src.logging import get_logger

logger = get_logger(__name__)

_STAMP_ENTITIES = (
    "UNWIND $names AS nm MATCH (e:__Entity__ {name: nm}) "
    "WHERE e.created_at IS NULL "
    "SET e.created_at = $ts, e.first_doc_id = $doc_id"
)
_STAMP_RELS = (
    "UNWIND $rels AS rel "
    "MATCH (a:__Entity__ {name: rel.src})-[r]->(b:__Entity__ {name: rel.tgt}) "
    "WHERE type(r) = rel.label AND r.created_at IS NULL "
    "SET r.created_at = $ts, r.first_doc_id = $doc_id"
)


def stamp_first_seen(store: Any | None, *, entity_names: list[str],
                     relations: list[tuple[str, str, str]], ingest_epoch: int, doc_id: str) -> None:
    if store is None:
        return
    try:
        if entity_names:
            store.structured_query(_STAMP_ENTITIES,
                                   param_map={"names": list(entity_names), "ts": ingest_epoch, "doc_id": doc_id})
        if relations:
            rels = [{"src": s, "label": lbl, "tgt": t} for (s, lbl, t) in relations]
            store.structured_query(_STAMP_RELS,
                                   param_map={"rels": rels, "ts": ingest_epoch, "doc_id": doc_id})
    except Exception as exc:  # noqa: BLE001
        logger.warning("stamp_first_seen failed: {e}", e=exc)
```

In `build_property_graph.py`, after the relations upsert, gated by the flag:

```python
from src.config import settings
from src.graph.first_seen import stamp_first_seen
from src.retrieval.date_filters import today_epoch_days

if settings.events.first_seen_enabled:
    ent_names = [e.name for e in entities]
    rel_triples = [(r.source_id, r.label, r.target_id) for r in relations]
    doc_id = ...  # derive from activity params / chunk metadata available here
    await asyncio.to_thread(
        stamp_first_seen, graph_store,
        entity_names=ent_names, relations=rel_triples,
        ingest_epoch=today_epoch_days(), doc_id=doc_id,
    )
```

> Implementer note (verify at impl): (1) `Relation.source_id`/`.target_id` must equal the entity `name` keys used in the MERGE — confirm in `merge.py`/llama_index; if they are synthetic ids, map them back to names. (2) `doc_id` availability inside the activity — pull from the activity's params or the chunk metadata that `parse_and_chunk` stamped. (3) Keep `first_seen_enabled` default `False`; the activation order at deploy is: ship → run `scripts.backfill_first_seen` → set `EVENTS_FIRST_SEEN_ENABLED=true`.

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/graph/first_seen.py && uv run ruff format src/graph/first_seen.py tests/test_graph/test_first_seen_stamp.py
git add src/graph/first_seen.py src/workflow/activities/build_property_graph.py tests/test_graph/test_first_seen_stamp.py
git commit -m "feat(events): ON-CREATE-emulated first_seen stamping in ingest write-path (E1)"
```

---

### Task 21: Event read primitives — `new_events`, `entity_new_connections`

**Files:**
- Create: `src/analytics/primitives/events.py`
- Test: `tests/test_analytics/test_events.py`

**Interfaces:** registered primitives reading `created_at >= since` (since = `today_epoch_days() - window_days`). `new_events(window_days, type, top_n)` unions new entities + new edges; `entity_new_connections(name, window_days, top_n)` returns new edges on a named entity.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_events.py
import pytest

from src.analytics.primitives import events as ev
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_new_events_filters_by_created_at(monkeypatch):
    monkeypatch.setattr(ev, "today_epoch_days", lambda: 19800)
    store = _FakeStore(by_call=[
        [{"name": "NewCo", "type": "Organization", "created_at": 19799}],   # new entities
        [{"src": "A", "rel": "OWNS", "tgt": "NewCo", "created_at": 19799}],  # new edges
    ])
    res = await ev.new_events(store, window_days=14)
    assert res.params["since"] == 19800 - 14
    assert "created_at >= $since" in res.cypher
    kinds = {r["kind"] for r in res.rows}
    assert kinds == {"entity", "edge"}


@pytest.mark.asyncio
async def test_entity_new_connections(monkeypatch):
    monkeypatch.setattr(ev, "today_epoch_days", lambda: 19800)
    store = _FakeStore(rows=[{"rel": "OWNS", "other": "NewCo", "created_at": 19799}])
    res = await ev.entity_new_connections(store, name="A", window_days=30)
    assert res.params["name"] == "A" and res.params["since"] == 19770
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement**

```python
# src/analytics/primitives/events.py
"""E1 read side — first_seen-based "what's new" primitives."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows
from src.config import settings
from src.retrieval.date_filters import today_epoch_days


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


_NEW_ENTITIES = (
    "MATCH (e:__Entity__) WHERE e.created_at >= $since "
    "RETURN e.name AS name, [l IN labels(e) WHERE l<>'__Entity__'][0] AS type, "
    "e.created_at AS created_at, e.first_doc_id AS first_doc_id "
    "ORDER BY e.created_at DESC LIMIT $top_n"
)
_NEW_EDGES = (
    "MATCH (a:__Entity__)-[r]->(b:__Entity__) WHERE r.created_at >= $since "
    "RETURN a.name AS src, type(r) AS rel, b.name AS tgt, r.created_at AS created_at, "
    "r.first_doc_id AS first_doc_id ORDER BY r.created_at DESC LIMIT $top_n"
)


class NewEventsParams(_Params):
    window_days: int | None = None
    type: str | None = None
    top_n: int = 25


async def new_events(store: Any | None, *, window_days: int | None = None, type: str | None = None, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    wd = window_days if window_days is not None else settings.events.new_window_days
    since = today_epoch_days() - int(wd)
    params = {"since": since, "top_n": top_n, "type": type}
    ents = await run_rows(store, _NEW_ENTITIES, params)
    edges = await run_rows(store, _NEW_EDGES, params)
    if type:
        ents = [e for e in ents if e.get("type") == type]
    rows = [{"kind": "entity", **e} for e in ents] + [{"kind": "edge", **e} for e in edges]
    rows.sort(key=lambda r: r.get("created_at", 0), reverse=True)
    cypher = _NEW_ENTITIES + " ;; " + _NEW_EDGES
    return PrimitiveResult(cypher=cypher, params=params, rows=rows[:top_n])


class EntityNewConnectionsParams(_Params):
    name: str
    window_days: int | None = None
    top_n: int = 25


async def entity_new_connections(store: Any | None, *, name: str, window_days: int | None = None, top_n: int = 25) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=25)
    wd = window_days if window_days is not None else settings.events.new_window_days
    since = today_epoch_days() - int(wd)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__) WHERE r.created_at >= $since "
        "RETURN type(r) AS rel, n.name AS other, r.created_at AS created_at, r.first_doc_id AS first_doc_id "
        "ORDER BY r.created_at DESC LIMIT $top_n"
    )
    params = {"name": name, "since": since, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("new_events", new_events, NewEventsParams,
                   "Entities/edges that first appeared in the graph within a recent window (first_seen)."))
register(Primitive("entity_new_connections", entity_new_connections, EntityNewConnectionsParams,
                   "New connections on a named entity within a recent window."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/events.py && uv run ruff format src/analytics/primitives/events.py tests/test_analytics/test_events.py
git add src/analytics/primitives/events.py tests/test_analytics/test_events.py
git commit -m "feat(events): new_events + entity_new_connections primitives (E1)"
```

---

## Phase H — P1: knowledge-quality flags

### Task 22: Quality primitives — `contradictions`, `orphans`

**Files:**
- Create: `src/analytics/primitives/quality.py` (part 1)
- Test: `tests/test_analytics/test_quality.py` (part 1)

**Interfaces:** `contradictions(top_n)` (affirmed vs negated **only when validity windows overlap** — a temporal change is NOT a contradiction); `orphans(min_degree, top_n)`.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_analytics/test_quality.py
import pytest

from src.analytics.primitives import quality as q
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_contradictions_requires_overlap_caveat_in_cypher():
    store = _FakeStore(rows=[{"a": "A", "rel": "OWNS", "b": "B"}])
    res = await q.contradictions(store)
    assert "affirmed" in res.cypher and "negated" in res.cypher
    # temporal-overlap guard present (don't flag a fact that changed over time)
    assert "valid_from" in res.cypher and "valid_to" in res.cypher


@pytest.mark.asyncio
async def test_orphans_uses_min_degree(monkeypatch):
    store = _FakeStore(rows=[{"name": "Lonely", "degree": 0}])
    res = await q.orphans(store, min_degree=1)
    assert res.params["min_degree"] == 1


@pytest.mark.asyncio
async def test_failsoft():
    assert (await q.contradictions(None)).rows == []
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement** (part 1 of `quality.py`)

```python
# src/analytics/primitives/quality.py
"""P1 — knowledge-quality flags (online, read-only)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows
from src.config import settings


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ContradictionsParams(_Params):
    top_n: int = 50


async def contradictions(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Flag affirmed+negated of the SAME (a,type,b) only when their validity windows
    # overlap (contemporaneous). A null window is treated as open/overlapping.
    cypher = (
        "MATCH (a:__Entity__)-[r1]->(b:__Entity__), (a)-[r2]->(b) "
        "WHERE type(r1)=type(r2) AND r1.polarity='affirmed' AND r2.polarity='negated' "
        "AND id(r1)<id(r2) "
        "AND (r1.valid_from IS NULL OR r2.valid_to IS NULL OR r1.valid_from <= r2.valid_to) "
        "AND (r2.valid_from IS NULL OR r1.valid_to IS NULL OR r2.valid_from <= r1.valid_to) "
        "RETURN a.name AS a, type(r1) AS rel, b.name AS b, "
        "r1.source_chunks AS affirmed_chunks, r2.source_chunks AS negated_chunks "
        "LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class OrphansParams(_Params):
    min_degree: int | None = None
    top_n: int = 50


async def orphans(store: Any | None, *, min_degree: int | None = None, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    floor = settings.signals.orphan_min_degree if min_degree is None else int(min_degree)
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "OPTIONAL MATCH (e)-[r]-(:__Entity__) "
        "WITH e, count(r) AS degree WHERE degree < $min_degree "
        "RETURN e.name AS name, degree, [l IN labels(e) WHERE l<>'__Entity__'][0] AS type "
        "ORDER BY degree ASC LIMIT $top_n"
    )
    params = {"min_degree": floor, "top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("contradictions", contradictions, ContradictionsParams,
                   "Facts asserted AND denied with overlapping validity (true contradictions, not changes)."))
register(Primitive("orphans", orphans, OrphansParams,
                   "Under-connected entities (degree below a floor) — noise or under-documented."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/quality.py && uv run ruff format src/analytics/primitives/quality.py tests/test_analytics/test_quality.py
git add src/analytics/primitives/quality.py tests/test_analytics/test_quality.py
git commit -m "feat(signals): P1 quality primitives — contradictions (temporal-safe) + orphans"
```

---

### Task 23: Quality primitives — `incomplete_entities`, `merge_candidates`

**Files:**
- Modify: `src/analytics/primitives/quality.py` (part 2)
- Test: extend `tests/test_analytics/test_quality.py`

**Interfaces:** `incomplete_entities(type, top_n)` (completeness vs `settings.signals.expected_attrs`); `merge_candidates(top_n)` (duplicate display-name groups).

- [ ] **Step 1: Write failing tests** (append)

```python
@pytest.mark.asyncio
async def test_incomplete_entities_uses_expected_attrs():
    store = _FakeStore(rows=[{"name": "Орг1", "missing": ["INN"], "have": ["OGRN"]}])
    res = await q.incomplete_entities(store, type="Organization")
    assert res.params["type"] == "Organization"
    assert "INN" in res.params["expected"]  # from settings.signals.expected_attrs


@pytest.mark.asyncio
async def test_merge_candidates_groups_duplicate_names():
    store = _FakeStore(rows=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}])
    res = await q.merge_candidates(store)
    assert "toLower" in res.cypher and "count(" in res.cypher.lower()
```

- [ ] **Step 2: Run to verify failure** → FAIL.

- [ ] **Step 3: Implement** (append to `quality.py`)

```python
class IncompleteEntitiesParams(_Params):
    type: str = "Organization"
    top_n: int = 50


async def incomplete_entities(store: Any | None, *, type: str = "Organization", top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    expected = settings.signals.expected_attrs.get(type, [])
    cypher = (
        "MATCH (e:__Entity__) WHERE $type IN labels(e) "
        "OPTIONAL MATCH (e)-[]-(id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $expected) "
        "WITH e, collect(DISTINCT [l IN labels(id) WHERE l IN $expected][0]) AS have "
        "RETURN e.name AS name, [x IN $expected WHERE NOT x IN have] AS missing, have "
        "ORDER BY size([x IN $expected WHERE NOT x IN have]) DESC LIMIT $top_n"
    )
    params = {"type": type, "expected": expected, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class MergeCandidatesParams(_Params):
    top_n: int = 50


async def merge_candidates(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    # Duplicate display-name groups (case/space-insensitive). ER-similarity upgrade
    # deferred to Wave 1 (P2). Identifier-keys are excluded.
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "WITH toLower(trim(e.name)) AS key, collect(e.name) AS names "
        "WITH key, names, size(names) AS count WHERE count > 1 "
        "RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("incomplete_entities", incomplete_entities, IncompleteEntitiesParams,
                   "Entities missing expected identifier attributes for their type (completeness)."))
register(Primitive("merge_candidates", merge_candidates, MergeCandidatesParams,
                   "Duplicate display-name groups — a ranked recommended-merge queue."))
```

- [ ] **Step 4: Run tests** → PASS. **Step 5: Lint + commit**

```bash
uv run ruff check src/analytics/primitives/quality.py && uv run ruff format src/analytics/primitives/quality.py tests/test_analytics/test_quality.py
git add src/analytics/primitives/quality.py tests/test_analytics/test_quality.py
git commit -m "feat(signals): P1 quality primitives — incomplete_entities + merge_candidates"
```

---

## Phase I — Integration

### Task 24: Catalog completeness + full-suite gate

**Files:**
- Modify: `src/analytics/primitives/__init__.py` (ensure all six modules imported)
- Test: `tests/test_analytics/test_catalog_complete.py`

**Interfaces:** asserts the full Wave-0 catalog is registered and the planner prompt renders all of it; final lint+format+test gate over the whole repo.

- [ ] **Step 1: Write the test**

```python
# tests/test_analytics/test_catalog_complete.py
import src.analytics.primitives  # noqa: F401 — triggers all registrations
from src.analytics.catalog import CATALOG, render_catalog_for_planner

_EXPECTED = {
    # Family 1
    "count_entities", "count_relationships", "distribution_by_type",
    "distribution_by_relation_type", "distribution_by_polarity",
    "top_entities_by_mentions", "top_entities_by_degree",
    # Family 2
    "entity_dossier", "neighbors_by_relation", "cooccurrence", "common_connections",
    "connection_path", "shared_identifier_entities", "identifier_lookup",
    # Family 3 (online subset)
    "community_overview", "entity_communities", "personalized_pagerank",
    # Family 4
    "relationship_timeline", "whats_changed", "topic_trend", "polarity_evolution", "entity_activity",
    # E1
    "new_events", "entity_new_connections",
    # P1
    "contradictions", "orphans", "incomplete_entities", "merge_candidates",
}


def test_wave0_catalog_is_complete():
    assert _EXPECTED <= set(CATALOG)


def test_planner_prompt_lists_every_primitive():
    rendered = render_catalog_for_planner()
    for name in _EXPECTED:
        assert name in rendered
```

- [ ] **Step 2: Run to verify** (should pass once all modules imported) — fix `primitives/__init__.py` if any import is missing.

Run: `uv run pytest tests/test_analytics/test_catalog_complete.py -q`

- [ ] **Step 3: Full gate**

```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pytest -q
```
Expected: all green. Fix any regressions before committing.

- [ ] **Step 4: Commit**

```bash
git add src/analytics/primitives/__init__.py tests/test_analytics/test_catalog_complete.py
git commit -m "test(analytics): Wave 0 catalog completeness + full-suite gate"
```

---

## Self-Review

**1. Spec coverage**

| Spec item | Task(s) |
|---|---|
| analytical §2–§3 architecture (package, workflow, planner) | 4, 9, 12, 13, 14 |
| analytical §4 Family 1 | 5 |
| analytical §4 Family 2 (incl. entity_dossier ⭐, shared_identifier_entities ⭐) | 6 |
| analytical §4 Family 3 online subset (communities, personalized_pagerank) | 8 |
| analytical §4 Family 4 temporal | 7 |
| analytical §6 schema card (text-to-Cypher) | **deferred** — fallback OFF (v1c/Wave 3); noted in Global Constraints |
| analytical §7 provenance contract | 2, 10 |
| analytical §8 surfaces (HTTP/MCP/CLI) | 15, 16, 17 |
| analytical §9 config/DI/worker/LLM | 3, 12, 14 |
| analytical §10 testing + §11 v1a phasing | every task (TDD) + 18 |
| analytical §11 v1b (materialization) | **out of scope** (Wave 1) — noted |
| event §1 first_seen migration/index | 19 |
| event §1/§3 M1 ON CREATE stamping | 20 |
| event §6 new_events / entity_new_connections | 21 |
| event §9 E1 phasing (no LLM change) | 19–21 |
| event E2/E3 | **out of scope** (Waves 2–3) |
| signals §3 contradictions (temporal-safe), orphans, incomplete, merge_candidates | 22, 23 |
| signals §9 P1 phasing (no LLM, no materialization) | 22–23 |
| signals P2–P4 | **out of scope** (Waves 1/2/4) |

**2. Placeholder scan:** Tasks contain full code. Two acknowledged "verify-at-impl" hooks (not placeholders, real unknowns the implementer must confirm against live code): (a) `Relation.source_id/target_id` ↔ entity-name equality + `doc_id` availability in `build_property_graph` (Task 20); (b) `CALL { } IN TRANSACTIONS` support via `structured_query` (Task 19). Both have stated fallbacks. The dead `_extract_json`/`_Empty`/`_augment` helpers in Task 9 are explicitly flagged for deletion.

**3. Type consistency:** `PrimitiveResult` (catalog.py, Task 4) is produced by every primitive and consumed by `step_from_primitive` (Task 10) → `StepResult` (contracts, Task 2) → `Provenance` → `AnalyticsOutcome`. `AnalysisPlan`/`PrimitiveCall` flow planner (Task 9) → workflow (Task 13) → executor (Task 12). `AnalyzeParams` is the single workflow input (Tasks 13/15/16/17). Catalog `param_model` validation (Task 9) uses the same pydantic models each primitive declares (Tasks 5–8, 21–23). Names checked consistent across tasks.

**Known scope boundaries (by design):** Wave 0 ships catalog-only (Cypher fallback OFF), online-only (no GDS materialization), and no Temporal Schedule — those are Waves 1/3. `E1` is first_seen only (no LLM event extraction); `P1` is quality flags only (no risk_score/queues, which need Wave 1 materialization).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-28-analytical-layer-wave0.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration (REQUIRED SUB-SKILL: `superpowers:subagent-driven-development`).
2. **Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batched with checkpoints.

Which approach?

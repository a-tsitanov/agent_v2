# Analytical Layer — Wave 1 Implementation Plan (v1b + P2 + Arc 1 rollups)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Wave 1 — the **heavy/offline tier** of the analytical layer: an offline `AnalyticsMaterializeWorkflow` that computes GDS centrality + link-prediction + composite `risk_score`/`completeness_score` and **writes them back into Neo4j** (`v1b`); the online read-primitives + composite decision signals/queues that sit on top (`P2`); and numeric rollups over `Amount`/`Metric` entities (`Arc 1`).

**Architecture:** Mirror `CommunityBuildWorkflow` — a durable workflow on the `kb-graph-build` queue that projects the `__Entity__` graph, runs heavy GDS algorithms inside the projection lifecycle, and persists results as node properties (`e.pagerank/betweenness/eigenvector/risk_score/completeness_score`) and `(:__Entity__)-[:LIKELY_LINK {score}]->(:__Entity__)` edges. Online catalog primitives (Wave 0 `src/analytics/`) then **read** those materialized values cheaply. Composite scoring is a **pure, testable function**; the offline activity gathers raw component values via Cypher and the pure scorer turns them into a banded `risk_score` with per-component provenance.

**Tech Stack:** Python 3.12, Temporal (`temporalio`, pydantic data converter), Neo4j + GDS 2.x via `Neo4jPropertyGraphStore`, pydantic v2 / pydantic-settings, pytest (`asyncio_mode=auto`), ruff. Builds directly on the Wave 0 `src/analytics/` package.

## Global Constraints

- **Two-tier discipline.** Heavy GDS runs **offline** in `AnalyticsMaterializeWorkflow` (queue `kb-graph-build`) and writes node properties / relationships. Online primitives only **read** materialized values — they never run GDS. (PageRank-personalized stays online; it already exists.)
- **Determinism / provenance unchanged from Wave 0.** Numbers come from Cypher rows (or materialized properties); the LLM only verbalizes. `risk_score` is a **heuristic, not ground truth** — its provenance lists which components fired and with what value; never present it as a verdict.
- **Fail-soft everywhere.** Every online primitive: `store is None` → empty; any exception → log WARN, return empty (Wave 0 `src/analytics/store_query.run_rows`). Every offline activity: never raise across the Temporal boundary — return a result with `persisted=False`/empty on error (mirror `detect_communities_activity`).
- **Idempotent materialization.** Re-running the workflow refreshes properties/edges in place (`MATCH … SET`, `MERGE … SET`), and `:LIKELY_LINK` is fully refreshed (delete-then-write). No duplication. Projection is created with a unique name and dropped in `finally` (mirror `communities.py`).
- **Blocking Neo4j/GDS off the event loop** via `asyncio.to_thread(_run_query, store, …)`.
- **Canonical date form = epoch-days (int)** (`src/retrieval/date_filters`); `created_at`/`first_seen` from Wave 0 E1 is epoch-days. Relationship `valid_from`/`valid_to` are ISO strings. Polarity ∈ `{affirmed,negated,uncertain}`; negated-exclusion filters use the Wave 0 convention `(r.polarity IS NULL OR r.polarity <> 'negated')`.
- **Entity label literal `"__Entity__"`** (`src/analytics/ids.ENTITY_LABEL`); identifier types = `src/analytics/ids.ID_TYPES`; mention-count property is `mention_count`.
- **Catalog conventions (Wave 0, reuse exactly):** each primitive is `async def fn(store, *, ...) -> PrimitiveResult` registered via `catalog.register(Primitive(name, fn, param_model, description, tier))`; param models subclass a base with `ConfigDict(extra="ignore")`; `clamp_top_n` on `top_n`; results carry the executed `cypher` for provenance. 🟠 materialized-read primitives use `tier="offline-mat"`.
- **Contracts are frozen pydantic** mirroring `src/workflow/contracts.py` (`_Frozen`/`ConfigDict(frozen=True)`, `Field(default_factory=...)`).
- **Quality gates** (before every commit): `uv run ruff check <changed files>` · `uv run ruff format <changed files>` · the task's pytest. ruff: line-length 100, target py312, ruleset `E,F,I,B,UP,SIM,RUF` (note: `BLE001` is NOT enabled → never add `# noqa: BLE001`; an `# noqa: F401` on an import-for-side-effect IS legitimate). Cyrillic allowed.
- **Test doubles** are the Wave 0 hand-rolled `_FakeStore` (captures `cypher`/`param_map`, returns canned rows; supports `by_call=[...]`) and `_StubLLM` from `tests/test_analytics/conftest.py`. GDS/centrality cannot run in unit tests → assert the generated GDS/write-back Cypher shape against `_FakeStore`; the workflow uses the Temporal time-skipping env with stubbed activities (mirror `tests/test_workflow/test_analytics_workflow.py`).
- **Git:** commit locally on `worktree-anal`; **never push, never commit to `main`**. (Controller commits at phase checkpoints per the run's policy.)

---

## Codebase-grounded facts (verified — build on these)

1. **Mirror target** = `CommunityBuildWorkflow` (`src/workflow/search/community_wf.py:92-176`): `@workflow.defn`, `__init__` with `self._state` dict, `@workflow.query get_state`, `@workflow.run(params)->result`, phases via `workflow.execute_activity(name, params, result_type=…, start_to_close_timeout=…, schedule_to_close_timeout=…, retry_policy=…, heartbeat_timeout=…)`.
2. **Activities** live in `src/workflow/search/activities/community.py` (`detect_communities_activity`, `summarize_community_activity`): `@activity.defn async def name(params)->result`; get store via the module's `_get_store()`; blocking work via `asyncio.to_thread(...)`; fail-safe (return empty/`persisted=False`, never raise).
3. **Worker:** `src/workflow/worker.py:219-226` `graph_build` group → `workflows=[CommunityBuildWorkflow]`, `activities=GRAPH_BUILD_ACTIVITIES`. `GRAPH_BUILD_ACTIVITIES` is defined in `src/workflow/search/activities/__init__.py:42-45`.
4. **Projection lifecycle** (`src/graph/communities.py`): `_new_graph_name()` (uuid-suffixed), `_project_cypher(graph_name)` (projects `__Entity__` subgraph with `relationshipProperties:{weight:coalesce(r.weight,1.0)}`, `undirectedRelationshipTypes:['*']`), `_drop_cypher(graph_name)` (`gds.graph.drop(...,false)`). Pattern: drop-stale → project → run algos → **drop in `finally`**. `_run_query(store, cypher, params)` is the sync `store.structured_query(cypher, param_map=…)` wrapper used with `asyncio.to_thread`.
5. **Existing GDS calls** (`src/graph/analysis.py`): `gds.pageRank.stream('{g}', {relationshipWeightProperty:'weight'}) YIELD nodeId, score`; `gds.wcc.stats`. Style: f-string graph name, `gds.util.asNode(nodeId).name`. `personalized_pagerank(store, seeds, *, top_n=20)` exists and is wrapped online (Wave 0 `communities.py` primitive).
6. **Write-back idiom** (`communities.py:176-191`): `MERGE (c:Community {…}) SET … ; UNWIND $members AS m MATCH (e:__Entity__ {name:m}) MERGE (e)-[:IN_COMMUNITY]->(c)`. Idempotent MERGE-on-key + UNWIND/MATCH/SET.
7. **Admin trigger** (`src/api/routes/search_v2.py:271-308`): fire-and-forget `client.start_workflow(WF.run, params, id=…, task_queue=settings.temporal.graph_build_task_queue, id_reuse_policy=ALLOW_DUPLICATE)` → `202 {workflow_id, status}`. Also `src/api/routes/graph_admin.py` hosts `/admin/graph/*` (pagerank/components/…) — the natural home for `/admin/graph/materialize`.
8. **Config** (`src/config.py`): `TemporalSettings` (`graph_build_task_queue="kb-graph-build"`, `graph_build_activity_concurrency=2`, `community_*`); `SignalsSettings` exists from Wave 0 (`orphan_min_degree`, `expected_attrs`) with env_prefix `SIGNALS_`.
9. **Retry/timeouts** (`src/workflow/search/_retry.py`): `FAST_RETRY` (3), `DETECT_RETRY` (2, non-retryable `MemoryError`), `LLM_START_TO_CLOSE=1h`, `LLM_SCHEDULE_TO_CLOSE=3h`. The detect activity uses `heartbeat_timeout` ~2m. Reuse these; for heavy GDS use the detect-style long timeouts + heartbeat.
10. **No Temporal Schedules yet** — trigger via admin endpoint only (the Schedule is out of scope for Wave 1, same as Wave 0).
11. ⚠️ **GDS is UNVERIFIED against a live install** (same caveat `analysis.py` carries) — `gds.betweenness.stream` / `gds.eigenvector.stream` / `gds.nodeSimilarity.stream` procedure names + YIELD shapes are GDS-2.x per docs; a live smoke test of the materialize workflow is a deploy-time validation step (call it out in the admin/runbook). Unit tests assert Cypher shape only.

---

## File Structure

**New:**
```
src/analytics/
├── risk.py                       # PURE composite scoring (normalize, weighted sum, banding, provenance)
├── materialize.py                # GDS compute + write-back Cypher builders + run helpers (off-loop)
└── primitives/
    ├── centrality.py             # v1b reads: top_central_entities, link_prediction (tier=offline-mat)
    ├── signals.py                # P2: risk_score read, investigate_next, review_queue, recommended_merges,
    │                             #     circular_ownership, shell_signal
    └── rollups.py                # Arc 1: numeric_rollup
src/workflow/analytics/
├── materialize_activities.py     # materialize_centrality / _link_prediction / _risk activities (+ MATERIALIZE_ACTIVITIES)
└── materialize_workflow.py       # AnalyticsMaterializeWorkflow
```

**Modified:**
```
src/config.py                                 # TemporalSettings materialize knobs; SignalsSettings risk weights/bands/expected
src/analytics/contracts.py                    # MaterializeParams/Result + activity in/out contracts
src/workflow/search/activities/__init__.py    # GRAPH_BUILD_ACTIVITIES += MATERIALIZE_ACTIVITIES
src/workflow/worker.py                         # graph_build group: register AnalyticsMaterializeWorkflow
src/api/routes/graph_admin.py                  # POST /admin/graph/materialize
src/analytics/primitives/__init__.py           # import centrality, signals, rollups (register)
```

**Tests:** `tests/test_analytics/{test_risk.py, test_materialize.py, test_centrality.py, test_signals.py, test_rollups.py, test_catalog_complete.py(extend)}`, `tests/test_workflow/test_materialize_workflow.py`, `tests/test_api/test_materialize_route.py`.

---

## Phase A — Config & contracts

### Task 1: Config — materialize knobs + risk weights/bands

**Files:** Modify `src/config.py`; Test `tests/test_analytics/test_config_wave1.py`

**Interfaces — Produces:** `settings.temporal.analytics_materialize_concurrency: int`; `settings.signals.risk_weights: dict[str,float]`, `.risk_bands: dict[str,float]`, `.link_prediction_top_k: int`, `.link_prediction_min_score: float`.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_config_wave1.py
from src.config import settings


def test_materialize_and_risk_settings_defaults():
    assert settings.temporal.analytics_materialize_concurrency >= 1
    w = settings.signals.risk_weights
    assert set(w) == {"affiliation", "brokerage", "controversy", "volatility", "opacity"}
    assert abs(sum(w.values()) - 1.0) < 1e-9          # weights normalized
    assert settings.signals.risk_bands["high"] >= settings.signals.risk_bands["medium"]
    assert settings.signals.link_prediction_top_k >= 1
```

- [ ] **Step 2: run → FAIL** (`uv run pytest tests/test_analytics/test_config_wave1.py -q`).

- [ ] **Step 3: implement** — add to `TemporalSettings` (near the `graph_build_*` fields):

```python
    analytics_materialize_concurrency: int = Field(
        default=2, ge=1,
        description="GDS-воркеры для офлайн-материализации аналитики (centrality/link-prediction)",
    )
```

Add to `SignalsSettings` (Wave 0 class):

```python
    risk_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "affiliation": 0.30, "brokerage": 0.20, "controversy": 0.20,
            "volatility": 0.15, "opacity": 0.15,
        },
        description="Веса компонентов composite risk_score (нормализованы к сумме 1.0)",
    )
    risk_bands: dict[str, float] = Field(
        default_factory=lambda: {"high": 0.66, "medium": 0.33},
        description="Пороги полос risk_score: >=high → high, >=medium → medium, иначе low",
    )
    link_prediction_top_k: int = Field(
        default=10, ge=1,
        description="top-K соседей на узел для GDS node-similarity (link prediction)",
    )
    link_prediction_min_score: float = Field(
        default=0.5, ge=0.0, le=1.0,
        description="Минимальный similarity для записи ребра :LIKELY_LINK",
    )
```

- [ ] **Step 4: run → PASS.** **Step 5: lint + commit-skip** (controller commits).

```bash
uv run ruff check src/config.py tests/test_analytics/test_config_wave1.py && uv run ruff format src/config.py tests/test_analytics/test_config_wave1.py
```

> Implementer note: also add the new `SIGNALS_*`/`TEMPORAL_*` vars to `scripts/make_env.py`'s `_ENV_DESCRIPTIONS` (Russian), matching how Wave 0 Task 3 satisfied `test_every_env_var_has_russian_description`. Run `uv run pytest tests/test_scripts/test_make_env.py::test_every_env_var_has_russian_description -q` and confirm the 5 new vars are NOT in the missing-description list (the test may still be red on pre-existing vars — that's out of scope).

---

### Task 2: Contracts — materialize workflow/activity wire types

**Files:** Modify `src/analytics/contracts.py`; Test `tests/test_analytics/test_contracts_wave1.py`

**Interfaces — Produces (frozen):**
- `MaterializeParams(metrics: list[str]=["pagerank","betweenness","eigenvector"], link_prediction: bool=True, risk: bool=True)`
- `MaterializeResult(centrality_written: int=0, links_written: int=0, risk_written: int=0, errors: list[str]=[])`
- `CentralityIn(metrics: list[str])` · `LinkPredictionIn()` (no fields) · `RiskIn()` (no fields)
- `StageResult(written: int=0, error: str="")`

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_contracts_wave1.py
import pytest
from pydantic import ValidationError
from src.analytics.contracts import MaterializeParams, MaterializeResult, StageResult


def test_materialize_defaults_and_frozen():
    p = MaterializeParams()
    assert p.metrics == ["pagerank", "betweenness", "eigenvector"]
    assert p.link_prediction is True and p.risk is True
    with pytest.raises(ValidationError):
        p.risk = False


def test_stage_and_result_defaults():
    assert StageResult().written == 0 and StageResult().error == ""
    r = MaterializeResult(centrality_written=5)
    assert r.links_written == 0 and r.errors == []
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — append to `src/analytics/contracts.py` (reuse the existing `_Frozen` base):

```python
class MaterializeParams(_Frozen):
    metrics: list[str] = Field(default_factory=lambda: ["pagerank", "betweenness", "eigenvector"])
    link_prediction: bool = True
    risk: bool = True


class CentralityIn(_Frozen):
    metrics: list[str] = Field(default_factory=lambda: ["pagerank", "betweenness", "eigenvector"])


class LinkPredictionIn(_Frozen):
    pass


class RiskIn(_Frozen):
    pass


class StageResult(_Frozen):
    written: int = 0
    error: str = ""


class MaterializeResult(_Frozen):
    centrality_written: int = 0
    links_written: int = 0
    risk_written: int = 0
    errors: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase B — Pure composite scoring

### Task 3: `risk.py` — pure risk scorer

**Files:** Create `src/analytics/risk.py`; Test `tests/test_analytics/test_risk.py`

**Interfaces — Produces:**
- `normalize(value: float, lo: float, hi: float) -> float` — clamp to `[0,1]` over `[lo,hi]` (hi<=lo → 0.0).
- `RiskResult` dataclass: `score: float, band: str, fired: dict[str, float]`.
- `compute_risk(components: dict[str, float], *, weights: dict[str, float], bands: dict[str, float]) -> RiskResult` — weighted sum of already-normalized (0..1) components; band by thresholds; `fired` = components with value > 0.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_risk.py
from src.analytics.risk import compute_risk, normalize, RiskResult

_W = {"affiliation": 0.3, "brokerage": 0.2, "controversy": 0.2, "volatility": 0.15, "opacity": 0.15}
_B = {"high": 0.66, "medium": 0.33}


def test_normalize():
    assert normalize(5, 0, 10) == 0.5
    assert normalize(-1, 0, 10) == 0.0 and normalize(99, 0, 10) == 1.0
    assert normalize(5, 10, 10) == 0.0          # degenerate range → 0


def test_compute_risk_weighted_and_banded():
    r = compute_risk({"affiliation": 1.0, "brokerage": 1.0}, weights=_W, bands=_B)
    assert isinstance(r, RiskResult)
    assert abs(r.score - 0.5) < 1e-9            # 0.3*1 + 0.2*1 = 0.5
    assert r.band == "medium"                   # 0.5 >= 0.33, < 0.66
    assert set(r.fired) == {"affiliation", "brokerage"}


def test_bands_low_and_high():
    assert compute_risk({}, weights=_W, bands=_B).band == "low"
    assert compute_risk({k: 1.0 for k in _W}, weights=_W, bands=_B).band == "high"  # sum=1.0


def test_unknown_component_ignored():
    r = compute_risk({"affiliation": 1.0, "bogus": 1.0}, weights=_W, bands=_B)
    assert abs(r.score - 0.3) < 1e-9            # bogus has no weight → ignored
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/analytics/risk.py
"""Pure composite risk scoring (no I/O). Components arrive already normalized to 0..1."""

from __future__ import annotations

from dataclasses import dataclass, field


def normalize(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (float(value) - lo) / (hi - lo)))


@dataclass(frozen=True)
class RiskResult:
    score: float
    band: str
    fired: dict[str, float] = field(default_factory=dict)


def compute_risk(components: dict[str, float], *, weights: dict[str, float],
                 bands: dict[str, float]) -> RiskResult:
    score = 0.0
    fired: dict[str, float] = {}
    for name, w in weights.items():
        v = float(components.get(name, 0.0) or 0.0)
        score += w * v
        if v > 0:
            fired[name] = v
    score = max(0.0, min(1.0, score))
    if score >= bands.get("high", 0.66):
        band = "high"
    elif score >= bands.get("medium", 0.33):
        band = "medium"
    else:
        band = "low"
    return RiskResult(score=round(score, 6), band=band, fired=fired)
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase C — Offline materialization (v1b core)

### Task 4: `materialize.py` — GDS compute + write-back helpers

**Files:** Create `src/analytics/materialize.py`; Test `tests/test_analytics/test_materialize.py`

**Interfaces — Produces (each `async`, off-loop, fail-soft returning rows/counts):**
- `_CENTRALITY_STREAM: dict[str,str]` — metric → GDS stream Cypher template (`{g}` placeholder), for `pagerank`/`betweenness`/`eigenvector`.
- `write_centrality(store, graph_name, metric) -> int` — run the metric's GDS stream, `UNWIND` rows → `SET e.<metric>` (metric from the fixed allowlist, inlined safely), return rows written.
- `write_link_prediction(store, graph_name, *, top_k, min_score) -> int` — delete all `:LIKELY_LINK`, run `gds.nodeSimilarity.stream(topK)`, write `MERGE (a)-[:LIKELY_LINK {score}]->(b)` for pairs `>= min_score`, return count.
- `project, drop` re-exported/wrapped from `communities.py` helpers (or `run_query`).

Uses `src/graph/communities.py::{_new_graph_name,_project_cypher,_drop_cypher}` and a local `_run_query`.

- [ ] **Step 1: failing test** (assert GDS + write-back Cypher shapes via `_FakeStore`)

```python
# tests/test_analytics/test_materialize.py
import pytest
from src.analytics import materialize as m
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_write_centrality_pagerank_shape():
    # call 1 = GDS stream rows; the write UNWIND returns nothing
    store = _FakeStore(by_call=[[{"name": "A", "score": 0.9}, {"name": "B", "score": 0.1}], []])
    n = await m.write_centrality(store, "g1", "pagerank")
    assert n == 2
    cyphers = " ".join(c for c, _ in store.calls)
    assert "gds.pageRank.stream" in cyphers
    assert "SET e.pagerank" in cyphers           # metric inlined into write-back


@pytest.mark.asyncio
async def test_write_centrality_rejects_unknown_metric():
    store = _FakeStore(rows=[])
    with pytest.raises(ValueError):
        await m.write_centrality(store, "g1", "bogus; DROP")   # allowlist guard


@pytest.mark.asyncio
async def test_write_link_prediction_filters_and_writes():
    store = _FakeStore(by_call=[
        [],                                                            # delete stale
        [{"a": "A", "b": "B", "score": 0.9}, {"a": "A", "b": "C", "score": 0.2}],  # nodeSimilarity
        [],                                                            # write
    ])
    n = await m.write_link_prediction(store, "g1", top_k=10, min_score=0.5)
    assert n == 1                                  # only the 0.9 pair kept
    joined = " ".join(c for c, _ in store.calls)
    assert "gds.nodeSimilarity.stream" in joined and ":LIKELY_LINK" in joined


@pytest.mark.asyncio
async def test_failsoft_none_store():
    assert await m.write_centrality(None, "g", "pagerank") == 0
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/analytics/materialize.py
"""Offline GDS compute + write-back into Neo4j. Mirrors src/graph/communities.py."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.graph.communities import _drop_cypher, _new_graph_name, _project_cypher  # noqa: F401

# metric -> GDS stream cypher (graph name f-substituted; weighted where applicable)
_CENTRALITY_STREAM: dict[str, str] = {
    "pagerank": (
        "CALL gds.pageRank.stream('{g}', {{relationshipWeightProperty:'weight'}}) "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
    "betweenness": (
        "CALL gds.betweenness.stream('{g}') "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
    "eigenvector": (
        "CALL gds.eigenvector.stream('{g}', {{relationshipWeightProperty:'weight'}}) "
        "YIELD nodeId, score RETURN gds.util.asNode(nodeId).name AS name, score"
    ),
}


def _run_query(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    return list(store.structured_query(cypher, param_map=params or {}))


async def _run(store: Any, cypher: str, params: dict | None = None) -> list[dict]:
    return await asyncio.to_thread(_run_query, store, cypher, params)


async def write_centrality(store: Any | None, graph_name: str, metric: str) -> int:
    if store is None:
        return 0
    if metric not in _CENTRALITY_STREAM:
        raise ValueError(f"unknown centrality metric: {metric!r}")
    try:
        rows = await _run(store, _CENTRALITY_STREAM[metric].format(g=graph_name))
        if not rows:
            return 0
        # metric is from the fixed allowlist above → safe to inline as a property key
        write = (
            "UNWIND $rows AS r MATCH (e:__Entity__ {name: r.name}) "
            f"SET e.{metric} = r.score"
        )
        await _run(store, write, {"rows": rows})
        return len(rows)
    except Exception as exc:  # noqa: BLE001 is NOT enabled — plain broad except, fail-soft
        logger.warning("write_centrality {m} failed: {e}", m=metric, e=exc)
        return 0


async def write_link_prediction(store: Any | None, graph_name: str, *, top_k: int,
                                min_score: float) -> int:
    if store is None:
        return 0
    try:
        await _run(store, "MATCH ()-[l:LIKELY_LINK]->() DELETE l")  # full refresh
        rows = await _run(
            store,
            "CALL gds.nodeSimilarity.stream('{g}', {{topK: $k}}) "
            "YIELD node1, node2, similarity "
            "RETURN gds.util.asNode(node1).name AS a, gds.util.asNode(node2).name AS b, "
            "similarity AS score".format(g=graph_name),
            {"k": int(top_k)},
        )
        pairs = [r for r in rows if float(r.get("score", 0.0)) >= min_score]
        if pairs:
            await _run(
                store,
                "UNWIND $pairs AS p MATCH (a:__Entity__ {name:p.a}), (b:__Entity__ {name:p.b}) "
                "MERGE (a)-[l:LIKELY_LINK]->(b) SET l.score = p.score",
                {"pairs": pairs},
            )
        return len(pairs)
    except Exception as exc:
        logger.warning("write_link_prediction failed: {e}", e=exc)
        return 0
```

> Implementer note: the comment about BLE001 is informational — do NOT add a `# noqa: BLE001`; ruff would flag it as unused (RUF100). Keep the bare `except Exception as exc:`. Confirm `_new_graph_name/_project_cypher/_drop_cypher` are importable from `src/graph/communities.py` (they are, per the mapping); if any is private-with-underscore and a linter objects, import is still fine (same package style as Wave 0's reuse of `analysis.personalized_pagerank`).

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 5: Materialize activities — centrality + link prediction

**Files:** Create `src/workflow/analytics/materialize_activities.py`; Test `tests/test_workflow/test_materialize_activities.py`

**Interfaces — Produces:**
- `@activity.defn materialize_centrality(p: CentralityIn) -> StageResult` — open one projection, run `write_centrality` for each metric, drop projection in `finally`; `written` = total rows; on error `StageResult(error=...)`.
- `@activity.defn materialize_link_prediction(p: LinkPredictionIn) -> StageResult`.
- module list `MATERIALIZE_ACTIVITIES = [materialize_centrality, materialize_link_prediction, materialize_risk]` (risk added in Task 6).
- helper `_get_store()` → `build_neo4j_graph_store()`.

- [ ] **Step 1: failing test**

```python
# tests/test_workflow/test_materialize_activities.py
import pytest
from src.analytics.contracts import CentralityIn
from src.workflow.analytics import materialize_activities as ma


class _Store:
    def __init__(self):
        self.calls = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append(cypher)
        # GDS stream → 1 row; everything else → []
        return [{"name": "A", "score": 0.5}] if "gds." in cypher and "stream" in cypher else []


@pytest.mark.asyncio
async def test_materialize_centrality_runs_each_metric(monkeypatch):
    monkeypatch.setattr(ma, "_get_store", lambda: _Store())
    res = await ma.materialize_centrality(CentralityIn(metrics=["pagerank", "betweenness"]))
    assert res.written == 2 and res.error == ""   # 1 row each


@pytest.mark.asyncio
async def test_materialize_centrality_failsoft(monkeypatch):
    def _boom():
        raise RuntimeError("neo4j down")
    monkeypatch.setattr(ma, "_get_store", _boom)
    res = await ma.materialize_centrality(CentralityIn())
    assert res.written == 0 and "neo4j down" in res.error
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/workflow/analytics/materialize_activities.py
"""Temporal activities for the offline analytics materialization (kb-graph-build)."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger
from temporalio import activity

from src.analytics import materialize as mz
from src.analytics.contracts import CentralityIn, LinkPredictionIn, RiskIn, StageResult
from src.config import settings
from src.graph.store import build_neo4j_graph_store


def _get_store() -> Any:
    return build_neo4j_graph_store()


async def _with_projection(store: Any, fn) -> int:
    graph = mz._new_graph_name()
    await mz._run(store, mz._drop_cypher(graph))          # clear stale
    await mz._run(store, mz._project_cypher(graph))
    try:
        return await fn(graph)
    finally:
        await mz._run(store, mz._drop_cypher(graph))


@activity.defn
async def materialize_centrality(p: CentralityIn) -> StageResult:
    try:
        store = _get_store()

        async def _do(graph: str) -> int:
            total = 0
            for metric in p.metrics:
                activity.heartbeat({"metric": metric})
                total += await mz.write_centrality(store, graph, metric)
            return total

        written = await _with_projection(store, _do)
        return StageResult(written=written)
    except Exception as exc:  # noqa
        logger.warning("materialize_centrality failed: {e}", e=exc)
        return StageResult(error=str(exc))


@activity.defn
async def materialize_link_prediction(p: LinkPredictionIn) -> StageResult:
    try:
        store = _get_store()
        s = settings.signals

        async def _do(graph: str) -> int:
            return await mz.write_link_prediction(
                store, graph, top_k=s.link_prediction_top_k, min_score=s.link_prediction_min_score,
            )

        return StageResult(written=await _with_projection(store, _do))
    except Exception as exc:  # noqa
        logger.warning("materialize_link_prediction failed: {e}", e=exc)
        return StageResult(error=str(exc))


# materialize_risk is added in Task 6; MATERIALIZE_ACTIVITIES is finalized there.
```

> Implementer note: `_with_projection` here mirrors `communities.py`'s lifecycle. If `src/graph/communities.py` already exposes a reusable `_with_projection(store, fn)` (the Wave 0 mapping suggested one in `analysis.py`/`communities.py`), prefer importing and reusing it instead of redefining — confirm and pick one; do not maintain two copies.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 6: Materialize activity — risk + completeness scores

**Files:** Modify `src/workflow/analytics/materialize_activities.py` (add `materialize_risk` + `MATERIALIZE_ACTIVITIES`); Test extend `tests/test_workflow/test_materialize_activities.py`

**Interfaces — Produces:** `@activity.defn materialize_risk(p: RiskIn) -> StageResult` — gathers per-entity component raw values via one Cypher read, computes `risk_score`/`band`/`fired` via `risk.compute_risk` and `completeness_score`, writes `SET e.risk_score, e.risk_band, e.risk_components, e.completeness_score`. `MATERIALIZE_ACTIVITIES = [materialize_centrality, materialize_link_prediction, materialize_risk]`.

Component gather Cypher (one pass over Organizations/Persons; betweenness read from the property written in Task 5):
```cypher
MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types)
OPTIONAL MATCH (e)-[ri]-(idn:__Entity__) WHERE any(l IN labels(idn) WHERE l IN $id_types)
WITH e, count(DISTINCT idn) AS id_links
OPTIONAL MATCH (e)-[r]-(:__Entity__)
WITH e, id_links, count(r) AS deg,
     sum(CASE WHEN r.polarity IN ['negated','uncertain'] THEN 1 ELSE 0 END) AS contested,
     sum(CASE WHEN r.created_at >= $since THEN 1 ELSE 0 END) AS recent
RETURN e.name AS name, coalesce(e.betweenness,0.0) AS betweenness, id_links, deg, contested, recent
```
Then per row build normalized components and call `compute_risk`. `completeness_score` = filled/expected via `settings.signals.expected_attrs` (reuse the Wave 0 incomplete-entities logic, but produce a ratio).

- [ ] **Step 1: failing test**

```python
# append to tests/test_workflow/test_materialize_activities.py
import json
from src.analytics.contracts import RiskIn


class _RiskStore:
    def __init__(self, rows):
        self._rows, self.writes = rows, []

    def structured_query(self, cypher, param_map=None):
        if cypher.strip().startswith("UNWIND"):      # write-back
            self.writes.append((cypher, param_map))
            return []
        return self._rows                            # the component-gather read


@pytest.mark.asyncio
async def test_materialize_risk_writes_scores(monkeypatch):
    rows = [{"name": "Shell", "betweenness": 1.0, "id_links": 3, "deg": 3, "contested": 2, "recent": 4}]
    store = _RiskStore(rows)
    monkeypatch.setattr(ma, "_get_store", lambda: store)
    res = await ma.materialize_risk(RiskIn())
    assert res.written == 1 and res.error == ""
    write_cypher = store.writes[0][0]
    assert "SET e.risk_score" in write_cypher and "e.risk_band" in write_cypher
    rowarg = store.writes[0][1]["rows"][0]
    assert 0.0 <= rowarg["score"] <= 1.0 and rowarg["band"] in {"low", "medium", "high"}
    json.loads(rowarg["components"])                 # components serialized as JSON
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — append to `materialize_activities.py`:

```python
import json

from src.analytics.ids import ID_TYPES
from src.analytics.risk import compute_risk
from src.retrieval.date_filters import today_epoch_days

_RISK_GATHER = (
    "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
    "OPTIONAL MATCH (e)-[]-(idn:__Entity__) WHERE any(l IN labels(idn) WHERE l IN $id_types) "
    "WITH e, count(DISTINCT idn) AS id_links "
    "OPTIONAL MATCH (e)-[r]-(:__Entity__) "
    "WITH e, id_links, count(r) AS deg, "
    "sum(CASE WHEN r.polarity IN ['negated','uncertain'] THEN 1 ELSE 0 END) AS contested, "
    "sum(CASE WHEN r.created_at >= $since THEN 1 ELSE 0 END) AS recent "
    "RETURN e.name AS name, coalesce(e.betweenness,0.0) AS betweenness, "
    "id_links, deg, contested, recent"
)
_RISK_WRITE = (
    "UNWIND $rows AS r MATCH (e:__Entity__ {name:r.name}) "
    "SET e.risk_score=r.score, e.risk_band=r.band, e.risk_components=r.components"
)


def _risk_row(raw: dict, weights: dict, bands: dict, max_bet: float) -> dict:
    deg = max(int(raw.get("deg", 0)), 1)
    components = {
        "affiliation": 1.0 if int(raw.get("id_links", 0)) >= 2 else 0.0,
        "brokerage": (float(raw.get("betweenness", 0.0)) / max_bet) if max_bet > 0 else 0.0,
        "controversy": int(raw.get("contested", 0)) / deg,
        "volatility": min(int(raw.get("recent", 0)) / deg, 1.0),
        "opacity": 1.0 if int(raw.get("id_links", 0)) > 0 and int(raw.get("deg", 0)) <= int(raw.get("id_links", 0)) else 0.0,
    }
    r = compute_risk(components, weights=weights, bands=bands)
    return {"name": raw["name"], "score": r.score, "band": r.band,
            "components": json.dumps(r.fired, ensure_ascii=False)}


@activity.defn
async def materialize_risk(p: RiskIn) -> StageResult:
    try:
        store = _get_store()
        s = settings.signals
        since = today_epoch_days() - s.new_window_days if hasattr(s, "new_window_days") else 0
        raws = await mz._run(store, _RISK_GATHER, {"id_types": ID_TYPES, "since": since})
        if not raws:
            return StageResult(written=0)
        max_bet = max((float(x.get("betweenness", 0.0)) for x in raws), default=0.0)
        rows = [_risk_row(x, s.risk_weights, s.risk_bands, max_bet) for x in raws]
        await mz._run(store, _RISK_WRITE, {"rows": rows})
        return StageResult(written=len(rows))
    except Exception as exc:  # noqa
        logger.warning("materialize_risk failed: {e}", e=exc)
        return StageResult(error=str(exc))


MATERIALIZE_ACTIVITIES = [materialize_centrality, materialize_link_prediction, materialize_risk]
```

> Implementer note: `settings.events.new_window_days` is the Wave 0 events window; `s` here is `settings.signals` which does NOT have `new_window_days`. Use `settings.events.new_window_days` for `since` instead of the `hasattr` guard shown — fix to `since = today_epoch_days() - settings.events.new_window_days`. `completeness_score` materialization is folded in as a follow-up if needed; the gather above covers the risk components. If you add completeness, extend `_RISK_GATHER`/`_RISK_WRITE` with the expected-attrs ratio (reuse Wave 0 `incomplete_entities` logic) and `SET e.completeness_score`.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 7: `AnalyticsMaterializeWorkflow`

**Files:** Create `src/workflow/analytics/materialize_workflow.py`; Test `tests/test_workflow/test_materialize_workflow.py`

**Interfaces — Produces:** `@workflow.defn class AnalyticsMaterializeWorkflow` with `@workflow.run async def run(self, params: MaterializeParams) -> MaterializeResult` (+ `@workflow.query get_state`). Sequence: centrality → link_prediction (if `params.link_prediction`) → risk (if `params.risk`); aggregate `StageResult`s into `MaterializeResult`; collect any `error` strings.

- [ ] **Step 1: failing test** (Temporal time-skipping env + stubbed activities by name — mirror `tests/test_workflow/test_analytics_workflow.py`)

```python
# tests/test_workflow/test_materialize_workflow.py
import uuid
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, UnsandboxedWorkflowRunner
from temporalio.contrib.pydantic import pydantic_data_converter
from src.analytics.contracts import MaterializeParams, StageResult
from src.workflow.analytics.materialize_workflow import AnalyticsMaterializeWorkflow


@activity.defn(name="materialize_centrality")
async def _c(p) -> StageResult: return StageResult(written=10)
@activity.defn(name="materialize_link_prediction")
async def _l(p) -> StageResult: return StageResult(written=3)
@activity.defn(name="materialize_risk")
async def _r(p) -> StageResult: return StageResult(written=7)


@pytest.mark.asyncio
async def test_materialize_workflow_aggregates():
    try:
        env = await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter)
    except Exception as exc:
        pytest.skip(f"temporal test server unavailable: {exc}")
    async with env:
        async with Worker(env.client, task_queue="t-mat", workflows=[AnalyticsMaterializeWorkflow],
                          activities=[_c, _l, _r], workflow_runner=UnsandboxedWorkflowRunner()):
            out = await env.client.execute_workflow(
                AnalyticsMaterializeWorkflow.run, MaterializeParams(),
                id=f"mat-{uuid.uuid4().hex}", task_queue="t-mat")
    assert out.centrality_written == 10 and out.links_written == 3 and out.risk_written == 7
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** (copy the import-passthrough + retry constants idiom from `src/workflow/analytics/workflow.py` / `community_wf.py`)

```python
# src/workflow/analytics/materialize_workflow.py
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.analytics.contracts import (
        CentralityIn, LinkPredictionIn, MaterializeParams, MaterializeResult, RiskIn, StageResult,
    )

_RETRY = RetryPolicy(maximum_attempts=2, initial_interval=timedelta(seconds=2),
                     maximum_interval=timedelta(seconds=30))
_START = timedelta(hours=1)
_S2C = timedelta(hours=3)
_HB = timedelta(minutes=2)


@workflow.defn
class AnalyticsMaterializeWorkflow:
    def __init__(self) -> None:
        self._state = {"phase": "init"}

    @workflow.query
    def get_state(self) -> dict:
        return dict(self._state)

    async def _stage(self, name: str, params) -> StageResult:
        return await workflow.execute_activity(
            name, params, result_type=StageResult,
            start_to_close_timeout=_START, schedule_to_close_timeout=_S2C,
            heartbeat_timeout=_HB, retry_policy=_RETRY,
        )

    @workflow.run
    async def run(self, params: MaterializeParams) -> MaterializeResult:
        errors: list[str] = []
        self._state["phase"] = "centrality"
        c = await self._stage("materialize_centrality", CentralityIn(metrics=params.metrics))
        if c.error:
            errors.append(f"centrality: {c.error}")
        links = StageResult()
        if params.link_prediction:
            self._state["phase"] = "link_prediction"
            links = await self._stage("materialize_link_prediction", LinkPredictionIn())
            if links.error:
                errors.append(f"link_prediction: {links.error}")
        risk = StageResult()
        if params.risk:
            self._state["phase"] = "risk"
            risk = await self._stage("materialize_risk", RiskIn())
            if risk.error:
                errors.append(f"risk: {risk.error}")
        self._state["phase"] = "done"
        return MaterializeResult(centrality_written=c.written, links_written=links.written,
                                 risk_written=risk.written, errors=errors)
```

- [ ] **Step 4: run → PASS** (or skip if no Temporal test server). **Step 5: lint.**

---

### Task 8: Worker registration + admin endpoint

**Files:** Modify `src/workflow/search/activities/__init__.py`, `src/workflow/worker.py`, `src/api/routes/graph_admin.py`; Test `tests/test_workflow/test_materialize_registration.py`, `tests/test_api/test_materialize_route.py`

**Interfaces — Produces:** `GRAPH_BUILD_ACTIVITIES += MATERIALIZE_ACTIVITIES`; `graph_build` worker group `workflows` includes `AnalyticsMaterializeWorkflow`; `POST /admin/graph/materialize` fires the workflow.

- [ ] **Step 1: failing tests**

```python
# tests/test_workflow/test_materialize_registration.py
from src.workflow.search.activities import GRAPH_BUILD_ACTIVITIES
from src.workflow.analytics.materialize_activities import (
    materialize_centrality, materialize_link_prediction, materialize_risk,
)


def test_materialize_activities_registered():
    for a in (materialize_centrality, materialize_link_prediction, materialize_risk):
        assert a in GRAPH_BUILD_ACTIVITIES
```

```python
# tests/test_api/test_materialize_route.py
from src.api.routes.graph_admin import router


def test_materialize_route_present():
    paths = {r.path for r in router.routes}
    assert "/admin/graph/materialize" in paths
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**
  1. `src/workflow/search/activities/__init__.py`: `from src.workflow.analytics.materialize_activities import MATERIALIZE_ACTIVITIES` and `GRAPH_BUILD_ACTIVITIES = [detect_communities_activity, summarize_community_activity, *MATERIALIZE_ACTIVITIES]`.
  2. `src/workflow/worker.py` graph_build group: `workflows=[CommunityBuildWorkflow, AnalyticsMaterializeWorkflow]` (import it).
  3. `src/api/routes/graph_admin.py`:

```python
@router.post("/materialize", dependencies=[Depends(require_api_key)],
             status_code=status.HTTP_202_ACCEPTED,
             summary="Trigger offline analytics materialization (GDS centrality + link-prediction + risk)")
async def materialize() -> dict[str, str]:
    from temporalio.common import WorkflowIDReusePolicy
    from src.analytics.contracts import MaterializeParams
    from src.workflow.analytics.materialize_workflow import AnalyticsMaterializeWorkflow
    from src.workflow.client import get_temporal_client

    request_id = uuid.uuid4().hex
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            AnalyticsMaterializeWorkflow.run, MaterializeParams(),
            id=f"analytics-materialize-{request_id}",
            task_queue=settings.temporal.graph_build_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        return {"workflow_id": handle.id, "status": "started"}
    except Exception as exc:  # noqa
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            f"materialize failed to start: {exc}") from exc
```

> Implementer note: confirm `graph_admin.py` already imports `uuid`, `status`, `HTTPException`, `Depends`, `require_api_key`, `settings`; add any missing. Match the file's existing import placement.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase D — v1b online read primitives

### Task 9: `centrality.py` — `top_central_entities`, `link_prediction`

**Files:** Create `src/analytics/primitives/centrality.py`; Test `tests/test_analytics/test_centrality.py`

**Interfaces — Produces (tier="offline-mat"):**
- `top_central_entities(metric="pagerank", type=None, top_n=20)` — reads `e.<metric>` (metric from allowlist `{pagerank,betweenness,eigenvector}`; reject others → empty).
- `link_prediction(name, top_n=20)` — reads `(e {name})-[l:LIKELY_LINK]->(m) RETURN m.name, l.score`.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_centrality.py
import pytest
from src.analytics.primitives import centrality as c
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_top_central_reads_metric_property():
    store = _FakeStore(rows=[{"name": "A", "score": 0.9}])
    res = await c.top_central_entities(store, metric="betweenness", top_n=5)
    assert "e.betweenness" in res.cypher and res.params["top_n"] == 5


@pytest.mark.asyncio
async def test_top_central_rejects_unknown_metric():
    store = _FakeStore(rows=[{"x": 1}])
    res = await c.top_central_entities(store, metric="bogus")
    assert res.rows == []                      # allowlist guard → empty, no injection


@pytest.mark.asyncio
async def test_link_prediction_reads_edges():
    store = _FakeStore(rows=[{"name": "B", "score": 0.8}])
    res = await c.link_prediction(store, name="A")
    assert ":LIKELY_LINK" in res.cypher and res.params["name"] == "A"
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/analytics/primitives/centrality.py
"""Family 3 heavy tier (offline-materialized reads): centrality + link prediction."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows

_METRICS = {"pagerank", "betweenness", "eigenvector"}


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class TopCentralParams(_Params):
    metric: str = "pagerank"
    type: str | None = None
    top_n: int = 20


async def top_central_entities(store: Any | None, *, metric: str = "pagerank",
                              type: str | None = None, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    if metric not in _METRICS:
        return PrimitiveResult(cypher="", params={"metric": metric}, rows=[])
    cypher = (
        f"MATCH (e:__Entity__) WHERE e.{metric} IS NOT NULL "
        "AND ($type IS NULL OR $type IN labels(e)) "
        f"RETURN e.name AS name, e.{metric} AS score ORDER BY e.{metric} DESC LIMIT $top_n"
    )
    params = {"type": type, "top_n": top_n, "metric": metric}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params),
                           truncated=True)


class LinkPredictionParams(_Params):
    name: str
    top_n: int = 20


async def link_prediction(store: Any | None, *, name: str, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__ {name:$name})-[l:LIKELY_LINK]->(m:__Entity__) "
        "RETURN m.name AS name, l.score AS score ORDER BY l.score DESC LIMIT $top_n"
    )
    params = {"name": name, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("top_central_entities", top_central_entities, TopCentralParams,
                   "Top entities by structural centrality (pagerank/betweenness/eigenvector) — reads "
                   "offline-materialized scores.", tier="offline-mat"))
register(Primitive("link_prediction", link_prediction, LinkPredictionParams,
                   "Probable not-yet-recorded links for an entity (a hypothesis) — reads materialized "
                   ":LIKELY_LINK edges.", tier="offline-mat"))
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase E — P2 composite signals & queues

### Task 10: `signals.py` (part 1) — `risk_score` read

**Files:** Create `src/analytics/primitives/signals.py`; Test `tests/test_analytics/test_signals.py`

**Interfaces — Produces:** `risk_score(name=None, band=None, top_n=20)` (tier="offline-mat") — reads materialized `e.risk_score/risk_band/risk_components`; provenance lists firing components. Filter by `name` (one entity) or `band` (e.g. "high"); else top by score.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_signals.py
import pytest
from src.analytics.primitives import signals as sig
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_risk_score_reads_materialized():
    store = _FakeStore(rows=[{"name": "Shell", "score": 0.8, "band": "high", "components": "{}"}])
    res = await sig.risk_score(store, band="high")
    assert "e.risk_score" in res.cypher and res.params["band"] == "high"


@pytest.mark.asyncio
async def test_risk_score_by_name():
    store = _FakeStore(rows=[{"name": "A", "score": 0.4, "band": "medium", "components": "{}"}])
    res = await sig.risk_score(store, name="A")
    assert res.params["name"] == "A"
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/analytics/primitives/signals.py
"""P2 — composite, decision-ready signals & queues (read materialized scores + compose Wave-0 primitives)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import ID_TYPES, clamp_top_n
from src.analytics.store_query import run_rows


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RiskScoreParams(_Params):
    name: str | None = None
    band: str | None = None
    top_n: int = 20


async def risk_score(store: Any | None, *, name: str | None = None, band: str | None = None,
                     top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
        "AND ($name IS NULL OR e.name=$name) AND ($band IS NULL OR e.risk_band=$band) "
        "RETURN e.name AS name, e.risk_score AS score, e.risk_band AS band, "
        "e.risk_components AS components ORDER BY e.risk_score DESC LIMIT $top_n"
    )
    params = {"name": name, "band": band, "top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params),
                           tier_note="reads offline-materialized risk_score")


register(Primitive("risk_score", risk_score, RiskScoreParams,
                   "Per-entity composite risk_score + band (low/medium/high) with firing components — "
                   "a transparent triage heuristic, not ground truth. Reads materialized scores.",
                   tier="offline-mat"))
```

> Implementer note: `PrimitiveResult` (Wave 0) has fields `cypher, params, rows, source_chunks, truncated` — it does NOT have `tier_note`. Remove the `tier_note=` kwarg (it will error); put the caveat in the `register(...)` description instead (as shown). This is a deliberate trap to catch — match the real `PrimitiveResult` dataclass.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 11: `signals.py` (part 2) — `investigate_next`, `review_queue`, `recommended_merges`

**Files:** Modify `src/analytics/primitives/signals.py`; Test extend `tests/test_analytics/test_signals.py`

**Interfaces — Produces:** `investigate_next(top_n=20)` (risk high × low completeness), `review_queue(top_n=50)` (contradictions ∪ merge candidates ∪ shell signals, ranked), `recommended_merges(top_n=50)` (duplicate-name groups — composes Wave 0 `merge_candidates`). These compose existing primitives/materialized props; each returns ranked rows + provenance.

- [ ] **Step 1: failing test**

```python
# append to tests/test_analytics/test_signals.py
@pytest.mark.asyncio
async def test_investigate_next_ranks_high_risk_low_completeness():
    store = _FakeStore(rows=[{"name": "X", "risk": 0.9, "completeness": 0.2}])
    res = await sig.investigate_next(store)
    assert "risk_score" in res.cypher and res.params["top_n"] == 20


@pytest.mark.asyncio
async def test_recommended_merges_groups_dup_names():
    store = _FakeStore(rows=[{"key": "ромашка", "names": ["Ромашка", "РОМАШКА"], "count": 2}])
    res = await sig.recommended_merges(store)
    assert "toLower" in res.cypher
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — append:

```python
class InvestigateNextParams(_Params):
    top_n: int = 20


async def investigate_next(store: Any | None, *, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__) WHERE e.risk_score IS NOT NULL "
        "RETURN e.name AS name, e.risk_score AS risk, "
        "coalesce(e.completeness_score, 0.0) AS completeness "
        "ORDER BY e.risk_score DESC, coalesce(e.completeness_score,0.0) ASC LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class RecommendedMergesParams(_Params):
    top_n: int = 50


async def recommended_merges(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    top_n = clamp_top_n(top_n, default=50)
    cypher = (
        "MATCH (e:__Entity__) WHERE NONE(l IN labels(e) WHERE l IN $id_types) "
        "WITH toLower(trim(e.name)) AS key, collect(e.name) AS names, count(e) AS count "
        "WHERE count > 1 RETURN key, names, count ORDER BY count DESC LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


class ReviewQueueParams(_Params):
    top_n: int = 50


async def review_queue(store: Any | None, *, top_n: int = 50) -> PrimitiveResult:
    """Shell-signal organizations (only identifier links) — the cheapest structural red flag for the queue."""
    top_n = clamp_top_n(top_n, default=50)
    cypher = (
        "MATCH (e:__Entity__:Organization) "
        "OPTIONAL MATCH (e)-[]-(n:__Entity__) "
        "WITH e, count(n) AS deg, "
        "sum(CASE WHEN any(l IN labels(n) WHERE l IN $id_types) THEN 1 ELSE 0 END) AS id_links "
        "WHERE deg > 0 AND deg = id_links "
        "RETURN e.name AS name, deg AS degree, 'shell_signal' AS flag LIMIT $top_n"
    )
    params = {"top_n": top_n, "id_types": ID_TYPES}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("investigate_next", investigate_next, InvestigateNextParams,
                   "Ranked lead list: high risk_score × low completeness — who deserves attention and is "
                   "under-documented. Reads materialized scores.", tier="offline-mat"))
register(Primitive("recommended_merges", recommended_merges, RecommendedMergesParams,
                   "Ranked duplicate-display-name groups — a recommended-merge queue."))
register(Primitive("review_queue", review_queue, ReviewQueueParams,
                   "Structural red-flag queue: organizations whose only links are identifiers (shell signal)."))
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 12: `signals.py` (part 3) — `circular_ownership` red flag

**Files:** Modify `src/analytics/primitives/signals.py`; Test extend `tests/test_analytics/test_signals.py`

**Interfaces — Produces:** `circular_ownership(top_n=20)` — `OWNS` cycles (length 2..6).

- [ ] **Step 1: failing test**

```python
@pytest.mark.asyncio
async def test_circular_ownership_cypher():
    store = _FakeStore(rows=[{"cycle": ["A", "B", "A"]}])
    res = await sig.circular_ownership(store)
    assert ":OWNS*2..6" in res.cypher or "OWNS*2..6" in res.cypher
```

- [ ] **Step 2: run → FAIL.** **Step 3: implement** — append:

```python
class CircularOwnershipParams(_Params):
    top_n: int = 20


async def circular_ownership(store: Any | None, *, top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH p=(a:__Entity__)-[:OWNS*2..6]->(a) "
        "RETURN [n IN nodes(p) | n.name] AS cycle LIMIT $top_n"
    )
    params = {"top_n": top_n}
    return PrimitiveResult(cypher=cypher, params=params, rows=await run_rows(store, cypher, params))


register(Primitive("circular_ownership", circular_ownership, CircularOwnershipParams,
                   "Ownership cycles (A owns … owns A) — a circular-ownership red flag."))
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase F — Arc 1 numeric rollups

### Task 13: `rollups.py` — `numeric_rollup`

**Files:** Create `src/analytics/primitives/rollups.py`; Test `tests/test_analytics/test_rollups.py`

**Interfaces — Produces:** `numeric_rollup(counterparty=None, top_n=20)` — sum/count of `Amount` identifier values attached to entities (per counterparty when `counterparty` set, else top counterparties by total). `Amount` node `.name` holds the value string → parse via a pure helper `parse_amount(s) -> float | None`.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_rollups.py
import pytest
from src.analytics.primitives import rollups as ro
from tests.test_analytics.conftest import _FakeStore


def test_parse_amount():
    assert ro.parse_amount("1 200,50") == 1200.5
    assert ro.parse_amount("$3,000.00") == 3000.0
    assert ro.parse_amount("n/a") is None


@pytest.mark.asyncio
async def test_numeric_rollup_sums_amounts_in_python():
    store = _FakeStore(rows=[
        {"counterparty": "A", "amount": "1 000"},
        {"counterparty": "A", "amount": "500"},
        {"counterparty": "B", "amount": "x"},
    ])
    res = await ro.numeric_rollup(store)
    by = {r["counterparty"]: r for r in res.rows}
    assert by["A"]["total"] == 1500.0 and by["A"]["count"] == 2
    assert "B" not in by or by["B"]["count"] == 0       # unparseable dropped


@pytest.mark.asyncio
async def test_failsoft():
    assert (await ro.numeric_rollup(None)).rows == []
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement**

```python
# src/analytics/primitives/rollups.py
"""Arc 1 — numeric rollups over Amount identifier entities (mini-OLAP). Parsing is pure."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows

_NUM = re.compile(r"-?\d[\d  .,]*")


def parse_amount(s: str) -> float | None:
    if not s:
        return None
    m = _NUM.search(s)
    if not m:
        return None
    tok = m.group(0).replace(" ", "").replace(" ", "")
    # if both separators present, treat ',' as thousands; else ',' as decimal
    if "," in tok and "." in tok:
        tok = tok.replace(",", "")
    elif "," in tok:
        tok = tok.replace(",", ".")
    try:
        return float(tok)
    except ValueError:
        return None


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class NumericRollupParams(_Params):
    counterparty: str | None = None
    top_n: int = 20


async def numeric_rollup(store: Any | None, *, counterparty: str | None = None,
                         top_n: int = 20) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__)-[]-(a:__Entity__:Amount) "
        "WHERE ($cp IS NULL OR e.name=$cp) "
        "RETURN e.name AS counterparty, a.name AS amount"
    )
    params = {"cp": counterparty}
    raw = await run_rows(store, cypher, params)
    agg: dict[str, dict] = defaultdict(lambda: {"total": 0.0, "count": 0})
    for r in raw:
        v = parse_amount(str(r.get("amount", "")))
        if v is None:
            continue
        cp = r.get("counterparty")
        agg[cp]["total"] += v
        agg[cp]["count"] += 1
    rows = [{"counterparty": k, "total": round(v["total"], 2), "count": v["count"]}
            for k, v in agg.items()]
    rows.sort(key=lambda x: x["total"], reverse=True)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows[:top_n])


register(Primitive("numeric_rollup", numeric_rollup, NumericRollupParams,
                   "Sum/count of Amount values per counterparty (mini-OLAP over identifier amounts)."))
```

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase G — Integration

### Task 14: Catalog completeness + finalize `__init__` + full gate

**Files:** Modify `src/analytics/primitives/__init__.py`; Test extend `tests/test_analytics/test_catalog_complete.py`

**Interfaces:** import `centrality`, `signals`, `rollups` in the primitives package so registration runs; extend the completeness test with the Wave-1 names; final lint+format+full-suite gate.

- [ ] **Step 1: extend the completeness test** — add to `_EXPECTED` in `tests/test_analytics/test_catalog_complete.py`:

```python
    # Wave 1 — v1b
    "top_central_entities", "link_prediction",
    # Wave 1 — P2
    "risk_score", "investigate_next", "recommended_merges", "review_queue", "circular_ownership",
    # Wave 1 — Arc 1
    "numeric_rollup",
```

- [ ] **Step 2: run → FAIL** (new names not yet imported/registered).

- [ ] **Step 3: implement** — add to `src/analytics/primitives/__init__.py` the three imports:

```python
from src.analytics.primitives import centrality  # noqa: F401
from src.analytics.primitives import rollups  # noqa: F401
from src.analytics.primitives import signals  # noqa: F401
```

- [ ] **Step 4: run → PASS** + sanity:

```bash
uv run pytest tests/test_analytics/test_catalog_complete.py -q
python -c "import src.analytics.primitives; from src.analytics.catalog import CATALOG; print(len(CATALOG))"   # >= 37
```

- [ ] **Step 5: full gate** (controller runs): `uv run ruff check <all changed files>` · `uv run ruff format --check <them>` · `uv run pytest -q` — confirm NO new failures beyond the known pre-existing baseline.

---

## Self-Review

**1. Spec coverage**

| Spec item | Task |
|---|---|
| analytical §5 offline materialization workflow | 4–8 |
| analytical §4 `top_central_entities` 🟠 | 9 |
| analytical §4 `link_prediction` 🟠 | 4 (write), 9 (read) |
| analytical §11 v1b (materialize + heavy reads + admin trigger) | 4–9 |
| signals §2 composite `risk_score` (materialized) | 3, 6, 10 |
| signals §2 red flags (circular ownership, shell signal) | 11 (shell via review_queue), 12 (circular) |
| signals §5 action queues (investigate-next, review queue, recommended merges) | 11 |
| signals §9 P2 phasing | 3, 6, 10–12 |
| analytical §12 Arc 1 numeric rollups | 13 |
| config (risk weights/bands, materialize concurrency) | 1 |
| testing (pure scorer, Cypher-shape, workflow time-skip) | every task |
| **Temporal Schedule for materialization** | **deferred** (Wave 1 = admin-trigger only; Schedule is a later wave) |
| **completeness_score full materialization** | partial (risk components cover it; folded as follow-up note in Task 6) |
| signals §2 nominee/bridge red flag; §4 domain rollups (Issue/Resolution, comms, sentiment) | **out of scope** → Wave 2 P3 |
| Arc 1 structural embeddings / temporal-graph snapshots / motifs (beyond rollups) | **out of scope** → far |

**2. Placeholder scan:** no TBD/TODO. Two deliberate "implementer-note traps" (the `tier_note=` kwarg that must be removed in Task 10; the `hasattr` `since` that must use `settings.events.new_window_days` in Task 6) are flagged with the correct fix — these are real corrections, not placeholders. GDS procedure names are UNVERIFIED against a live install (noted in Global Constraints #11 + Task deploy note) — unit tests assert Cypher shape only; a live smoke test is a deploy-time step.

**3. Type consistency:** `PrimitiveResult(cypher, params, rows, source_chunks, truncated)` and `Primitive(name, fn, param_model, description, tier)` match the Wave 0 dataclasses (Task 10 trap enforces this). `StageResult(written, error)` / `MaterializeResult(...)` / `MaterializeParams(...)` consistent across activities (5,6) → workflow (7) → admin (8). `compute_risk(...)->RiskResult` (3) consumed by `materialize_risk` (6). Materialized property names (`e.pagerank/betweenness/eigenvector/risk_score/risk_band/risk_components`, `:LIKELY_LINK {score}`) consistent between writers (4,6) and readers (9,10,11).

**Known boundaries:** Wave 1 ships admin-triggered materialization (no Schedule); domain rollups (P3), nominee red flag, structural embeddings → later waves. The Wave 0 fast-follow `contradictions` merge-time reimplementation and per-type rel `created_at` index remain open (not in this plan's scope).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-29-analytical-layer-wave1.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task + per-task review + final whole-branch review (`superpowers:subagent-driven-development`).
2. **Inline Execution** — batched with checkpoints (`superpowers:executing-plans`).

Which approach?

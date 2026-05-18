# Per-role LLM models + split merge into a child workflow

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two independent improvements that ship as two stages.

1. **Per-role LLM models** — replace the single project-wide `build_llm()` with three role-specific factories (`extraction`, `judge`, `search`) so the operator can pick the right size/speed model for each job.
2. **Split merge + property-graph build into a child workflow** — moves the heavy LLM merge step (`merge_and_resolve` + `build_property_graph`) into a separate `GraphBuildWorkflow` that runs as a Temporal child of `DocumentIngestWorkflow`. Independent retry / observability / scheduling for the slow half; parent finishes vector ingest faster, falls back to `vector_only` cleanly when the child fails.

**Architecture:**
- Per-role models: add three optional model name fields to `LiteLLMSettings` (each defaulting to the existing `llm_model`), expose `build_llm(role)` plus thin wrappers `build_extraction_llm() / build_judge_llm() / build_search_llm()`. Translation falls under `extraction`.
- Child workflow: new `src/workflow/graph_build.py` with `GraphBuildWorkflow` running two existing activities back-to-back. Parent calls `workflow.execute_child_workflow(GraphBuildWorkflow.run, kg, task_queue=settings.temporal.llm_task_queue, id=f"graph-{doc_id}")`. Inner `try/except ChildWorkflowError` keeps the `vector_only` downgrade behaviour intact.

**Tech Stack:** Python 3.12, temporalio 1.8+, LiteLLM-proxied LlamaIndex `OpenAILike`, Pydantic v2 settings.

**Spec context:** Temporal workflow design at `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md`. Current activities: `src/workflow/activities/{extract_kg,merge_and_resolve,build_property_graph}.py`. Current LLM factory: `src/retrieval/llm.py:build_llm`.

**Session protocol:** Pause after each labelled **Stage** for sync.

**Defaults chosen without explicit ask** (flag if you disagree before T1):
- Translation (`parse_and_chunk`) uses the **extraction** model. Translation requires similar semantic depth to extraction; both touch the full chunk text.
- Child workflow is **awaited** (`execute_child_workflow`), not fire-and-forget. Keeps "doc is complete" semantics simple. Sibling/fire-and-forget pattern is a later option if we want batched merges across docs.
- Child runs on the **`kb-ingest-llm` task queue** — its activities are LLM-bound, the queue cap already protects the GPU.
- `ChildWorkflowError` is caught by parent's inner `try/except` and downgrades `graph_status` to `vector_only`, same as today's activity-error path.

---

## Stage 1 — Per-role LLM models

### Task 1: Add three role-specific model fields to `LiteLLMSettings`

**Files:**
- Modify: `src/config.py` (`LiteLLMSettings`, lines 96-121).
- Modify: `.env.example`.
- Modify: `tests/test_config.py` — settings roundtrip + fallback assertion.

- [ ] **Step 1: Add the test first**

In `tests/test_config.py`, add:

```python
def test_per_role_model_defaults_fall_back_to_llm_model(monkeypatch):
    """When the role-specific env var is unset, the role factory
    must read `llm_model` so existing deployments aren't broken."""
    monkeypatch.delenv("LITELLM_EXTRACTION_MODEL", raising=False)
    monkeypatch.delenv("LITELLM_JUDGE_MODEL", raising=False)
    monkeypatch.delenv("LITELLM_SEARCH_MODEL", raising=False)
    monkeypatch.setenv("LITELLM_MODEL", "fallback-model")

    # Re-import to pick up monkeypatched env.
    from importlib import reload
    import src.config as cfg
    reload(cfg)
    assert cfg.settings.litellm.model_for("extraction") == "fallback-model"
    assert cfg.settings.litellm.model_for("judge") == "fallback-model"
    assert cfg.settings.litellm.model_for("search") == "fallback-model"


def test_per_role_model_explicit_override(monkeypatch):
    monkeypatch.setenv("LITELLM_MODEL", "default-model")
    monkeypatch.setenv("LITELLM_EXTRACTION_MODEL", "extract-3.5b")
    monkeypatch.setenv("LITELLM_JUDGE_MODEL", "judge-1.5b")
    monkeypatch.setenv("LITELLM_SEARCH_MODEL", "search-7b")

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    assert cfg.settings.litellm.model_for("extraction") == "extract-3.5b"
    assert cfg.settings.litellm.model_for("judge") == "judge-1.5b"
    assert cfg.settings.litellm.model_for("search") == "search-7b"
```

Expected: ImportError / AttributeError on `model_for`. Good — drives the impl.

- [ ] **Step 2: Extend `LiteLLMSettings`**

In `src/config.py`, in `LiteLLMSettings`, after `llm_model`:

```python
    # Per-role overrides for the project LLM. Empty (None) means
    # "use ``llm_model``" — keeps single-model deployments simple.
    # Use these when you want a smaller / faster model for high-volume
    # judge calls (cross-chunk merge, ER pair-wise judgements) while
    # keeping a stronger model for extraction or for the answering
    # agent.  See ``model_for`` for resolution semantics.
    extraction_model: str | None = None
    judge_model: str | None = None
    search_model: str | None = None

    def model_for(self, role: Literal["extraction", "judge", "search"]) -> str:
        """Return the configured model name for ``role`` with fallback
        to ``llm_model`` when the role-specific field is empty."""
        if role == "extraction":
            return self.extraction_model or self.llm_model
        if role == "judge":
            return self.judge_model or self.llm_model
        if role == "search":
            return self.search_model or self.llm_model
        raise ValueError(f"unknown llm role: {role!r}")
```

Add `from typing import Literal` at the top of `src/config.py` if not present.

- [ ] **Step 3: Update `.env.example`**

```env
LITELLM_BASE_URL=http://localhost:4000
LITELLM_API_KEY=sk-litellm-stub
LITELLM_MODEL=qwen3:8b
# Per-role overrides (empty = use LITELLM_MODEL).
# Recommended pairings:
#   extraction → biggest / smartest (KG entity & relation extraction).
#   judge      → cheapest / fastest (cross-chunk merge summary +
#                ER pair-wise yes/no judgements).
#   search     → balanced (ReAct agent + reflective synthesis).
LITELLM_EXTRACTION_MODEL=
LITELLM_JUDGE_MODEL=
LITELLM_SEARCH_MODEL=
LITELLM_EMBEDDING_MODEL=nomic-embed-text
```

- [ ] **Step 4: Run tests + commit**

```bash
uv run pytest tests/test_config.py -v
git add src/config.py .env.example tests/test_config.py
git commit -m "feat(config): per-role LLM model overrides (extraction/judge/search)"
```

---

### Task 2: Role-based factories in `src/retrieval/llm.py`

**Files:**
- Modify: `src/retrieval/llm.py`.
- Test: `tests/test_retrieval/test_llm_factory.py` (new).

- [ ] **Step 1: Write the failing test**

```python
"""Role-based LLM factory tests.

Confirms the three wrappers pull the right model name from settings
and that legacy ``build_llm()`` still works (returns the
``llm_model`` default)."""
from __future__ import annotations

from unittest.mock import patch


def test_build_extraction_llm_uses_extraction_model(monkeypatch):
    monkeypatch.setenv("LITELLM_EXTRACTION_MODEL", "ext-model")

    captured: dict = {}
    def _spy(*a, **kw):
        captured.update(kw)
        from unittest.mock import MagicMock
        return MagicMock()

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.retrieval.llm as llm_mod
    reload(llm_mod)
    with patch("src.retrieval.llm.OpenAILike", side_effect=_spy):
        llm_mod.build_extraction_llm()
    assert captured["model"] == "ext-model"


def test_build_llm_no_role_falls_back_to_llm_model(monkeypatch):
    monkeypatch.delenv("LITELLM_EXTRACTION_MODEL", raising=False)
    monkeypatch.setenv("LITELLM_MODEL", "default-model")

    captured: dict = {}
    def _spy(*a, **kw):
        captured.update(kw)
        from unittest.mock import MagicMock
        return MagicMock()

    from importlib import reload
    import src.config as cfg
    reload(cfg)
    import src.retrieval.llm as llm_mod
    reload(llm_mod)
    with patch("src.retrieval.llm.OpenAILike", side_effect=_spy):
        llm_mod.build_llm()           # legacy
    assert captured["model"] == "default-model"
```

- [ ] **Step 2: Rewrite the factory**

In `src/retrieval/llm.py`, keep the legacy `build_llm()` (no role kwarg) so call sites we don't touch in this stage keep working. Add three role-keyed wrappers:

```python
from typing import Literal

from llama_index.core.llms import LLM
from llama_index.llms.openai_like import OpenAILike

from src.config import settings


_LLM_Role = Literal["extraction", "judge", "search"]


def _build(model: str) -> LLM:
    cfg = settings.litellm
    return OpenAILike(
        api_base=cfg.base_url,
        api_key=cfg.api_key.get_secret_value(),
        model=model,
        is_chat_model=True,
        timeout=cfg.timeout_s,
        max_retries=cfg.max_retries,
    )


def build_llm(role: _LLM_Role | None = None) -> LLM:
    """Construct an LLM client for the given ``role``.

    ``role=None`` (legacy) returns the same LLM the project used
    before per-role models existed — backed by ``LITELLM_MODEL``.
    Call sites should migrate to the explicit role wrappers below.
    """
    if role is None:
        return _build(settings.litellm.llm_model)
    return _build(settings.litellm.model_for(role))


def build_extraction_llm() -> LLM:
    return build_llm("extraction")


def build_judge_llm() -> LLM:
    return build_llm("judge")


def build_search_llm() -> LLM:
    return build_llm("search")
```

(Adjust to the existing imports/decorators in the file.)

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_retrieval/test_llm_factory.py -v
git add src/retrieval/llm.py tests/test_retrieval/test_llm_factory.py
git commit -m "feat(llm): build_extraction_llm / build_judge_llm / build_search_llm"
```

---

### Task 3: Wire role-specific factories into call sites

**Files (every place that calls `build_llm()` today):**
- Modify: `src/workflow/activities/extract_kg.py` → `build_extraction_llm()`.
- Modify: `src/workflow/activities/parse_and_chunk.py` → `build_extraction_llm()` (translator).
- Modify: `src/workflow/activities/merge_and_resolve.py` → `build_judge_llm()` for both `merge_kg_extraction` and `resolve_entities`.
- Modify: `src/di/providers.py` (the LLM provider that backs `/agent` & friends) → `build_search_llm()`.
- Modify: `src/ingestion/run.py` (CLI ingester) → `build_extraction_llm()` for translator.
- Update affected tests' patch paths (they currently patch `build_llm`; switch to `build_extraction_llm` / `build_judge_llm` / `build_search_llm` per file).

- [ ] **Step 1: Inventory call sites**

```bash
grep -rln "build_llm()" src/ tests/ | sort | uniq
```

Confirm there are no surprises beyond the 5 source files + their tests.

- [ ] **Step 2: Replace per file**

For each source file:

```python
# Before
from src.retrieval.llm import build_llm
...
llm = build_llm()

# After (example for extract_kg)
from src.retrieval.llm import build_extraction_llm
...
llm = build_extraction_llm()
```

Use the role mapping at the top of this task. **Important** — `merge_and_resolve` uses one LLM for two distinct calls today (merge summary + ER). Both stay on `build_judge_llm()` — they're cheap, batch-friendly, and benefit from a smaller model.

- [ ] **Step 3: Update tests**

Every test that monkey-patches `build_llm` must patch the matching role factory. Example:

```python
# tests/test_workflow/test_extract_kg.py — before
), patch(
    "src.workflow.activities.extract_kg.build_llm",
    return_value=MagicMock(),
)
# after
), patch(
    "src.workflow.activities.extract_kg.build_extraction_llm",
    return_value=MagicMock(),
)
```

- [ ] **Step 4: Run + commit**

```bash
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
git add src/workflow/activities/ src/di/providers.py src/ingestion/run.py \
        tests/
git commit -m "feat: route call sites to role-specific LLM factories"
```

---

**🛑 STAGE 1 GATE.**  Quick smoke: bring up the worker and confirm the start banner shows the three configured models (add a one-line log there in T2 if it isn't already explicit). Re-ingest a fixture doc; verify `extract_kg` / `merge_and_resolve` / `agent` each used their configured model (LiteLLM's log shows the requested model name).

---

## Stage 2 — Split merge + graph-build into a child workflow

### Task 4: `GraphBuildWorkflow` (new workflow class)

**Files:**
- Create: `src/workflow/graph_build.py`.
- Test: `tests/test_workflow/test_graph_build_workflow.py` (new).

- [ ] **Step 1: Write the failing test**

`tests/test_workflow/test_graph_build_workflow.py`:

```python
"""GraphBuildWorkflow runs `merge_and_resolve` then
`build_property_graph` as a single child workflow.  Tests against
the docker-compose Temporal (matches sibling tests in
test_document_ingest_workflow.py)."""
from __future__ import annotations

import socket
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from src.workflow.contracts import (
    Ctx,
    GraphBuilt,
    KGExtracted,
    Merged,
    Parsed,
)
from src.workflow.graph_build import GraphBuildWorkflow


def _temporal_up() -> bool:
    try:
        with socket.create_connection(("localhost", 7233), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _temporal_up(),
    reason="docker-compose Temporal (localhost:7233) not reachable",
)


@activity.defn(name="merge_and_resolve")
async def merge_stub(kg: KGExtracted) -> Merged:
    return Merged(
        kg=kg,
        merged_entities_uri="s3://kb-staging/run-test/merged.pkl",
    )


@activity.defn(name="build_property_graph")
async def build_pg_stub(merged: Merged) -> GraphBuilt:
    return GraphBuilt(entities=3, relations=2)


@pytest.mark.asyncio
async def test_graph_build_workflow_chains_two_activities():
    client = await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )
    queue = f"gb-test-{uuid.uuid4()}"
    ctx = Ctx(doc_id="d", local_path="/x", cleanup_dir=None,
              workflow_run_id="run-test")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/run-test/parsed.pkl",
                    chunk_count=1)
    kg = KGExtracted(parsed=parsed,
                     nodes_with_kg_uri="s3://kb-staging/run-test/kg.pkl")
    async with Worker(
        client, task_queue=queue,
        workflows=[GraphBuildWorkflow],
        activities=[merge_stub, build_pg_stub],
    ):
        result = await client.execute_workflow(
            GraphBuildWorkflow.run, kg,
            id=f"graph-{uuid.uuid4()}", task_queue=queue,
        )
    assert isinstance(result, GraphBuilt)
    assert result.entities == 3
    assert result.relations == 2
```

- [ ] **Step 2: Implement the child workflow**

`src/workflow/graph_build.py`:

```python
"""`GraphBuildWorkflow` — runs the heavy LLM merge + Neo4j graph
build as a Temporal **child** of `DocumentIngestWorkflow`.

Splitting them out of the parent has three concrete wins:
  * Independent retry policy and schedule_to_close ceiling — a stuck
    merge can be retried / cancelled without restarting the whole
    document ingest.
  * Separate visibility in Temporal Web UI: parent shows
    `ingest-{doc_id}` finishing in seconds for the vector half;
    child shows `graph-{doc_id}` doing the slow LLM work.
  * Future flexibility: parent can stop awaiting the child (sibling
    pattern) once we want to batch merges across multiple docs.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import GraphBuilt, KGExtracted, Merged


_HEAVY_FOREVER = RetryPolicy(
    initial_interval=timedelta(minutes=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=30),
    maximum_attempts=0,
)
_FAST_FOREVER = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=0,
)


@workflow.defn
class GraphBuildWorkflow:
    @workflow.run
    async def run(self, kg: KGExtracted) -> GraphBuilt:
        log = workflow.logger
        workflow.upsert_memo({
            "doc_id": kg.parsed.ctx.doc_id,
            "stage": "merge_and_resolve",
        })
        log.info(
            "graph_build start  doc_id=%s  chunks=%d",
            kg.parsed.ctx.doc_id, kg.parsed.chunk_count,
        )

        merged: Merged = await workflow.execute_activity(
            "merge_and_resolve", kg,
            result_type=Merged,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(hours=24),
            retry_policy=_HEAVY_FOREVER,
        )
        log.info("← merge_and_resolve  uri=%s", merged.merged_entities_uri)

        workflow.upsert_memo({"stage": "build_property_graph"})
        built: GraphBuilt = await workflow.execute_activity(
            "build_property_graph", merged,
            result_type=GraphBuilt,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=5),
            schedule_to_close_timeout=timedelta(hours=24),
            retry_policy=_FAST_FOREVER,
        )
        log.info(
            "graph_build done  entities=%d  relations=%d",
            built.entities, built.relations,
        )
        return built
```

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_workflow/test_graph_build_workflow.py -v
git add src/workflow/graph_build.py \
        tests/test_workflow/test_graph_build_workflow.py
git commit -m "feat(workflow): GraphBuildWorkflow — child for merge + property-graph"
```

---

### Task 5: Parent calls child instead of inline activities

**Files:**
- Modify: `src/workflow/document_ingest.py` — replace `merge_and_resolve`
  + `build_property_graph` activity calls with a single
  `execute_child_workflow(GraphBuildWorkflow.run, kg, ...)`.
- Modify: `tests/test_workflow/test_document_ingest_workflow.py` — register
  `GraphBuildWorkflow` on the test worker and update the failure-mode
  stubs to throw from inside the child where applicable.

- [ ] **Step 1: Edit the parent**

In `src/workflow/document_ingest.py`, replace:

```python
# Before
merged = await workflow.execute_activity(
    "merge_and_resolve", kg, ...
)
built = await workflow.execute_activity(
    "build_property_graph", merged, ...
)
```

with:

```python
from src.workflow.graph_build import GraphBuildWorkflow
from temporalio.exceptions import ChildWorkflowError
from temporalio.workflow import ParentClosePolicy

# After (still inside the same inner try/except — see below)
built = await workflow.execute_child_workflow(
    GraphBuildWorkflow.run, kg,
    id=f"graph-{params.doc_id}",
    task_queue=settings.temporal.llm_task_queue,
    parent_close_policy=ParentClosePolicy.REQUEST_CANCEL,
    id_reuse_policy=workflow.WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
)
```

Extend the inner `except ActivityError` to also catch
`ChildWorkflowError` so a failed child still downgrades the parent
to `graph_status="vector_only"`:

```python
except (ActivityError, ChildWorkflowError) as exc:
    log.warning("graph stage failed, downgrading to vector_only: %s", exc)
    graph_status = "vector_only"
```

- [ ] **Step 2: Adjust workflow tests**

In `tests/test_workflow/test_document_ingest_workflow.py`:

- Register both workflows on the test Worker:
  ```python
  workflows=[DocumentIngestWorkflow, GraphBuildWorkflow],
  ```
- `test_graph_failure_downgrades_to_vector_only` previously raised
  from `extract_kg`. That stays. Add a second variant
  (`test_graph_failure_via_child_downgrades`) where `merge_and_resolve`
  raises `ApplicationError(non_retryable=True)` so the failure goes
  through the child — proves the parent catches `ChildWorkflowError`.

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_workflow/test_document_ingest_workflow.py -v
uv run pytest tests/test_workflow -q --ignore=tests/test_workflow/test_workflow_local.py
git add src/workflow/document_ingest.py \
        tests/test_workflow/test_document_ingest_workflow.py
git commit -m "refactor(workflow): parent calls GraphBuildWorkflow as child"
```

---

### Task 6: Register the child workflow on the worker

**Files:**
- Modify: `src/workflow/worker.py`.

- [ ] **Step 1: Add `GraphBuildWorkflow` to the LLM-queue Worker**

The LLM worker hosts the two activities (`merge_and_resolve`,
`build_property_graph`) already. It now also needs to run the child
workflow itself.

```python
from src.workflow.graph_build import GraphBuildWorkflow

llm_worker = Worker(
    client,
    task_queue=settings.temporal.llm_task_queue,
    workflows=[GraphBuildWorkflow],
    activities=LLM_ACTIVITIES,
    max_concurrent_activities=settings.temporal.llm_activity_concurrency,
)
```

The main worker continues to host `DocumentIngestWorkflow` only.

- [ ] **Step 2: Live smoke**

```bash
uv run python -m src.workflow.worker > /tmp/wf-stage2.log 2>&1 &
WPID=$!
sleep 5
grep "temporal worker" /tmp/wf-stage2.log
# Trigger a real ingest via Bruno / curl, watch Temporal UI for the
# nested `graph-{doc_id}` workflow under `ingest-{doc_id}`.
kill $WPID
```

- [ ] **Step 3: Commit**

```bash
git add src/workflow/worker.py
git commit -m "feat(worker): register GraphBuildWorkflow on the LLM-queue worker"
```

---

### Task 7: Update spec + docs

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md` — add a Section 13 (or wherever follow-ups live) documenting the child split and per-role LLMs.
- Modify: `docs/ARCHITECTURE.md` — the workflow ascii diagram needs a "→ GraphBuildWorkflow (child)" branch and a "extraction / judge / search" annotation on LLM calls.
- Modify: `docs/bruno/Ingestion/Upload Document.bru` — add a "Two workflows per ingest" note so API consumers know to expect both `ingest-{doc_id}` and `graph-{doc_id}` in Temporal UI.

- [ ] **Step 1: Spec section**

Append to the spec something along these lines (paraphrase, don't
copy the activity table verbatim):

```markdown
## 13. Graph-build child workflow (added 2026-05-18)

`merge_and_resolve` and `build_property_graph` moved from inline
activities of `DocumentIngestWorkflow` into a new child workflow
``GraphBuildWorkflow``. Parent calls
``workflow.execute_child_workflow(GraphBuildWorkflow.run, kg, ...)``.
Inner ``try/except (ActivityError, ChildWorkflowError)`` keeps the
``vector_only`` downgrade behaviour intact.

The child runs on ``settings.temporal.llm_task_queue`` so the heavy
LLM activities still serialise against the GPU. Independent
visibility in Temporal Web UI: ``ingest-{doc_id}`` shows the vector
half completing fast, ``graph-{doc_id}`` shows the slow merge.

## 14. Per-role LLM models (added 2026-05-18)

``LiteLLMSettings`` gained three optional fields —
``extraction_model``, ``judge_model``, ``search_model`` — each
defaulting to ``llm_model``. ``build_llm(role)`` returns a client
backed by the model configured for that role. Call sites:

| Role        | Used by                                                    |
|-------------|------------------------------------------------------------|
| extraction  | `extract_kg`, `parse_and_chunk` (translator), CLI ingest   |
| judge       | `merge_and_resolve` (summary + ER LLM judge)               |
| search      | `/api/v1/{agent,selfrag,legacy/agent}` route LLMs          |

Recommended pairing: largest model for extraction, smallest for
judge (high call volume + binary outputs), balanced for search
(latency-sensitive).
```

- [ ] **Step 2: Architecture diagram**

In `docs/ARCHITECTURE.md`, update the workflow flow box to show:

```
DocumentIngestWorkflow
  ├─ fetch_source
  ├─ parse_and_chunk         (LLM: extraction)
  ├─ index_vector
  ├─ inject_canonical
  ├─ extract_kg              (LLM: extraction)
  │   ↓
  └─ GraphBuildWorkflow      (child, queue=kb-ingest-llm)
       ├─ merge_and_resolve  (LLM: judge)
       └─ build_property_graph
  ↓
  finalize
```

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs: GraphBuildWorkflow + per-role LLM model architecture"
```

---

**🛑 STAGE 2 GATE.**  Run an end-to-end ingest, then in Temporal Web UI:
- Open `ingest-{doc_id}` — its history shows a `ChildWorkflowExecutionStarted` and `ChildWorkflowExecutionCompleted` event around the graph step.
- Open the linked `graph-{doc_id}` — its own history shows the
  two activities, each with their own retry / heartbeat trace.

---

## Self-review

**Spec coverage:**
- Three models — Task 1 adds settings, Task 2 adds factories, Task 3
  routes call sites. Backward compat via `build_llm()` legacy
  fallback to `llm_model`.
- Child workflow — Task 4 adds the class, Task 5 wires the parent,
  Task 6 registers on the worker, Task 7 documents it. Failure
  semantics preserved (`ChildWorkflowError` → `vector_only`).

**Placeholder scan:** No "TBD" / "implement later"; every code block
compiles as written.

**Type consistency:**
- `_LLM_Role` literal type and the three role-name strings match
  between `LiteLLMSettings.model_for` (Task 1) and the factory
  module (Task 2).
- `GraphBuildWorkflow.run(kg: KGExtracted) -> GraphBuilt` signature
  matches what `DocumentIngestWorkflow` passes (Task 5).

**Rollback story:**
- Stage 1: setting the role-specific env vars to empty restores the
  old single-model behaviour without a code change. `build_llm()`
  with no role kwarg still works for any code that wasn't migrated.
- Stage 2: revert the parent commit (Task 5) and the worker
  commit (Task 6); the child workflow class can stay in the repo
  unused.

---

## Execution handoff

**Plan complete and saved to
`docs/superpowers/plans/2026-05-18-multimodel-and-child-merge-workflow.md`.**

Three execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task with
   review between tasks.
2. **Inline Execution** — executing-plans skill, batch with stage
   gates.
3. **Stage-by-stage operator** — you trigger Stage 1, validate live,
   then Stage 2.

Also confirm or override the four defaults I picked at the top
(translation role → extraction; child workflow awaited not
fire-and-forget; LLM queue for the child; `ChildWorkflowError`
maps to `vector_only`).

**Which approach and what to override?**

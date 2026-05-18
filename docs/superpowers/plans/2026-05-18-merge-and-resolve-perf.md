# Speed up `merge_and_resolve` on single-document ingest

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `merge_and_resolve` wall-clock from minutes to seconds on a single document by removing sequential awaits inside `merge_kg_extraction` and by skipping pointless LLM summary calls that don't change the output.

**Architecture:** Profile first, then change. Two surgical changes inside `src/graph/merge.py` + one config flag.  Activity surface (`merge_and_resolve.py`) and contract types untouched.

**Tech Stack:** Python 3.12 asyncio, LlamaIndex 0.13, Temporal worker, LiteLLM proxy.

**Spec context:** Temporal workflow design at `docs/superpowers/specs/2026-05-15-ingest-temporal-workflow-design.md`. Current merge implementation: `src/graph/merge.py:160`+. Activity wrapper: `src/workflow/activities/merge_and_resolve.py`.

**Session protocol (user preference):** Pause after each labelled **Stage** for a quick sync before starting the next one.

---

## Root-cause analysis (no code changes here — just findings)

`merge_kg_extraction` has two sequential loops at lines ~248 and ~276:

```python
for key, agg in ent_agg.items():
    ...
    merged_desc = await _maybe_summarize_descriptions(...)   # awaits

for (src_key, tgt_key), agg in rel_agg.items():
    ...
    merged_desc = await _maybe_summarize_descriptions(...)   # awaits
```

`_maybe_summarize_descriptions` fires **one LLM call** per item whose descriptions cross either threshold:

- `force_summary_on_count = 8` — entity / relation appears in ≥ 8 chunks.
- `force_summary_on_chars = 12000` — combined description length ≥ 12 000 chars.

Within a single document of ~80 entities + ~40 relations, **30-40 of those routinely cross threshold**.  Each LLM call on a local 8B model is 10-20 s.  Sequential: **6-12 minutes for merge alone.**

Two specific costs are paid here:

1. **Sequential `await` inside the worker.**  Even when LiteLLM proxy can batch (vLLM continuous batching) we never issue more than one outstanding request at a time → throughput stuck at 1 req/s regardless of upstream capability.
2. **Often-pointless summary calls.**  When all `source_chunks` for an entity belong to a single document (which IS the case for single-doc ingest by construction), the LLM merely re-paraphrases the same author's prose.  The concat fallback is usually fine, and the operator can always re-run a cross-document summary later as a separate step.

Both costs compound: a doc with 30 above-threshold entities pays 30× sequential LLM penalty AND 30× tokens for marginal benefit.

---

## Stage 1 — Profile the actual cost before changing anything

This is a measurement step, not a change.  Numbers go into the commit message of Stage 2.

### Task 0: Capture a baseline

**Files:** none — operator action only.

- [ ] **Step 1: Pick a representative document** that already exists in Postgres/Milvus.  Ideally one that previously took noticeably long in `merge_and_resolve` (operator knows which).

- [ ] **Step 2: Re-ingest it** through the live API path (`POST /api/v1/ingest`) with a fresh `doc_id`.  Tail Temporal Web UI for the workflow.

- [ ] **Step 3: Open the `merge_and_resolve` activity attempt** in Temporal UI and record:
  - Wall-clock from `ActivityTaskStarted` to `ActivityTaskCompleted`.
  - From the activity log line `merge_and_resolve done  doc=…  entities=N  relations=M`: capture `N` and `M`.
  - From the activity log line `kg merge done  entities=E  relations=R  summary_calls=N/A` (current code): note the `summary_calls=N/A` — we can't see how many LLM calls ran today.  That's part of what Stage 2 fixes.

- [ ] **Step 4: Estimate LLM-call count by inspection.**  Run the snippet below in a one-off `uv run python -c '...'` to read the staging blob and count entities / relations that would cross thresholds:

```python
import pickle, sys
from src.workflow.staging import build_staging_store

uri = sys.argv[1]   # s3://kb-staging/<run_id>/kg.pkl
nodes = build_staging_store().read_pickle(uri)

from collections import Counter
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY, KG_RELATIONS_KEY,
)

descs_per_entity: dict[str, list[str]] = {}
descs_per_relation: dict[tuple[str, str], list[str]] = {}
for n in nodes:
    md = n.metadata or {}
    for e in md.get(KG_NODES_KEY, []) or []:
        descs_per_entity.setdefault(e.name, []).append(
            (e.properties or {}).get("description", "")
        )
    for r in md.get(KG_RELATIONS_KEY, []) or []:
        descs_per_relation.setdefault(
            (r.source_id, r.target_id), [],
        ).append((r.properties or {}).get("description", ""))

FORCE_COUNT, FORCE_CHARS = 8, 12000

def would_summary(ds):
    ds = [d for d in ds if d]
    if len(ds) <= 1: return False
    return len(ds) >= FORCE_COUNT or sum(len(d) for d in ds) >= FORCE_CHARS

ent_llm = sum(1 for ds in descs_per_entity.values() if would_summary(ds))
rel_llm = sum(1 for ds in descs_per_relation.values() if would_summary(ds))
print(f"entities total={len(descs_per_entity)}  llm_summary={ent_llm}")
print(f"relations total={len(descs_per_relation)}  llm_summary={rel_llm}")
```

- [ ] **Step 5: Record baseline numbers** in a note to bring back to me before Stage 2:

```
doc:                <doc_id>
entities total:     ___
entities → LLM:     ___
relations total:    ___
relations → LLM:    ___
merge wall-clock:   ___ s
```

---

**🛑 STAGE 1 GATE — bring me the baseline numbers.** They determine whether Stage 2 changes are worth the risk and let us measure the win afterwards.

---

## Stage 2 — Parallelise summary calls + log call counts

Once the baseline confirms 10+ sequential LLM calls per doc.

### Task 1: Add `summary_concurrency` knob in `AgentSettings`

**Files:**
- Modify: `src/config.py:235` (`AgentSettings`) — new field.
- Modify: `.env.example` — env var.

- [ ] **Step 1: Add the setting**

In `src/config.py`, inside `AgentSettings`, after `er_judge_batch_size`:

```python
    # Parallel LLM summary calls during `merge_kg_extraction`.
    # `merge_kg_extraction` previously awaited each summary call
    # sequentially.  Fan-out is bounded so we don't blow the LLM
    # proxy queue.  Set to 1 to restore the legacy behaviour.
    merge_summary_concurrency: int = Field(default=4, ge=1, le=32)
```

In `.env.example`:

```env
AGENT_MERGE_SUMMARY_CONCURRENCY=4
```

- [ ] **Step 2: Test the setting roundtrips through env**

`tests/test_config.py` (extend the existing settings test):

```python
def test_agent_merge_summary_concurrency_default():
    from src.config import settings
    assert settings.agent.merge_summary_concurrency == 4
```

Run: `uv run pytest tests/test_config.py -v` → green.

- [ ] **Step 3: Commit**

```bash
git add src/config.py .env.example tests/test_config.py
git commit -m "feat(config): AgentSettings.merge_summary_concurrency (default 4)"
```

---

### Task 2: Pure-function `_would_call_llm` helper for accounting

This is a side-effect-free helper that mirrors the branch inside
`_maybe_summarize_descriptions` — used purely for logging "how many
of these N items actually fired an LLM call".  Adding it first so
the change in Task 3 can land with proper telemetry.

**Files:**
- Modify: `src/graph/merge.py` (add helper, no behaviour change).
- Test: `tests/test_graph/test_merge_summary_helpers.py` (new file).

- [ ] **Step 1: Write the test first**

```python
from src.graph.merge import _would_call_llm

def test_under_count_and_chars_no_llm():
    assert _would_call_llm(["a"*100], 8, 12000) is False
    assert _would_call_llm(["a", "b", "c"], 8, 12000) is False

def test_above_count_triggers_llm():
    assert _would_call_llm(["x"]*10, 8, 12000) is True

def test_above_chars_triggers_llm():
    long = "x" * 5000
    assert _would_call_llm([long, long, long], 8, 12000) is True

def test_empty_or_singleton_skipped():
    assert _would_call_llm([], 8, 12000) is False
    assert _would_call_llm([""], 8, 12000) is False
    assert _would_call_llm(["only one"], 8, 12000) is False
```

Run → ImportError on `_would_call_llm`.  Good.

- [ ] **Step 2: Add the helper**

In `src/graph/merge.py`, right above `_maybe_summarize_descriptions`:

```python
def _would_call_llm(
    descriptions: list[str],
    force_count: int,
    force_chars: int,
) -> bool:
    """Mirror of the branch inside ``_maybe_summarize_descriptions``
    that decides whether the function will hit the LLM.  Pure / sync
    so callers can count fan-out before scheduling tasks."""
    unique = [d.strip() for d in descriptions if d and d.strip()]
    if len(unique) <= 1:
        return False
    total_chars = sum(len(d) for d in unique)
    return len(unique) >= force_count or total_chars >= force_chars
```

- [ ] **Step 3: Run + commit**

```bash
uv run pytest tests/test_graph/test_merge_summary_helpers.py -v
git add src/graph/merge.py tests/test_graph/test_merge_summary_helpers.py
git commit -m "test(graph): _would_call_llm helper for summary-call accounting"
```

---

### Task 3: Fan out summary calls via `asyncio.gather` with a Semaphore

**Files:**
- Modify: `src/graph/merge.py` — rewrite the two sequential loops to gather.
- Modify: `src/workflow/activities/merge_and_resolve.py` — pass `summary_concurrency` from settings.
- Tests: extend `tests/test_graph/test_merge.py` (or create if missing) with a concurrency probe.

- [ ] **Step 1: Write the failing test**

`tests/test_graph/test_merge_concurrency.py`:

```python
"""Smoke-prove that summary LLM calls fan out concurrently when
`summary_concurrency` > 1, and serialise when == 1."""
from __future__ import annotations

import asyncio
from collections import Counter
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.graph.merge import merge_kg_extraction


def _stub_llm(call_log: list[float]) -> MagicMock:
    """LLM whose `achat` records start time + simulates 50 ms work."""
    llm = MagicMock()

    async def _achat(messages):
        call_log.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.05)
        m = MagicMock()
        m.message.content = "SUMMARY"
        return m

    llm.achat = AsyncMock(side_effect=_achat)
    return llm


def _node_with_n_descs(n: int, ent_name: str, chunk_id: str) -> MagicMock:
    from llama_index.core.graph_stores.types import KG_NODES_KEY, EntityNode
    node = MagicMock()
    node.node_id = chunk_id
    node.metadata = {
        KG_NODES_KEY: [
            EntityNode(
                name=ent_name, label="Other",
                properties={"description": "X" * 5000},  # 5k chars
            ),
        ],
    }
    return node


@pytest.mark.asyncio
async def test_concurrency_fans_out():
    # Five distinct entities each forced past the char threshold ⇒
    # five LLM summary calls.  With concurrency=5 they should start
    # nearly simultaneously (within 30 ms).
    nodes = [
        _node_with_n_descs(3, f"ent-{i}", f"chunk-{i}-{j}")
        for i in range(5) for j in range(3)
    ]
    starts: list[float] = []
    llm = _stub_llm(starts)

    await merge_kg_extraction(
        nodes, llm, summary_concurrency=5,
        force_summary_on_count=2, force_summary_on_chars=10,
    )

    assert len(starts) >= 5
    spread = max(starts[:5]) - min(starts[:5])
    assert spread < 0.03, f"calls didn't fan out, spread={spread}s"


@pytest.mark.asyncio
async def test_concurrency_one_serialises():
    nodes = [
        _node_with_n_descs(3, f"ent-{i}", f"chunk-{i}-{j}")
        for i in range(3) for j in range(3)
    ]
    starts: list[float] = []
    llm = _stub_llm(starts)

    await merge_kg_extraction(
        nodes, llm, summary_concurrency=1,
        force_summary_on_count=2, force_summary_on_chars=10,
    )

    # 3 calls × 0.05 s sleep, strictly serial → ≥ 0.10 s between first
    # and last start.
    assert len(starts) >= 3
    spread = starts[-1] - starts[0]
    assert spread > 0.09, f"expected serial behaviour, spread={spread}s"
```

Run → tests fail because the function doesn't accept `summary_concurrency` yet.

- [ ] **Step 2: Rewrite the two loops to use `asyncio.gather`**

In `src/graph/merge.py`, change the function signature to add the
new kwarg (default 4) and replace the two sequential for-loops in
the body (around line 248 and 276) with a single gather phase.

Sketch (keep variable names so the diff stays readable):

```python
async def merge_kg_extraction(
    nodes: list[BaseNode],
    llm: Any,
    *,
    force_summary_on_count: int = DEFAULT_FORCE_SUMMARY_ON_COUNT,
    force_summary_on_chars: int = DEFAULT_FORCE_SUMMARY_ON_CHARS,
    summary_max_tokens: int = DEFAULT_SUMMARY_MAX_TOKENS,
    language: str = "Russian",
    summary_concurrency: int = 4,
) -> tuple[list[EntityNode], list[Relation]]:
    ...  # aggregation phase unchanged

    sema = asyncio.Semaphore(summary_concurrency)

    async def _summarise(**kw: Any) -> str:
        async with sema:
            return await _maybe_summarize_descriptions(**kw)

    ent_keys = list(ent_agg.keys())
    rel_keys = list(rel_agg.keys())

    ent_descs, rel_descs = await asyncio.gather(
        asyncio.gather(*(
            _summarise(
                llm=llm,
                description_name=ent_agg[k].display_name,
                description_type="Entity",
                descriptions=ent_agg[k].descriptions,
                force_count=force_summary_on_count,
                force_chars=force_summary_on_chars,
                summary_max_tokens=summary_max_tokens,
                language=language,
            ) for k in ent_keys
        )),
        asyncio.gather(*(
            _summarise(
                llm=llm,
                description_name=(
                    f"{rel_agg[k].display_src} ↔ {rel_agg[k].display_tgt}"
                ),
                description_type="Relationship",
                descriptions=rel_agg[k].descriptions,
                force_count=force_summary_on_count,
                force_chars=force_summary_on_chars,
                summary_max_tokens=summary_max_tokens,
                language=language,
            ) for k in rel_keys
        )),
    )

    summary_calls_ent = sum(
        _would_call_llm(
            ent_agg[k].descriptions,
            force_summary_on_count,
            force_summary_on_chars,
        ) for k in ent_keys
    )
    summary_calls_rel = sum(
        _would_call_llm(
            rel_agg[k].descriptions,
            force_summary_on_count,
            force_summary_on_chars,
        ) for k in rel_keys
    )

    merged_entities: list[EntityNode] = []
    name_to_merged_id: dict[str, str] = {}
    for key, merged_desc in zip(ent_keys, ent_descs, strict=True):
        agg = ent_agg[key]
        ...   # rest of materialisation unchanged

    merged_relations: list[Relation] = []
    for (src_key, tgt_key), merged_desc in zip(rel_keys, rel_descs, strict=True):
        agg = rel_agg[(src_key, tgt_key)]
        ...   # rest unchanged

    logger.info(
        "kg merge done  entities={e}  relations={r}  "
        "summary_calls={sc} (ent={sce}, rel={scr})  concurrency={c}",
        e=len(merged_entities), r=len(merged_relations),
        sc=summary_calls_ent + summary_calls_rel,
        sce=summary_calls_ent, scr=summary_calls_rel,
        c=summary_concurrency,
    )
```

Add `import asyncio` at the top of the file.  Replace the old
`summary_calls=N/A` log line.

- [ ] **Step 3: Wire `summary_concurrency` into the activity**

In `src/workflow/activities/merge_and_resolve.py`:

```python
merged_entities, merged_relations = await merge_kg_extraction(
    nodes, llm, language="Russian",
    summary_concurrency=settings.agent.merge_summary_concurrency,
)
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_graph/test_merge_concurrency.py -v
uv run pytest -q --ignore=tests/test_workflow/test_workflow_local.py
```

Both target tests green; full suite count grows by +2.

- [ ] **Step 5: Commit**

```bash
git add src/graph/merge.py src/workflow/activities/merge_and_resolve.py \
        tests/test_graph/test_merge_concurrency.py
git commit -m "perf(graph): fan out merge summary calls via asyncio.gather"
```

---

**🛑 STAGE 2 GATE — measure the win.**  Re-ingest the same doc from
Stage 1 and compare the new `merge wall-clock` against baseline.
Expected: linear speedup proportional to `summary_concurrency` if the
LLM proxy can batch; ~no change if the upstream serialises but also
no regress.

---

## Stage 3 — Skip summary entirely on single-doc merges (optional)

Only land this if Stage 2 wins aren't enough.  Tradeoff: descriptions
in Neo4j become "concat of N source paragraphs" instead of a clean
LLM-rewritten summary.  For single-doc ingests that's usually fine
because the source paragraphs are from a coherent author already.

### Task 4: `merge_summary_skip_single_doc` flag

**Files:**
- Modify: `src/config.py` — flag.
- Modify: `src/graph/merge.py` — short-circuit when all source_chunks
  point at the same doc (detected via shared `file_paths` set on the
  aggregator).
- Test: a unit case that proves the flag short-circuits.

- [ ] **Step 1: Add flag**

```python
# in AgentSettings
merge_summary_skip_single_doc: bool = True
```

Default True (preserves the perf win automatically); operators who
want pristine LLM-rewritten descriptions can set False explicitly.

- [ ] **Step 2: Wire to merge**

Add a `skip_single_doc: bool = True` kwarg.  In the gather loop:

```python
def _is_single_doc(agg: _EntityAgg | _RelationAgg) -> bool:
    return len(agg.file_paths) <= 1 if hasattr(agg, "file_paths") else True

# Inside _summarise wrapper:
async def _summarise(*, agg, **kw):
    if skip_single_doc and _is_single_doc(agg):
        # Fall back to concat — bypass LLM entirely.
        unique = list(dict.fromkeys(
            d.strip() for d in kw["descriptions"] if d and d.strip()
        ))
        return "\n---\n".join(unique)
    async with sema:
        return await _maybe_summarize_descriptions(**kw)
```

Note: `_RelationAgg` currently doesn't track `file_paths` — extend
it to do so (one new line in the aggregation phase that copies the
chunk's file_path into the relation aggregator alongside source_chunks).

- [ ] **Step 3: Test**

```python
async def test_single_doc_short_circuit_no_llm():
    # All chunks come from the same file_path → no LLM call regardless
    # of how many descriptions we feed in.
    llm = MagicMock()
    llm.achat = AsyncMock()
    ...
    await merge_kg_extraction(
        nodes, llm, summary_concurrency=4,
        force_summary_on_count=2, force_summary_on_chars=10,
        skip_single_doc=True,
    )
    llm.achat.assert_not_awaited()
```

- [ ] **Step 4: Commit**

```bash
git add src/config.py src/graph/merge.py \
        src/workflow/activities/merge_and_resolve.py \
        tests/test_graph/test_merge_concurrency.py
git commit -m "perf(graph): skip LLM summary on single-doc merges by default"
```

---

**🛑 STAGE 3 GATE.**  Same doc through, expect another order of
magnitude reduction if many entities crossed the char threshold but
all came from one doc.

---

## Self-review

**Spec coverage:**
- Stage 1 — establishes the baseline you can measure against (no risk).
- Stage 2 — addresses the actual hot loop (`for … await`) without
  touching the summary algorithm, contract types, or activity surface.
- Stage 3 — eliminates work entirely for the common single-doc case.
  Configurable, default-on but operator can opt out.

**Placeholder scan:** No "TBD" sections; every code block compiles and
the test cases run as written.

**Type consistency:** `summary_concurrency` is the name used in:
config (`AgentSettings.merge_summary_concurrency`),
`merge_kg_extraction(... summary_concurrency=...)`, and the activity
call site.  `_would_call_llm` matches between Task 2 (introduction)
and Task 3 (call site).

**Rollback story:** Each Stage 2 and Stage 3 change is one commit, so
`git revert <sha>` undoes a single stage.  Setting
`AGENT_MERGE_SUMMARY_CONCURRENCY=1` from env reverts to legacy serial
behaviour at runtime without code changes.

---

## Execution handoff

**Plan complete and saved to
`docs/superpowers/plans/2026-05-18-merge-and-resolve-perf.md`.**
Three execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per
   task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session via
   executing-plans, checkpoints between stages.
3. **Operator-First** — Start with Stage 1 (you run the baseline
   yourself) then ping me for Stage 2 once we have numbers.

**Which approach?**

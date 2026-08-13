# Optional Synthesis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller ask the search surface for retrieved material without paying for the final large-model write-up.

**Architecture:** Every search workflow already returns everything except the prose — `SearchOutcome` carries `sources`, `documents`, `citations` and `step_stats`, and `SearchResponse` already projects sources and documents onto the HTTP shape. Only `answer` comes from the synthesis step. So this is a short-circuit, not a restructuring: a request flag threads to the workflow, which skips one activity and returns `answer=""`.

**Tech Stack:** FastAPI, Temporal workflows, FastMCP, pydantic, pytest.

## Why this exists

The MCP-2 and MCP-3 tool surfaces return data and let the calling agent write the answer. The HTTP search routes cannot: synthesis is step 4 of the orchestrator, pinned to the large-model queue, with no way to skip it. An agent that wants chunks pays for a paragraph it will discard, and waits on the slowest model in the stack to produce it.

This is step 3 of `docs/ROADMAP-contours.md`. It blocks nothing, but every client that ships against the current behaviour makes it more expensive to change later.

## Global Constraints

- **Backward compatible by default.** `synthesize` defaults to `True`; an existing caller sees no change in behaviour or response shape.
- When synthesis is skipped, `answer` is `""` — not a placeholder sentence, not `None`. The response model keeps `answer: str`.
- Everything else in the response is populated exactly as it would have been: skipping synthesis must not change retrieval, reranking, coverage, or which sources come back.
- No new endpoint. No change to `SearchResponse`'s field set.
- The flag is uniform across local, global, drift and auto. Global's material is already reduced by its map stage before synthesis, so skipping the final call does not risk an unbounded payload.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.
- Do not change retrieval, reranking, the coverage loop, or `synthesize_answer` itself.

---

## File Structure

**Modify:**
- `src/models/search.py` — `SearchRequest.synthesize`
- `src/workflow/contracts.py` — the same flag on `OrchestratorParams` and `GlobalSearchParams`
- `src/workflow/search/orchestrator.py` — skip step 4 when false
- `src/workflow/search/global_wf.py` — same for the global/drift reduce
- `src/api/routes/search_v2.py` — thread the flag through both param builders
- `src/mcp/search_server.py` — expose it on the four MCP-1 tools
- `docs/runbook/mcp.md`, `docs/SEARCH.md` — document it

**Test:**
- `tests/test_workflow/test_search_orchestrator.py`, `tests/test_workflow/test_search_global.py`
- `tests/test_api/test_search_v2_synthesize.py` (create)
- `tests/test_mcp/test_search_server_synthesize.py` (create)

---

### Task 1: The flag, and the local short-circuit

**Files:**
- Modify: `src/models/search.py`, `src/workflow/contracts.py`, `src/workflow/search/orchestrator.py`, `src/api/routes/search_v2.py`
- Test: `tests/test_api/test_search_v2_synthesize.py` (create), `tests/test_workflow/test_search_orchestrator.py`

**Interfaces:**
- Produces: `SearchRequest.synthesize: bool = True`; `OrchestratorParams.synthesize: bool = True`; `_local_params` threads it.

- [ ] **Step 1: Write the failing tests**

Read `tests/test_workflow/test_search_orchestrator.py` first and follow its existing style for driving the workflow — do not invent a new harness.

Create `tests/test_api/test_search_v2_synthesize.py`:

```python
"""The `synthesize` flag on /api/v1/search/*.

Asserts the flag reaches the workflow parameters; the workflow's own
short-circuit is covered in tests/test_workflow/.
"""

from __future__ import annotations

from src.api.routes.search_v2 import _local_params
from src.models.search import SearchRequest


def test_synthesize_defaults_to_true():
    assert SearchRequest(query="q").synthesize is True


def test_local_params_carry_the_flag():
    assert _local_params(SearchRequest(query="q")).synthesize is True
    assert _local_params(SearchRequest(query="q", synthesize=False)).synthesize is False
```

In `tests/test_workflow/test_search_orchestrator.py`, add a test that the workflow skips the synthesis activity when the flag is false and still returns the retrieved sources. Mirror how the existing tests in that file stub activities; assert both that `synthesize_answer` was never invoked and that `outcome.answer == ""` while `outcome.sources` is non-empty.

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_api/test_search_v2_synthesize.py tests/test_workflow/test_search_orchestrator.py -v`
Expected: FAIL — `SearchRequest` has no field `synthesize`.

- [ ] **Step 3: Add the field**

`src/models/search.py`, on `SearchRequest`, beside `answer_template`:

```python
    # When false the final large-model synthesis is skipped and `answer`
    # comes back empty; everything else in the response is unchanged.
    # For callers that compose their own answer from `sources` and only
    # pay for retrieval.
    synthesize: bool = Field(
        default=True,
        description="Run the final answer synthesis (default). False returns retrieval only.",
    )
```

`src/workflow/contracts.py`, on `OrchestratorParams`: `synthesize: bool = True`.

- [ ] **Step 4: Short-circuit the orchestrator**

In `src/workflow/search/orchestrator.py`, guard step 4. Keep the reranking that precedes it — the sources it selects are returned either way — and skip only the activity:

```python
        if params.synthesize:
            self._state["phase"] = "synthesize"
            synth_queue, synth_params = build_synthesize_call(...)
            synth: SynthesizeResult = await workflow.execute_activity(...)
        else:
            # Retrieval-only: the caller composes its own answer from
            # `sources`. Everything else in the outcome is unchanged.
            self._state["phase"] = "skip-synthesize"
            synth = SynthesizeResult(text="")
```

Check `SynthesizeResult`'s definition before writing that constructor — if its other fields lack defaults, build it with whatever empty values its shape requires rather than assuming.

- [ ] **Step 5: Thread it through the route**

In `src/api/routes/search_v2.py`, `_local_params` passes `synthesize=req.synthesize`.

- [ ] **Step 6: Run the tests, lint, commit**

```bash
uv run pytest tests/test_api/test_search_v2_synthesize.py tests/test_workflow/test_search_orchestrator.py -v
uv run ruff check src/models/search.py src/workflow/contracts.py src/workflow/search/orchestrator.py src/api/routes/search_v2.py
git commit -m "feat(search): add a synthesize flag and short-circuit the local flow"
```

---

### Task 2: Global, drift and auto

**Files:**
- Modify: `src/workflow/contracts.py`, `src/workflow/search/global_wf.py`, `src/api/routes/search_v2.py`
- Test: `tests/test_workflow/test_search_global.py`, `tests/test_api/test_search_v2_synthesize.py`

**Interfaces:**
- Consumes: Task 1's `SearchRequest.synthesize`.
- Produces: `GlobalSearchParams.synthesize: bool = True`; `_global_params` threads it.

**Also fixes a finding from the Task 1 review.** `_drift_local_fallback`
(`src/workflow/search/router_wf.py:86-90`) and the `except` branch of
`DriftSearchWorkflow.run` (`:146-150`) return the *local* outcome relabelled
`mode="drift"` when the global expansion pass fails. After Task 1, a caller
who sets `synthesize=False` on `/search/drift` gets a real answer on the happy
path (the global leg still defaults to `True`) but `answer=""` on the
degradation path — the same request behaving two different ways depending on
whether an internal step failed. Threading the flag through global/drift is
what makes the two paths agree; there must be a test pinning that they do.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_workflow/test_search_global.py`, following that file's existing stubbing style: with `synthesize=False`, the global flow does not invoke the synthesis activity, returns `answer == ""`, and still returns its reduce sources and `step_stats`.

Add a drift test pinning consistency across the fallback: with `synthesize=False`, the happy path and the global-failure fallback must agree on whether `answer` is empty. With `synthesize=True` (the default), the fallback must still carry a real answer exactly as it does today.

Add to `tests/test_api/test_search_v2_synthesize.py`:

```python
def test_global_params_carry_the_flag():
    from src.api.routes.search_v2 import _global_params

    assert _global_params(SearchRequest(query="q")).synthesize is True
    assert _global_params(
        SearchRequest(query="q", synthesize=False),
    ).synthesize is False


def test_drift_params_carry_the_flag():
    from src.api.routes.search_v2 import _global_params

    p = _global_params(SearchRequest(query="q", synthesize=False), drift_mode=True)
    assert p.synthesize is False
    assert p.drift_mode is True
```

- [ ] **Step 2: Run to verify they fail**

`uv run pytest tests/test_workflow/test_search_global.py tests/test_api/test_search_v2_synthesize.py -v`

- [ ] **Step 3: Implement**

Add `synthesize: bool = True` to `GlobalSearchParams`, guard the reduce activity in `src/workflow/search/global_wf.py` the same way Task 1 guarded the orchestrator, and thread the flag in `_global_params`.

Check how `AutoSearchWorkflow` (`src/workflow/search/router_wf.py`) builds the params it dispatches with — the flag must survive routing to whichever mode is chosen. If the router constructs params itself rather than forwarding them, thread it there too.

- [ ] **Step 4: Run the tests, lint, commit**

Run the two files above plus `uv run pytest tests/test_workflow tests/test_api -q` to confirm no regression.

```bash
git commit -m "feat(search): honour the synthesize flag in global, drift and auto"
```

---

### Task 3: MCP-1 surface and docs

**Files:**
- Modify: `src/mcp/search_server.py`, `docs/runbook/mcp.md`, `docs/SEARCH.md`
- Test: `tests/test_mcp/test_search_server_synthesize.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_mcp/test_search_server_synthesize.py` asserting that `_local_params` / `_global_params` in `src/mcp/search_server.py` carry the flag, mirroring the conventions in `tests/test_mcp/`. Read those helpers first — MCP-1 has its own copies, separate from the FastAPI ones.

- [ ] **Step 2: Implement**

Add a `synthesize: bool = True` argument to the four MCP-1 tools and thread it into their param builders. Extend each tool's docstring with one sentence: passing `false` returns the retrieved sources with an empty `answer`, for when the client writes its own answer and does not want to pay for the model call.

- [ ] **Step 3: Docs**

- `docs/runbook/mcp.md` — document the flag in the MCP-1 section, and say plainly what it is for: a client that composes its own answer stops paying for a synthesis it discards.
- `docs/SEARCH.md` — document the request field. Check whether that file lists `SearchRequest`'s fields; if it does not, add the flag wherever the request shape is actually described, and say so in your report.

- [ ] **Step 4: Full check and commit**

```bash
uv run pytest tests/test_mcp tests/test_api tests/test_workflow -q
uv run ruff check src/mcp/search_server.py tests/test_mcp/test_search_server_synthesize.py
git commit -m "feat(mcp): expose the synthesize flag on the MCP-1 search tools"
```

---

## Verification

With the branch in place, `POST /api/v1/search/local` with `{"query": "...", "synthesize": false}` returns `answer: ""`, a populated `sources` array, and a latency that no longer includes a large-model call. The same request without the field behaves exactly as it does today.

## Notes for the implementer

- The point of the default is that nobody has to change anything. If any existing test needs editing to keep passing, that is a signal the change is not backward compatible — stop and report it rather than adjusting the test.
- `answer=""` is the contract. Do not substitute "Synthesis skipped" or similar; a caller checking truthiness must see nothing.

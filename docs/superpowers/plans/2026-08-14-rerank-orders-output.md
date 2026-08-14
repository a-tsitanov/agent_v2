# Rerank Orders the Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the reranker's verdict reach the caller. Today it picks the best chunks for the synthesis prompt and its ordering is thrown away; `SearchOutcome.sources` has always been the raw unranked merged pool.

**Architecture:** The cross-encoder already scores every chunk in the pool — `top_n` only controls how many it hands back. So asking for the whole pool costs nothing extra. The orchestrator requests all of them ranked, returns that as `sources`, and caps separately for the synthesis prompt using the `cap_synth_sources` helper that already exists for exactly that.

**Tech Stack:** Temporal workflows, LlamaIndex cross-encoder postprocessor, pytest.

## The problem in one paragraph

Search merges graph and vector hits into a pool of perhaps fifty chunks in arbitrary order. `rerank_sources` scores them all against the query and returns the best five (`settings.temporal.rerank_top_n`). Those five become the synthesis context. But `SearchOutcome.sources` is set from `merged` — the raw fifty — so the caller receives an unordered pile while the system's own judgement about which chunks matter stays inside the workflow. An agent composing its own answer is the case where this hurts most: it gets the pile and none of the ranking.

## Global Constraints

- **Nothing is lost.** The caller still receives every chunk it receives today — the same set, reordered. Not the top-N, not a truncation. A client relying on the full pool must keep working.
- The synthesis prompt keeps its existing cap. Only the *returned* ordering changes.
- The rerank now runs unconditionally, including when `synthesize=False` — its output is the returned order, so it is no longer discardable work. The guard added in the previous branch comes off.
- Degradation is unchanged in kind: if the reranker is unavailable the activity already returns the pool untouched, which under this change means the full pool in retrieval order — the status quo ante. Fail-open must stay fail-open.
- `RerankParams`' contract does not change. `top_n` keeps meaning "how many to return"; the orchestrator simply asks for all of them.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.

---

### Task 1: Return the ranked pool, cap only for synthesis

**Files:**
- Modify: `src/workflow/search/orchestrator.py`, `src/workflow/contracts.py` (comment only)
- Test: `tests/test_workflow/test_search_orchestrator_synthesize.py`, `tests/test_workflow/test_search_orchestrator.py`

**Interfaces:** no signature changes. `rerank_sources` is called with `top_n=len(pool)`; `SearchOutcome.sources` becomes the activity's ranked output; `cap_synth_sources(ranked, settings.temporal.rerank_top_n)` feeds synthesis.

- [ ] **Step 1: Confirm the premise before changing anything**

Read `src/workflow/search/activities/rerank.py`. Verify that `get_reranker(top_n)` and `postprocess_nodes` truncate to `top_n` and that passing the pool size therefore returns everything ranked, with the same model work. Verify `apply_group_weights` re-sorts whatever it is given and does not assume a short list. Verify `rerank_sources` has exactly one caller (`orchestrator.py:317`).

If any of that turns out false, stop and report — the whole plan rests on it.

- [ ] **Step 2: Write the failing tests**

Follow the harness in `tests/test_workflow/test_search_orchestrator_synthesize.py` (`_start_env()` / `WorkflowEnvironment.start_time_skipping`) — do not invent another.

Three properties, with a rerank stub that returns its input reordered:

1. **Ordering reaches the caller.** `SearchOutcome.sources` comes back in the stub's order, not the merged order.
2. **Nothing is lost.** The returned set equals the merged set — same chunk_ids, same count. Feed a pool larger than `rerank_top_n` so a truncation bug cannot hide; assert the count explicitly.
3. **Synthesis still gets the cap.** The sources handed to `synthesize_answer` are the first `rerank_top_n` of the ranked order, not the whole pool.

Plus: with `synthesize=False`, the rerank activity IS invoked (the inverse of what the previous branch asserted) and `sources` is still the full ranked pool.

Check whether the previous branch left a test asserting the rerank is skipped when `synthesize=False`. If so it now encodes the old intent and must be replaced, not deleted silently — say so in your report.

- [ ] **Step 3: Run to verify they fail**

`uv run pytest tests/test_workflow/test_search_orchestrator_synthesize.py tests/test_workflow/test_search_orchestrator.py -v`

- [ ] **Step 4: Implement**

In `src/workflow/search/orchestrator.py`:
- Take the rerank block back out of the `if params.synthesize:` guard so it always runs.
- Pass `top_n=len(merged)` to `RerankParams` so the activity returns the whole pool ranked. Keep the existing fail-open `try/except` exactly as it is.
- Name the result something honest — it is no longer "synth sources". Use it for `SearchOutcome.sources`.
- Feed synthesis `cap_synth_sources(ranked, settings.temporal.rerank_top_n)`.

Update the comment on `OrchestratorParams.synthesize` (`src/workflow/contracts.py`, around line 495): reranking now runs on both paths and the returned sources are ranked either way. That comment has been corrected twice already — make it match the code this time.

- [ ] **Step 5: Check the payload**

The activity's result grows from `rerank_top_n` nodes to the whole pool. Its *input* already carries the whole pool, so the round trip is symmetric — but confirm there is no Temporal payload limit this could cross at a realistic pool size, and record what you checked. If there is a limit, report it rather than silently capping.

- [ ] **Step 6: Run, lint, commit**

```bash
uv run pytest tests/test_workflow tests/test_api tests/test_mcp -q
uv run ruff check src/workflow/search/orchestrator.py src/workflow/contracts.py
git commit -m "feat(search): return the reranked ordering to the caller"
```

---

### Task 2: Documentation

**Files:** `docs/SEARCH.md`, `docs/runbook/search-usage.md`

- [ ] **Step 1: Correct the claim that the displayed pool is unranked**

`docs/SEARCH.md` §4 describes the rerank, and a nearby passage states that the displayed `SearchOutcome.sources` stay the FULL merged pool with only the synthesis context trimmed. Half of that is still true — the pool is still full — and half is now wrong: it is ranked. Find the exact passage, read it, and correct only what changed.

- [ ] **Step 2: Note it where a caller looks**

`docs/runbook/search-usage.md` describes the practical request/response contract. Say plainly that `sources` comes back best-first, and that this holds with `synthesize=false` too — that is the case where it matters most, since the caller is doing its own reading.

Check whether that file's mode table mentions `TEMPORAL_RERANK_TOP_N` as determining what the caller receives; if it does, it is now wrong in a second way and should say the setting bounds the synthesis context only.

- [ ] **Step 3: Commit**

```bash
git commit -m "docs(search): sources now come back ranked"
```

---

## Verification

A `synthesize=false` request returns the same chunk_ids it returns today, best-first instead of arbitrary, with an empty `answer`. A default request is unchanged except that `sources` is ordered.

## Notes for the implementer

- The temptation is to return `top_n` results because that is what the reranker was built to do. Resist it: dropping chunks the caller receives today is the one thing this change must not do.
- If an existing test needs editing, distinguish two cases in your report: a test that encoded the old *intent* (rerank skipped when synthesis is skipped) legitimately changes; a test that merely broke is a signal something is wrong.

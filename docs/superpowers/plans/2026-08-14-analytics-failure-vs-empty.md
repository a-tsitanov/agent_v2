# Distinguish "could not compute" from "computed zero" Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stop the analytics layer from reporting a backend limitation as a factual zero.

**Architecture:** Two independently sensible decisions cancel each other out. `NebulaGraphStore.structured_query` raises `NotImplementedError` on a non-empty `param_map` — deliberately, "fail loud rather than silently dropping caller-supplied params" (`src/graph/nebula_store.py:264-272`). `run_rows` catches every exception and returns `[]` — also deliberately, fail-soft for analytics (`src/analytics/store_query.py:23-25`). The loud failure becomes silence, and `[]` is indistinguishable from "the graph genuinely has nothing".

The fix separates two cases that are currently one: a query that *failed this time* (transient — keep failing soft) and an operation this backend *can never perform* (structural — must be reported).

**Tech Stack:** Temporal activities, pydantic contracts, pytest.

## Why it matters

Confirmed by reading the code: on the production `nebula` backend, `topic_trend` (`src/analytics/primitives/dynamics.py:113-127`) passes `{"topic": topic}` to `run_rows` and therefore always returns `[]`, for every topic, with no error. It is registered in `CATALOG` (`dynamics.py:176`), so `/api/v1/analyze` and the `kb_analyze` MCP tool reach it.

`synthesize_analytical` already guards the total-failure case — if *no* step has rows it answers "Не удалось вычислить ответ" (`src/workflow/analytics/activities.py:74-76`). The dangerous case is a **mixed plan**: some primitives return real numbers, `topic_trend` returns `[]`, and synthesis proceeds — presenting a backend limitation as a measured zero, next to genuine figures, with equal authority.

Not currently observed in production: zero `analytics query failed` warnings in seven days of worker logs, and `topic_trend` never appears by name, while 22 080 other warnings reach the same sink. The defect is latent, not active. This closes it before it fires.

## Global Constraints

- **Transient failures keep failing soft.** Only a structurally impossible operation — `NotImplementedError` — is promoted to a reported error. A flaky query must not start breaking `/analyze`.
- **Additive contract change only.** `StepResult` gains an optional field with a default; existing consumers keep working and the API response shape stays backward compatible.
- A failed step must be distinguishable from an empty one **in `provenance`**, because that is where callers are told to read numbers from (`docs/` and the openclaw agent's own instructions both say: take figures from provenance rows, not from the prose).
- Do not change what any primitive computes. Do not touch `CATALOG` registration.
- Do not make `/analyze` fail as a whole because one step could not run. A partial answer that says which part is missing beats no answer.
- Ruff: `line-length = 100`, `py312`, `select = ["E","F","I","B","UP","SIM","RUF"]`. pytest `asyncio_mode = "auto"`.

---

### Task 1: Report the failure instead of swallowing it

**Files:**
- Modify: `src/analytics/store_query.py`, `src/analytics/contracts.py`, `src/analytics/provenance.py`, `src/workflow/analytics/activities.py`, `src/analytics/synthesis.py`
- Test: `tests/test_analytics/test_store_query.py`, `tests/test_analytics/` (step/provenance), `tests/test_workflow/test_analytics_activities.py` — read what exists before creating new files

**Interfaces:**
- `StepResult` gains `error: str = ""`.
- `run_rows` re-raises `NotImplementedError`; every other exception keeps returning `[]`.
- `step_from_primitive` passes the error through.

- [ ] **Step 1: Confirm the premise**

Read `src/analytics/store_query.py`, `src/graph/nebula_store.py:264-272`, `src/workflow/analytics/activities.py:42-67` and `src/analytics/synthesis.py`. Confirm that `NotImplementedError` is the exception a nebula backend raises for a parameterised query, that `run_rows` swallows it, and that `execute_step` currently catches only `TypeError`. If any of that is wrong, stop and report.

- [ ] **Step 2: Write the failing tests**

Cover, following the conventions of whatever test files already exist for these modules:

1. `run_rows` re-raises `NotImplementedError` — it does not return `[]`.
2. `run_rows` still returns `[]` on an ordinary exception (e.g. `RuntimeError`), and still logs.
3. `execute_step` catches `NotImplementedError` and returns a `StepResult` whose `error` is non-empty, `rows` empty, and whose `primitive`/`params` are still populated — the step is reported, not dropped.
4. `execute_step` keeps its existing `TypeError` fail-soft behaviour, with `error` left empty — a planner mistake is not a backend failure.
5. The synthesis prompt distinguishes them: a step with `error` set must not be presented to the model as a zero result. Assert on the built prompt, not on model output.
6. The all-steps-failed case does not claim a computed answer. Check what `synthesize_analytical` does today when no step has rows and make the failed case at least as honest.

- [ ] **Step 3: Run to verify they fail**

`uv run pytest tests/test_analytics tests/test_workflow/test_analytics_activities.py -v`

- [ ] **Step 4: Implement**

`src/analytics/store_query.py` — re-raise `NotImplementedError` before the general handler:

```python
    try:
        return await asyncio.to_thread(_run_query, store, cypher, params)
    except NotImplementedError:
        # The backend cannot perform this operation at all — not a transient
        # failure. Swallowing it returns [] , which the caller cannot tell
        # from "the graph genuinely has nothing".  Let it reach execute_step.
        raise
    except Exception as exc:  # fail-soft like analysis.py
        logger.warning("analytics query failed: {e}", e=exc)
        return []
```

`src/analytics/contracts.py` — add `error: str = ""` to `StepResult`, documented as "why this step produced nothing; empty means it ran".

`src/analytics/provenance.py` — carry `error` through `step_from_primitive`.

`src/workflow/analytics/activities.py` — in `execute_step`, catch `NotImplementedError` separately from `TypeError` and build a `StepResult` with the error text. Keep the existing `TypeError` branch as it is.

`src/analytics/synthesis.py` — the prompt builder must render a failed step as "не удалось вычислить: <reason>" rather than as an empty result set, so the model cannot narrate it as zero.

- [ ] **Step 5: Run, lint, commit**

```bash
uv run pytest tests/test_analytics tests/test_workflow tests/test_api -q
uv run ruff check src/analytics src/workflow/analytics/activities.py
git commit -m "fix(analytics): report a backend limitation instead of an empty result"
```

- [ ] **Step 6: Document**

`docs/ANALYTICS-GUIDE.md` or `docs/runbook/graph-analytics.md` — whichever actually describes provenance for an operator; read both and pick. State that a step now carries `error`, and that an empty `rows` with a non-empty `error` means the primitive could not run, not that the answer is zero. One short paragraph.

---

## Verification

An `/api/v1/analyze` request whose plan includes `topic_trend` on the nebula backend returns a provenance step for it with empty `rows` and a populated `error`, and the synthesized answer does not assert that the topic has no trend.

## Notes for the implementer

- The temptation is to make `topic_trend` itself check the backend. Resist it: every primitive passing parameters to nebula has the same fault, and the fix belongs where the two safety mechanisms meet, not in one caller.
- Do not "fix" `NebulaGraphStore.structured_query` to bind params. That is a real feature (Phase 2 in its own comment) and far outside this change.

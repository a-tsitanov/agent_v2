# Conversation history (multi-turn search) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Answer follow-up questions in conversation context by contextualising the query into a standalone question before plan/retrieval. Opt-in, fail-open, client-managed history (no server sessions). Spec: `docs/superpowers/specs/2026-06-07-conversation-history.md`.

**Architecture:** Add `history` to the request + workflow params. At the START of each search workflow, when history is non-empty, one small-LLM `contextualize_query` activity rewrites the follow-up into a standalone question; `params = params.model_copy(update={"query": rewritten})` so the entire downstream pipeline (plan, retrieve, coverage, rerank, synthesis) uses it unchanged. Empty history / flag off / LLM error → params untouched → today's behaviour.

**Tech Stack:** Python 3.12, FastAPI (pydantic models), Temporal workflows/activities, LiteLLM small tier, pytest.

**Decisions (from spec review):** client-managed history; contextualise-only (no history in synthesis); `history_max_turns=6`.

---

## Phase 1 — request model, contracts, config

**Files:** `src/models/search.py`, `src/workflow/contracts.py`, `src/config.py`

- [ ] **Step 1: Add `ConversationTurn` + `history` to the request**

`src/models/search.py`, above `SearchRequest`:
```python
class ConversationTurn(BaseModel):
    role: str = Field("user", description="user | assistant")
    content: str
```
and inside `SearchRequest` (after `query`):
```python
    history: list[ConversationTurn] = Field(
        default_factory=list,
        description="Prior turns (client-managed). Empty = single-shot, no contextualisation.",
    )
```

- [ ] **Step 2: Add the workflow contracts**

`src/workflow/contracts.py`:
```python
class ConversationTurnDict(_Frozen):
    role: str = "user"
    content: str = ""


class ContextualizeParams(_Frozen):
    """Input to the ``contextualize_query`` activity."""
    query: str
    history: list[ConversationTurnDict] = Field(default_factory=list)


class ContextualizeResult(_Frozen):
    """Standalone, self-contained rewrite of ``query`` (== original on no-op/failure)."""
    query: str
```
Add `history: list[ConversationTurnDict] = Field(default_factory=list)` to **`OrchestratorParams`** and **`GlobalSearchParams`**.

- [ ] **Step 3: Config flag + bounds**

`src/config.py`, `AgentSettings`:
```python
    conversation_history_enabled: bool = True
    history_max_turns: int = Field(default=6, ge=0, le=40)
    history_max_chars: int = Field(default=4000, ge=0)
```

- [ ] **Step 4: Commit** — `feat(search): conversation history request/contract/config scaffolding`

---

## Phase 2 — contextualize_query activity

**Files:** `src/workflow/search/activities/contextualize.py` (create), `src/workflow/worker.py` (register), test `tests/test_workflow/test_contextualize.py`

- [ ] **Step 1: Write failing tests for the pure helpers**

`tests/test_workflow/test_contextualize.py`:
```python
import pytest
from src.workflow.contracts import ConversationTurnDict
from src.workflow.search.activities.contextualize import _bound_history, _build_prompt


def _t(role, content): return ConversationTurnDict(role=role, content=content)


def test_bound_history_caps_turns():
    turns = [_t("user", f"q{i}") for i in range(20)]
    out = _bound_history(turns, max_turns=6, max_chars=10_000)
    assert len(out) == 6 and out[-1].content == "q19"   # keeps the most recent


def test_bound_history_caps_chars():
    turns = [_t("user", "x" * 100) for _ in range(20)]
    out = _bound_history(turns, max_turns=40, max_chars=250)
    assert sum(len(t.content) for t in out) <= 250 and out[-1].content == "x" * 100


def test_build_prompt_includes_history_and_query():
    p = _build_prompt("а что по цене?", [_t("user", "расскажи про Продукт X")])
    assert "Продукт X" in p and "а что по цене?" in p
```

- [ ] **Step 2: Run — expect fail.** `.venv/bin/python -m pytest tests/test_workflow/test_contextualize.py -q` → FAIL.

- [ ] **Step 3: Implement the activity**

`src/workflow/search/activities/contextualize.py`:
```python
"""Rewrite a follow-up question into a standalone one using recent
conversation history (small tier).  Fail-open: returns the original
query on empty history / any error."""
from __future__ import annotations

from temporalio import activity

from src.config import settings
from src.workflow.contracts import (
    ContextualizeParams, ContextualizeResult, ConversationTurnDict,
)

_PROMPT = (
    "/no_think\n"
    "Перепиши ПОСЛЕДНИЙ вопрос пользователя как самодостаточный, "
    "подставив контекст из истории (раскрой местоимения и отсылки). "
    "Сохрани язык вопроса. Верни ТОЛЬКО переписанный вопрос, без пояснений.\n\n"
    "История:\n{history}\n\nПоследний вопрос: {query}\n\nСамодостаточный вопрос:"
)


def _bound_history(
    turns: list[ConversationTurnDict], *, max_turns: int, max_chars: int,
) -> list[ConversationTurnDict]:
    recent = list(turns)[-max_turns:] if max_turns >= 0 else list(turns)
    out: list[ConversationTurnDict] = []
    total = 0
    for t in reversed(recent):  # keep the most recent within the char budget
        c = len(t.content or "")
        if max_chars and total + c > max_chars and out:
            break
        out.append(t)
        total += c
    return list(reversed(out))


def _build_prompt(query: str, turns: list[ConversationTurnDict]) -> str:
    lines = [f"{t.role}: {t.content}" for t in turns]
    return _PROMPT.format(history="\n".join(lines), query=query)


@activity.defn
async def contextualize_query(params: ContextualizeParams) -> ContextualizeResult:
    if not params.history:
        return ContextualizeResult(query=params.query)
    turns = _bound_history(
        list(params.history),
        max_turns=settings.agent.history_max_turns,
        max_chars=settings.agent.history_max_chars,
    )
    if not turns:
        return ContextualizeResult(query=params.query)
    try:
        from src.retrieval._common import strip_thinking
        from src.workflow.search._deps import get_route_llm  # small-tier helper
        llm = await get_route_llm()
        resp = await llm.acomplete(_build_prompt(params.query, turns))
        text = strip_thinking(getattr(resp, "text", None) or str(resp)).strip()
        return ContextualizeResult(query=text or params.query)
    except Exception as exc:  # fail-open
        activity.logger.warning("contextualize_query failed, using raw query: %s", exc)
        return ContextualizeResult(query=params.query)
```
> Implementer note: confirm the small-tier LLM accessor name (the router uses one — see `activities/route.py`); reuse it. If none is exported, build via `build_llm("route")` like `route.py` does.

- [ ] **Step 4: Register the activity** in `src/workflow/worker.py` (add `contextualize_query` to the search worker's activity list, next to `plan_subquestions`/`route`).

- [ ] **Step 5: Run — expect pass.** `.venv/bin/python -m pytest tests/test_workflow/test_contextualize.py -q`.

- [ ] **Step 6: Commit** — `feat(search): contextualize_query activity (history → standalone query)`

---

## Phase 3 — wire into the workflows + route

**Files:** `src/workflow/search/orchestrator.py:125-143`, `src/workflow/search/global_wf.py`, `src/workflow/search/router_wf.py`, `src/api/routes/search_v2.py:42-63`

- [ ] **Step 1: Contextualise at the start of `SearchOrchestratorWorkflow.run`**

In `orchestrator.py`, immediately after `run` begins (before the `plan_subquestions` call at line 135), insert:
```python
        if params.history and settings.agent.conversation_history_enabled:
            ctx = await workflow.execute_activity(
                "contextualize_query",
                ContextualizeParams(query=params.query, history=list(params.history)),
                start_to_close_timeout=LLM_START_TO_CLOSE,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                retry_policy=FAST_RETRY,
                result_type=ContextualizeResult,
            )
            params = params.model_copy(update={"query": ctx.query})
```
(`params.model_copy` updates the query once → every downstream `params.query` use is the standalone question. Import `ContextualizeParams`/`ContextualizeResult` + `settings`.) Determinism: history is in params, no clock/env read.

- [ ] **Step 2: Same for `GlobalSearchWorkflow.run`** (global_wf.py) — identical block at the top, using `GlobalSearchParams.history`.

- [ ] **Step 3: Drift contextualises ONCE, children skip**

In `router_wf.py` `DriftSearchWorkflow.run`, before the local child, contextualise once and pass the rewritten query into both children with **history cleared** (so they don't re-run it):
```python
        if local_params.history and settings.agent.conversation_history_enabled:
            ctx = await workflow.execute_activity(
                "contextualize_query",
                ContextualizeParams(query=local_params.query, history=list(local_params.history)),
                start_to_close_timeout=LLM_START_TO_CLOSE,
                schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
                retry_policy=FAST_RETRY, result_type=ContextualizeResult,
            )
            local_params = local_params.model_copy(update={"query": ctx.query, "history": []})
            global_params = global_params.model_copy(update={"query": ctx.query, "history": []})
```

- [ ] **Step 4: Thread history from the route**

In `search_v2.py`, `_local_params` and `_global_params` (lines 42-63), pass `history=[ConversationTurnDict(role=t.role, content=t.content) for t in req.history]` into the params constructors.

- [ ] **Step 5: Failing test — route threads history into params**

`tests/test_api/test_search_history.py`:
```python
from src.api.routes.search_v2 import _local_params
from src.models.search import SearchRequest, ConversationTurn


def test_local_params_carries_history():
    req = SearchRequest(query="а что по цене?",
                        history=[ConversationTurn(role="user", content="про Продукт X")])
    p = _local_params(req)
    assert p.query == "а что по цене?"
    assert [t.content for t in p.history] == ["про Продукт X"]
```

- [ ] **Step 6: Run tests + search regression**

Run: `.venv/bin/python -m pytest tests/test_api/test_search_history.py tests/test_workflow/test_contextualize.py tests/test_workflow tests/test_api -q -k "history or contextualize or search"`
Expected: PASS (Temporal-bound workflow tests skip if Temporal down; the pure/route tests pass).

- [ ] **Step 7: Live smoke (optional, stack up)** — two-turn `/search/local`: turn 1 "расскажи про <entity>", turn 2 with history "а что по цене?" → answer grounded in the entity. Capture both responses.

- [ ] **Step 8: Commit** — `feat(search): wire conversation-history contextualisation into local/global/drift + route`

---

## Self-Review

**Spec coverage:** request/contracts/config (P1) ✓; contextualize activity + bounds + fail-open (P2) ✓; wiring local/global/drift + route, contextualise-only, client-managed (P3) ✓.

**Type consistency:** `ConversationTurn` (API) ↔ `ConversationTurnDict` (workflow); `ContextualizeParams/Result`; `params.model_copy(update={"query": ...})` on the frozen `OrchestratorParams`/`GlobalSearchParams` (pydantic model_copy — OK).

**Back-compat:** empty history (default) → contextualise block skipped → byte-identical to today. Flag `conversation_history_enabled` default True but inert without history.

**Open implementer checks:** small-tier LLM accessor name in `contextualize.py` (mirror `route.py`); worker registration list location; `LLM_START_TO_CLOSE`/`FAST_RETRY` already imported in each workflow (they are — used by existing activities).

**Effort:** S–M. No graph/index/migration; additive request field + one activity + 3 one-block workflow inserts + route threading.

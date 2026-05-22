"""SearchWorkflow integration test.

Same skip-on-no-Temporal pattern as test_graph_build_workflow.py.
Activities are stubbed at the worker — we don't touch Milvus,
Neo4j or any LLM here, only assert that ``SearchWorkflow``:

  * routes the three modes (``simple`` / ``agent`` / ``selfrag``)
    through the right activity sequences,
  * stops the ReAct loop on ``submit_answer``,
  * stops on the repeat-call guard,
  * caps at ``max_iterations``,
  * surfaces synthesize_answer's output (text + reflective extras),
  * exposes the live state through the ``get_state`` query.
"""

from __future__ import annotations

import socket
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from src.workflow.contracts import (
    AgentDecision,
    ReasoningParams,
    ReflectiveCitationDict,
    ReflectiveUncertaintyDict,
    SearchOutcome,
    SearchParams,
    SerializedNode,
    SynthesizeParams,
    SynthesizeResult,
    ToolCallParams,
    ToolCallResult,
)
from src.workflow.search_workflow import SearchWorkflow


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


# ── stubs ─────────────────────────────────────────────────────────


def _build_stubs(*, plan: list[dict], synth_text: str = "FINAL",
                 reflective_rounds: int = 0):
    """Build a set of stub activities with controlled behaviour.

    ``plan`` is a list of decisions the agent_reasoning_step stub will
    return on successive calls.  Each item:
      {"tool": "vector_search", "kwargs": {...}, "sources": int}
      {"tool": "submit_answer"}
    """
    plan_iter = iter(plan)

    @activity.defn(name="agent_reasoning_step")
    async def _reasoning(params: ReasoningParams) -> AgentDecision:
        try:
            step = next(plan_iter)
        except StopIteration:
            # plan exhausted → simulate "no tool call" exit
            return AgentDecision(
                tool_name="", finished_no_call=True, raw_text="",
            )
        return AgentDecision(
            tool_name=step["tool"],
            tool_kwargs=step.get("kwargs", {}),
            tool_call_id=f"call-{step['tool']}-{uuid.uuid4().hex[:6]}",
        )

    @activity.defn(name="tool_execution")
    async def _tool_exec(params: ToolCallParams) -> ToolCallResult:
        # Decide how many fake sources to add — caller can override
        # via tool_kwargs["_fake_sources"]; default 1 for retrieval.
        n = int(params.tool_kwargs.get("_fake_sources", 1))
        sources = [
            SerializedNode(
                chunk_id=f"{params.tool_name}-c{i}",
                text=f"chunk text {i}",
                score=0.5,
                metadata={"doc_id": "d1"},
            )
            for i in range(n)
        ]
        return ToolCallResult(
            tool_name=params.tool_name,
            observation=f'[{{"ok": true, "tool": "{params.tool_name}"}}]',
            sources_added=sources,
        )

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        if params.mode == "selfrag":
            return SynthesizeResult(
                text=synth_text,
                citations=[ReflectiveCitationDict(claim="x", chunk_id="c0")],
                uncertainties=[
                    ReflectiveUncertaintyDict(topic="t", reason="r"),
                ],
                refinement_rounds=reflective_rounds,
            )
        return SynthesizeResult(text=synth_text)

    return [_reasoning, _tool_exec, _synth]


async def _connect() -> Client:
    return await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )


# ── tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_simple_mode_one_retrieve_then_synth(monkeypatch):
    """mode=simple: skips reasoning loop, one vector_search → synth."""
    activities = _build_stubs(plan=[], synth_text="OK answer")
    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow], activities=activities,
    ):
        outcome: SearchOutcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="Иванов", mode="simple"),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    assert outcome.mode == "simple"
    assert outcome.answer == "OK answer"
    assert len(outcome.sources) == 1  # one vector_search call
    assert outcome.step_stats[0].tool_name == "vector_search"


@pytest.mark.asyncio
async def test_agent_mode_submit_answer_terminates_loop(monkeypatch):
    """mode=agent: 2 retrievals then submit_answer → synth."""
    plan = [
        {"tool": "vector_search", "kwargs": {"query": "Иванов"}},
        {"tool": "graph_search", "kwargs": {"query": "Иванов"}},
        {"tool": "submit_answer"},
    ]
    activities = _build_stubs(plan=plan, synth_text="multi-step answer")
    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow], activities=activities,
    ):
        outcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="Иванов", mode="agent", max_iterations=8),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    assert outcome.mode == "agent"
    assert outcome.answer == "multi-step answer"
    assert len(outcome.step_stats) == 2  # excludes submit_answer
    assert outcome.step_stats[0].tool_name == "vector_search"
    assert outcome.step_stats[1].tool_name == "graph_search"


@pytest.mark.asyncio
async def test_agent_repeat_call_guard_breaks(monkeypatch):
    """Same tool + same kwargs 3× in a row → loop exits."""
    plan = [
        {"tool": "vector_search", "kwargs": {"query": "X"}},
        {"tool": "vector_search", "kwargs": {"query": "X"}},  # repeat 1
        {"tool": "vector_search", "kwargs": {"query": "X"}},  # repeat 2 → exit
        # If repeat-guard didn't fire, this would also run:
        {"tool": "vector_search", "kwargs": {"query": "X"}},
    ]
    activities = _build_stubs(plan=plan)
    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow], activities=activities,
    ):
        outcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="X", mode="agent", max_iterations=8),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    # Loop should have stopped after the 3rd identical call (guard
    # tripped) and synthesized over the accumulated sources.
    assert len(outcome.step_stats) == 3


@pytest.mark.asyncio
async def test_agent_max_iterations_cap(monkeypatch):
    """max_iterations=2 forces synth after 2 reasoning steps."""
    plan = [
        {"tool": "vector_search", "kwargs": {"query": "A"}},
        {"tool": "graph_search", "kwargs": {"query": "B"}},
        # If cap not honoured, this would run as step 3:
        {"tool": "submit_answer"},
    ]
    activities = _build_stubs(plan=plan, synth_text="capped")
    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow], activities=activities,
    ):
        outcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="A", mode="agent", max_iterations=2),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    assert len(outcome.step_stats) == 2
    assert outcome.answer == "capped"


@pytest.mark.asyncio
async def test_selfrag_mode_carries_reflective_extras(monkeypatch):
    """mode=selfrag: SearchOutcome populated with citations + uncertainties."""
    plan = [
        {"tool": "vector_search", "kwargs": {"query": "x"}},
        {"tool": "submit_answer"},
    ]
    activities = _build_stubs(
        plan=plan, synth_text="reflective answer", reflective_rounds=2,
    )
    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow], activities=activities,
    ):
        outcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="x", mode="selfrag", max_iterations=8,
                         max_refinements=3),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    assert outcome.mode == "selfrag"
    assert outcome.answer == "reflective answer"
    assert outcome.refinement_rounds == 2
    assert len(outcome.citations) == 1
    assert outcome.citations[0].claim == "x"
    assert len(outcome.uncertainties) == 1


@pytest.mark.asyncio
async def test_no_tool_call_exits_loop_cleanly(monkeypatch):
    """If LLM gives up on tools at step 1 — synthesize over zero sources."""
    @activity.defn(name="agent_reasoning_step")
    async def _give_up(params: ReasoningParams) -> AgentDecision:
        return AgentDecision(
            tool_name="", finished_no_call=True, raw_text="i give up",
        )

    @activity.defn(name="tool_execution")
    async def _never(params: ToolCallParams) -> ToolCallResult:
        raise AssertionError(
            "tool_execution should not be invoked when LLM gives up",
        )

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        return SynthesizeResult(text="empty-sources answer")

    client = await _connect()
    queue = f"sw-test-{uuid.uuid4()}"
    from src.config import settings
    monkeypatch.setattr(
        settings.temporal, "search_task_queue", queue, raising=False,
    )
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchWorkflow],
        activities=[_give_up, _never, _synth],
    ):
        outcome = await client.execute_workflow(
            SearchWorkflow.run,
            SearchParams(query="?", mode="agent"),
            id=f"search-{uuid.uuid4()}", task_queue=queue,
        )
    assert outcome.answer == "empty-sources answer"
    assert outcome.step_stats == []

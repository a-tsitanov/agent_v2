"""SearchOrchestratorWorkflow + SubQueryRetrievalWorkflow tests (R2).

Same skip-on-no-Temporal pattern as test_search_workflow.py.  Activities
are stubbed at the worker — no Milvus / Neo4j / LLM.  We assert the
plan-execute flow:

  * SubQueryRetrievalWorkflow dedups its sources by chunk_id and invokes
    NO agent_reasoning_step,
  * SearchOrchestratorWorkflow plans (2 sub-questions), runs BOTH
    SubQuery children, merges + dedups sources by chunk_id across them,
    and calls synthesize_answer EXACTLY once.

The core merge/dedup logic also has fast unit coverage in
``test_search_v2_merge.py`` (no Temporal needed).
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
    OrchestratorParams,
    PlanParams,
    PlanResult,
    RetrieveParams,
    RetrieveResult,
    SearchOutcome,
    SerializedNode,
    SubQueryParams,
    SubQueryResult,
    SynthesizeParams,
    SynthesizeResult,
)
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow


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


def _node(cid: str) -> SerializedNode:
    return SerializedNode(chunk_id=cid, text=f"text {cid}", score=0.5,
                          metadata={"doc_id": "d1"})


async def _connect() -> Client:
    return await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )


# ── SubQueryRetrievalWorkflow ──────────────────────────────────────


@pytest.mark.asyncio
async def test_subquery_dedups_and_no_reasoning(monkeypatch):
    """retrieve returns overlapping sources → deduped by chunk_id;
    agent_reasoning_step must NOT be invoked."""

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        # c1 duplicated → must collapse to one.
        return RetrieveResult(
            subquestion=params.subquestion,
            sources=[_node("c1"), _node("c2"), _node("c1")],
        )

    @activity.defn(name="agent_reasoning_step")
    async def _never_reason(params) -> None:
        raise AssertionError("plan-execute flow must not reason per-tool")

    client = await _connect()
    queue = f"sub-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[SubQueryRetrievalWorkflow],
        activities=[_retrieve, _never_reason],
    ):
        out: SubQueryResult = await client.execute_workflow(
            SubQueryRetrievalWorkflow.run,
            SubQueryParams(subquestion="кто такой Иванов?"),
            id=f"sub-{uuid.uuid4()}", task_queue=queue,
        )
    assert [n.chunk_id for n in out.sources] == ["c1", "c2"]


# ── SearchOrchestratorWorkflow ─────────────────────────────────────


@pytest.mark.asyncio
async def test_orchestrator_plans_fans_out_merges_synth_once(monkeypatch):
    """plan → 2 children run in parallel → sources merged+deduped by
    chunk_id → synthesize_answer called exactly once."""
    synth_calls: list[int] = []

    @activity.defn(name="plan_subquestions")
    async def _plan(params: PlanParams) -> PlanResult:
        return PlanResult(subquestions=["sub A", "sub B"])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        # Both sub-questions share chunk "shared"; each has a unique one.
        if params.subquestion == "sub A":
            srcs = [_node("shared"), _node("a1")]
        else:
            srcs = [_node("shared"), _node("b1")]
        return RetrieveResult(subquestion=params.subquestion, sources=srcs)

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        synth_calls.append(len(params.accumulated))
        assert params.use_synthesis_llm is True  # large tier
        return SynthesizeResult(text="FINAL ANSWER")

    client = await _connect()
    queue = f"orch-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=[_plan, _retrieve, _synth],
    ):
        out: SearchOutcome = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="кто Иванов и где работает?"),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    assert out.answer == "FINAL ANSWER"
    # shared + a1 + b1 = 3 unique chunks (the duplicate "shared" collapsed).
    assert sorted(n.chunk_id for n in out.sources) == ["a1", "b1", "shared"]
    # synthesize_answer called exactly once, over the merged 3 sources.
    assert synth_calls == [3]
    # one step-stat per sub-question (both children ran).
    assert len(out.step_stats) == 2


@pytest.mark.asyncio
async def test_orchestrator_atomic_single_child(monkeypatch):
    """Atomic question → planner returns [query] → one child runs."""

    @activity.defn(name="plan_subquestions")
    async def _plan(params: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[params.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(subquestion=params.subquestion,
                              sources=[_node("only")])

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        return SynthesizeResult(text="ATOMIC ANSWER")

    client = await _connect()
    queue = f"orch-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=[_plan, _retrieve, _synth],
    ):
        out = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="кто такой Иванов?"),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    assert out.answer == "ATOMIC ANSWER"
    assert len(out.step_stats) == 1
    assert [n.chunk_id for n in out.sources] == ["only"]

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

from src.config import settings
from src.workflow.contracts import (
    OrchestratorParams,
    PlanParams,
    PlanResult,
    RerankParams,
    RerankResult,
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


@activity.defn(name="rerank_sources")
async def _rerank_passthrough(params: RerankParams) -> RerankResult:
    """No-op rerank stub for orchestrator tests — returns the pool
    unchanged so existing source-merge assertions still hold (the real
    bge model is exercised in test_search_rerank.py)."""
    return RerankResult(sources=list(params.sources))


def _pin_large_queue(monkeypatch, queue: str) -> None:
    """Pin the large-tier synthesize queue to the test's own Worker queue
    so one in-test Worker serves every activity (R5 routes
    synthesize_answer to ``large_task_queue``)."""
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)


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
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=[_plan, _retrieve, _rerank_passthrough, _synth],
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
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=[_plan, _retrieve, _rerank_passthrough, _synth],
    ):
        out = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="кто такой Иванов?"),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    assert out.answer == "ATOMIC ANSWER"
    assert len(out.step_stats) == 1
    assert [n.chunk_id for n in out.sources] == ["only"]


# ── coverage gate (R4) ─────────────────────────────────────────────


def _orch_activities_with_coverage(cov_results, retrieve_calls, synth_calls):
    """Build plan/retrieve/coverage/synth stubs sharing call-trackers."""

    @activity.defn(name="plan_subquestions")
    async def _plan(params: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[params.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        retrieve_calls.append(params.subquestion)
        # gap question retrieves a fresh chunk; base question another.
        cid = "gap" if params.subquestion == "MISSING TOPIC" else "base"
        return RetrieveResult(subquestion=params.subquestion,
                              sources=[_node(cid)])

    @activity.defn(name="coverage_check")
    async def _coverage(params) -> "CoverageResult":
        from src.workflow.contracts import CoverageResult
        return cov_results.pop(0)

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        synth_calls.append([n.chunk_id for n in params.accumulated])
        return SynthesizeResult(text="ANSWER")

    return [_plan, _retrieve, _coverage, _rerank_passthrough, _synth]


@pytest.mark.asyncio
async def test_orchestrator_coverage_gap_runs_one_extra_subquery(monkeypatch):
    """coverage returns complete=False+missing → exactly ONE extra
    sub-question runs, its sources merge in, then synthesize once."""
    from src.workflow.contracts import CoverageResult

    retrieve_calls: list[str] = []
    synth_calls: list[list[str]] = []
    acts = _orch_activities_with_coverage(
        cov_results=[CoverageResult(complete=False, missing="MISSING TOPIC")],
        retrieve_calls=retrieve_calls, synth_calls=synth_calls,
    )

    client = await _connect()
    queue = f"orch-cov-{uuid.uuid4()}"
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=acts,
    ):
        out = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="base q", max_coverage_rounds=1),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    # base sub-question + exactly one coverage sub-question ("MISSING TOPIC").
    assert retrieve_calls == ["base q", "MISSING TOPIC"]
    # merged pool has both chunks; synthesize called exactly once over them.
    assert synth_calls == [["base", "gap"]]
    assert sorted(n.chunk_id for n in out.sources) == ["base", "gap"]
    # step-stats: one base child + one coverage child.
    assert len(out.step_stats) == 2


@pytest.mark.asyncio
async def test_orchestrator_coverage_complete_no_extra_round(monkeypatch):
    """coverage complete=True → NO extra sub-question, synth once."""
    from src.workflow.contracts import CoverageResult

    retrieve_calls: list[str] = []
    synth_calls: list[list[str]] = []
    acts = _orch_activities_with_coverage(
        cov_results=[CoverageResult(complete=True, missing="")],
        retrieve_calls=retrieve_calls, synth_calls=synth_calls,
    )

    client = await _connect()
    queue = f"orch-cov-{uuid.uuid4()}"
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=acts,
    ):
        out = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="base q", max_coverage_rounds=1),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    assert retrieve_calls == ["base q"]  # no extra round
    assert synth_calls == [["base"]]
    assert len(out.step_stats) == 1


@pytest.mark.asyncio
async def test_orchestrator_coverage_fail_open_on_activity_error(monkeypatch):
    """coverage_check raises → NO extra round, synth proceeds (no raise)."""

    @activity.defn(name="plan_subquestions")
    async def _plan(params: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[params.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(subquestion=params.subquestion,
                              sources=[_node("base")])

    @activity.defn(name="coverage_check")
    async def _coverage(params):
        raise RuntimeError("boom")

    synth_calls: list[int] = []

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        synth_calls.append(len(params.accumulated))
        return SynthesizeResult(text="ANSWER")

    client = await _connect()
    queue = f"orch-cov-{uuid.uuid4()}"
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=[_plan, _retrieve, _coverage, _rerank_passthrough, _synth],
    ):
        out = await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="base q", max_coverage_rounds=1),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    assert out.answer == "ANSWER"
    assert synth_calls == [1]  # synthesized over the base source, no extra


@pytest.mark.asyncio
async def test_orchestrator_coverage_bounded_to_max_rounds(monkeypatch):
    """Gap persists but max_coverage_rounds=1 → at most ONE extra round."""
    from src.workflow.contracts import CoverageResult

    retrieve_calls: list[str] = []
    synth_calls: list[list[str]] = []
    # Two consecutive gaps offered; the bound must stop after the first.
    acts = _orch_activities_with_coverage(
        cov_results=[
            CoverageResult(complete=False, missing="MISSING TOPIC"),
            CoverageResult(complete=False, missing="MISSING TOPIC"),
        ],
        retrieve_calls=retrieve_calls, synth_calls=synth_calls,
    )

    client = await _connect()
    queue = f"orch-cov-{uuid.uuid4()}"
    _pin_large_queue(monkeypatch, queue)
    async with Worker(
        client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=acts,
    ):
        await client.execute_workflow(
            SearchOrchestratorWorkflow.run,
            OrchestratorParams(query="base q", max_coverage_rounds=1),
            id=f"orch-{uuid.uuid4()}", task_queue=queue,
        )
    # base + exactly ONE coverage round (second gap never acted upon).
    assert retrieve_calls == ["base q", "MISSING TOPIC"]
    assert len(synth_calls) == 1

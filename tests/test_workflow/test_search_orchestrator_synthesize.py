"""SearchOrchestratorWorkflow ``synthesize=False`` guards (LOCAL path).

Lives apart from ``test_search_orchestrator.py`` because that file drives
docker-compose's Temporal at ``localhost:7233`` and skips wholesale when
the dev stack is down — which left the local short-circuit, the default
and most-used mode, with no CI regression cover.

These tests use the bundled **time-skipping test server** instead (same
pattern as ``test_search_global.py`` / ``test_search_drift_roundtrip.py``):
NOT docker-compose's localhost:7233, nothing shared, nothing to clean up.
Skipped gracefully only if the test-server binary cannot start.

``coverage_check`` is disabled per-params (as the drift tests do) so the
workflow never waits out retries on an unregistered activity, and
``large_task_queue`` is pinned to each test's own Worker queue so a
missing guard fails fast rather than hanging on an unpolled queue.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

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
    SynthesizeParams,
    SynthesizeResult,
)
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow


async def _start_env() -> WorkflowEnvironment:
    try:
        return await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover - infra-dependent
        pytest.skip(f"time-skipping test server unavailable: {exc}")


def _node(cid: str) -> SerializedNode:
    return SerializedNode(chunk_id=cid, text=f"text {cid}", score=0.5,
                          metadata={"doc_id": "d1"})


@pytest.mark.asyncio
async def test_orchestrator_skips_synthesis_when_flag_false(monkeypatch):
    """synthesize=False → synthesize_answer is NEVER invoked; the outcome
    still carries the retrieved sources with answer == ""."""

    @activity.defn(name="plan_subquestions")
    async def _plan(params: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[params.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(params: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(subquestion=params.subquestion,
                              sources=[_node("only")])

    @activity.defn(name="rerank_sources")
    async def _rerank(params: RerankParams) -> RerankResult:
        return RerankResult(sources=list(params.sources))

    @activity.defn(name="synthesize_answer")
    async def _synth(params: SynthesizeParams) -> SynthesizeResult:
        raise AssertionError("synthesize_answer must not be invoked")

    env = await _start_env()
    queue = f"orch-synth-{uuid.uuid4()}"
    # synthesize_answer is pinned to large_task_queue inside the workflow —
    # point it at this test's own queue so the single in-test Worker would
    # serve it, and a missing guard fails fast via _synth rather than
    # hanging on an unpolled queue.
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        async with Worker(
            env.client, task_queue=queue,
            workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
            activities=[_plan, _retrieve, _rerank, _synth],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            out: SearchOutcome = await asyncio.wait_for(
                env.client.execute_workflow(
                    SearchOrchestratorWorkflow.run,
                    OrchestratorParams(
                        query="кто такой Иванов?", synthesize=False,
                        coverage_check_enabled=False,
                    ),
                    id=f"orch-{uuid.uuid4()}", task_queue=queue,
                ),
                timeout=30,
            )
    finally:
        await env.shutdown()
    assert out.answer == ""
    assert [n.chunk_id for n in out.sources] == ["only"]

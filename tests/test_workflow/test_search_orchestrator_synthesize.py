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
    ReflectiveCitationDict,
    ReflectiveUncertaintyDict,
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


# ── the rerank guard ───────────────────────────────────────────────────
#
# ``synth_sources`` (orchestrator.py, the 3c rerank block) has exactly one
# reader: the ``build_synthesize_call`` inside ``if params.synthesize:``.
# With synthesize=False the cross-encoder pass therefore ran over the full
# merged pool — GPU, three-minute timeout — and nothing consumed it.
#
# Guarding it is safe ONLY because ``SearchOutcome.sources`` is ``merged``,
# the UNRANKED pool, in both modes (documented in docs/SEARCH.md §4). The
# second test below pins exactly that: if the surfaced sources ever start
# depending on the rerank, the guard has silently become a behaviour
# change and that test must fail.


def _tracked_activities(rerank_calls: list, synth_calls: list) -> list:
    """plan/retrieve/rerank/synth stubs sharing call-trackers.

    The rerank stub deliberately returns a REORDERED, TRUNCATED pool
    (``c3, c1`` out of ``c1, c2, c3``) so a test can tell the surfaced
    ``SearchOutcome.sources`` apart from the reranked synthesis context.
    """

    @activity.defn(name="plan_subquestions")
    async def _plan(p: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[p.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(p: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(
            subquestion=p.subquestion,
            sources=[_node("c1"), _node("c2"), _node("c3")],
        )

    @activity.defn(name="rerank_sources")
    async def _rerank(p: RerankParams) -> RerankResult:
        rerank_calls.append([n.chunk_id for n in p.sources])
        by_id = {n.chunk_id: n for n in p.sources}
        return RerankResult(sources=[by_id["c3"], by_id["c1"]])

    @activity.defn(name="synthesize_answer")
    async def _synth(p: SynthesizeParams) -> SynthesizeResult:
        synth_calls.append([n.chunk_id for n in p.accumulated])
        # Non-empty citations/uncertainties/refinement_rounds so a test can
        # show these are products of synthesis, not retrieval.
        return SynthesizeResult(
            text="REAL ANSWER",
            citations=[ReflectiveCitationDict(claim="c", chunk_id="c1")],
            uncertainties=[ReflectiveUncertaintyDict(topic="t", reason="r")],
            refinement_rounds=2,
        )

    return [_plan, _retrieve, _rerank, _synth]


async def _run_local(
    env: WorkflowEnvironment, queue: str, *,
    synthesize: bool, rerank_calls: list, synth_calls: list,
) -> SearchOutcome:
    async with Worker(
        env.client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=_tracked_activities(rerank_calls, synth_calls),
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        return await asyncio.wait_for(
            env.client.execute_workflow(
                SearchOrchestratorWorkflow.run,
                OrchestratorParams(
                    query="q", synthesize=synthesize,
                    coverage_check_enabled=False,
                ),
                id=f"orch-{uuid.uuid4()}", task_queue=queue,
            ),
            timeout=30,
        )


@pytest.mark.asyncio
async def test_orchestrator_skips_rerank_when_flag_false(monkeypatch):
    """synthesize=False → the cross-encoder rerank is NEVER invoked, the
    same way synthesize_answer is not.  Nothing would have read its
    result."""
    rerank_calls: list = []
    synth_calls: list = []
    env = await _start_env()
    queue = f"orch-rerank-{uuid.uuid4()}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        out = await _run_local(
            env, queue, synthesize=False,
            rerank_calls=rerank_calls, synth_calls=synth_calls,
        )
    finally:
        await env.shutdown()
    assert rerank_calls == []
    assert synth_calls == []
    assert out.answer == ""
    # the full merged pool still comes back, unranked
    assert [n.chunk_id for n in out.sources] == ["c1", "c2", "c3"]


@pytest.mark.asyncio
async def test_outcome_sources_identical_with_and_without_synthesis(monkeypatch):
    """THE property that makes guarding the rerank safe: over the same
    stubbed retrieval, ``SearchOutcome.sources`` is byte-identical between
    a synthesize=True run and a synthesize=False run.

    If this ever fails, the surfaced sources have started depending on the
    rerank and the guard is no longer a pure no-op for callers.
    """
    on_calls: list = []
    on_synth: list = []
    off_calls: list = []
    off_synth: list = []
    env = await _start_env()
    queue = f"orch-parity-{uuid.uuid4()}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        with_synth = await _run_local(
            env, queue, synthesize=True,
            rerank_calls=on_calls, synth_calls=on_synth,
        )
        without_synth = await _run_local(
            env, queue, synthesize=False,
            rerank_calls=off_calls, synth_calls=off_synth,
        )
    finally:
        await env.shutdown()

    # The synthesising run really did rerank, and synthesis really did see
    # the reordered/truncated pool — so the stub is discriminating.
    assert on_calls == [["c1", "c2", "c3"]]
    assert on_synth == [["c3", "c1"]]
    # The retrieval-only run did neither.
    assert off_calls == []
    assert off_synth == []

    # ...and yet the surfaced sources are the same unranked merged pool.
    assert [n.chunk_id for n in with_synth.sources] == ["c1", "c2", "c3"]
    assert with_synth.sources == without_synth.sources
    # The other RETRIEVAL products are unchanged too.
    assert with_synth.documents == without_synth.documents
    assert with_synth.step_stats == without_synth.step_stats

    # The SYNTHESIS products are NOT unchanged — they come back empty,
    # because the skip branch builds SynthesizeResult(text="").  This is
    # what docs/SEARCH.md and docs/runbook/mcp.md must say: MCP clients
    # read citations/uncertainties/refinement_rounds via _outcome_to_dict
    # even though SearchResponse does not carry them over HTTP.
    assert with_synth.answer == "REAL ANSWER"
    assert without_synth.answer == ""
    assert len(with_synth.citations) == 1
    assert without_synth.citations == []
    assert len(with_synth.uncertainties) == 1
    assert without_synth.uncertainties == []
    assert with_synth.refinement_rounds == 2
    assert without_synth.refinement_rounds == 0

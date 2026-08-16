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


# ── rerank now runs on every path, and its ordering IS the output ──────
#
# Task 1 (2026-08-14-rerank-orders-output) reverses the guard that used
# to live here.  Previously: ``synth_sources`` (the 3c rerank block) had
# exactly one reader — ``build_synthesize_call`` inside
# ``if params.synthesize:`` — so with synthesize=False the rerank was
# skipped as dead work, and that was invisible to callers because
# ``SearchOutcome.sources`` was ``merged`` (the UNRANKED pool) either way.
#
# Now ``SearchOutcome.sources`` IS the rerank activity's output (the whole
# pool, ranked — ``top_n=len(merged)``, not the old ``rerank_top_n``), so
# the rerank determines what every caller sees and can no longer be
# skipped on either path.  Only the synthesis prompt still gets the
# ``rerank_top_n`` cap, via ``cap_synth_sources``.
#
# ``test_orchestrator_skips_rerank_when_flag_false`` and
# ``test_outcome_sources_identical_with_and_without_synthesis`` encoded
# the old guard's intent (rerank skipped off-path; sources unranked and
# thus flag-independent for an unrelated reason) and are replaced below —
# not deleted silently — by their inverse plus an updated parity test.


def _reorder_rerank_activities(rerank_calls: list, synth_calls: list) -> list:
    """plan/retrieve/rerank/synth stubs where rerank returns the FULL pool
    REVERSED — a permutation, never a truncation — so a test can tell
    ranked order apart from merge order while proving nothing is dropped.

    Retrieval returns 7 sources, comfortably more than the default
    ``rerank_top_n`` (5, monkeypatched to 3 in some tests below) so a
    truncation bug can't hide behind a short pool.
    """

    @activity.defn(name="plan_subquestions")
    async def _plan(p: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[p.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(p: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(
            subquestion=p.subquestion,
            sources=[_node(f"c{i}") for i in range(1, 8)],
        )

    @activity.defn(name="rerank_sources")
    async def _rerank(p: RerankParams) -> RerankResult:
        rerank_calls.append([n.chunk_id for n in p.sources])
        return RerankResult(sources=list(reversed(p.sources)))

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


async def _run_reorder(
    env: WorkflowEnvironment, queue: str, *,
    synthesize: bool, rerank_calls: list, synth_calls: list,
) -> SearchOutcome:
    async with Worker(
        env.client, task_queue=queue,
        workflows=[SearchOrchestratorWorkflow, SubQueryRetrievalWorkflow],
        activities=_reorder_rerank_activities(rerank_calls, synth_calls),
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


_MERGED_ORDER = [f"c{i}" for i in range(1, 8)]
_RANKED_ORDER = list(reversed(_MERGED_ORDER))


@pytest.mark.asyncio
async def test_orchestrator_sources_come_back_ranked_and_complete(monkeypatch):
    """Three properties in one run: (1) ordering reaches the caller —
    ``SearchOutcome.sources`` is in the stub's reranked order, not merge
    order; (2) nothing is lost — same 7 chunk_ids, explicit count; (3)
    synthesis still only sees the ``rerank_top_n``-capped head of the
    ranked order, not the whole pool."""
    monkeypatch.setattr(settings.temporal, "rerank_top_n", 3)
    rerank_calls: list = []
    synth_calls: list = []
    env = await _start_env()
    queue = f"orch-ranked-{uuid.uuid4()}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        out = await _run_reorder(
            env, queue, synthesize=True,
            rerank_calls=rerank_calls, synth_calls=synth_calls,
        )
    finally:
        await env.shutdown()

    # rerank was asked for the WHOLE pool (top_n=len(merged)=7), not the
    # old rerank_top_n=3 cap.
    assert rerank_calls == [_MERGED_ORDER]

    # (1) ordering reaches the caller: best-first per the stub.
    assert [n.chunk_id for n in out.sources] == _RANKED_ORDER
    # (2) nothing lost: same set AND same count as the merged pool.
    assert len(out.sources) == 7
    assert {n.chunk_id for n in out.sources} == set(_MERGED_ORDER)

    # (3) synthesis only sees the capped (rerank_top_n=3) head of the
    # ranked order — the first 3 of _RANKED_ORDER, not all 7.
    assert synth_calls == [_RANKED_ORDER[:3]]


@pytest.mark.asyncio
async def test_orchestrator_reranks_even_when_synthesis_skipped(monkeypatch):
    """Inverse of the old guard test: synthesize=False no longer skips the
    rerank — it is the only source of the returned ordering now, so it
    must run on both paths.  ``sources`` still comes back as the FULL
    ranked pool (nothing lost); ``synthesize_answer`` is still never
    called."""
    rerank_calls: list = []
    synth_calls: list = []
    env = await _start_env()
    queue = f"orch-rerank-off-{uuid.uuid4()}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        out = await _run_reorder(
            env, queue, synthesize=False,
            rerank_calls=rerank_calls, synth_calls=synth_calls,
        )
    finally:
        await env.shutdown()

    assert rerank_calls == [_MERGED_ORDER]  # rerank WAS invoked
    assert synth_calls == []  # synthesis was NOT
    assert out.answer == ""
    # full ranked pool, not merge order, not truncated.
    assert [n.chunk_id for n in out.sources] == _RANKED_ORDER
    assert len(out.sources) == 7


@pytest.mark.asyncio
async def test_outcome_sources_identical_with_and_without_synthesis(monkeypatch):
    """``SearchOutcome.sources`` is byte-identical between a synthesize=True
    run and a synthesize=False run — but now because rerank runs ONCE,
    unconditionally, before the synthesize branch, not because sources
    bypass rerank (the old guard's invariant, inverted by the two tests
    above)."""
    on_rerank: list = []
    on_synth: list = []
    off_rerank: list = []
    off_synth: list = []
    env = await _start_env()
    queue = f"orch-parity-{uuid.uuid4()}"
    monkeypatch.setattr(settings.temporal, "large_task_queue", queue)
    try:
        with_synth = await _run_reorder(
            env, queue, synthesize=True,
            rerank_calls=on_rerank, synth_calls=on_synth,
        )
        without_synth = await _run_reorder(
            env, queue, synthesize=False,
            rerank_calls=off_rerank, synth_calls=off_synth,
        )
    finally:
        await env.shutdown()

    # Both runs reranked the identical merged pool — rerank is no longer
    # gated by the synthesize flag.
    assert on_rerank == off_rerank == [_MERGED_ORDER]
    # Only the synthesising run called synthesize_answer.
    assert on_synth and not off_synth

    # ...and the surfaced sources are the same ranked pool either way.
    assert [n.chunk_id for n in with_synth.sources] == _RANKED_ORDER
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


@pytest.mark.asyncio
async def test_orchestrator_falls_back_to_the_merged_pool_when_rerank_activity_raises(
    monkeypatch,
):
    """The orchestrator's OWN ``except`` around the ``rerank_sources``
    activity call (orchestrator.py, not the activity's internal
    "reranker unavailable" fallback already covered in
    ``test_search_rerank.py``): when the ACTIVITY ITSELF raises — every
    attempt, so retries don't rescue it — ``SearchOutcome.sources`` must
    still come back as the full merged pool, in merge order, not dropped
    or truncated."""

    @activity.defn(name="plan_subquestions")
    async def _plan(p: PlanParams) -> PlanResult:
        return PlanResult(subquestions=[p.query])

    @activity.defn(name="retrieve_subquestion")
    async def _retrieve(p: RetrieveParams) -> RetrieveResult:
        return RetrieveResult(
            subquestion=p.subquestion,
            sources=[_node(f"c{i}") for i in range(1, 4)],
        )

    @activity.defn(name="rerank_sources")
    async def _rerank(p: RerankParams) -> RerankResult:
        raise RuntimeError("reranker blew up")

    @activity.defn(name="synthesize_answer")
    async def _synth(p: SynthesizeParams) -> SynthesizeResult:
        raise AssertionError("synthesize_answer must not be invoked")

    env = await _start_env()
    queue = f"orch-rerank-raises-{uuid.uuid4()}"
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
                        query="q", synthesize=False,
                        coverage_check_enabled=False,
                    ),
                    id=f"orch-{uuid.uuid4()}", task_queue=queue,
                ),
                timeout=30,
            )
    finally:
        await env.shutdown()

    assert [n.chunk_id for n in out.sources] == ["c1", "c2", "c3"]
    assert len(out.sources) == 3
    assert out.answer == ""

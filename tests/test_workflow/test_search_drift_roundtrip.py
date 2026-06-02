"""Regression: drift path decodes GlobalSearchParams (not dict) inside a
real Temporal worker with the pydantic data converter.

Guards the exact call that raised
``AttributeError: 'dict' object has no attribute 'drift_mode'`` —
``GlobalSearchWorkflow.run(args=[GlobalSearchParams(drift_mode=True),
list[SerializedNode]])`` — which the pure-helper tests never exercised.

Uses the bundled time-skipping test server (NOT docker-compose's
localhost:7233), so it runs without the dev stack — but the test server
binary must be fetchable/cached; skipped gracefully if it cannot start.
"""

from __future__ import annotations

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from src.config import settings
from src.workflow.contracts import (
    DocumentsForCommunitiesParams,
    DocumentsForCommunitiesResult,
    MapCommunitiesParams,
    MapCommunitiesResult,
    MapPartialParams,
    MapPartialResult,
    SerializedNode,
    SynthesizeParams,
    SynthesizeResult,
)
from src.workflow.contracts import GlobalSearchParams
from src.workflow.search.global_wf import GlobalSearchWorkflow


@activity.defn(name="map_communities")
async def _map_communities(p: MapCommunitiesParams) -> MapCommunitiesResult:
    return MapCommunitiesResult(communities=[])


@activity.defn(name="documents_for_communities")
async def _documents_for_communities(
    p: DocumentsForCommunitiesParams,
) -> DocumentsForCommunitiesResult:
    # GlobalSearchWorkflow now resolves contributing communities → source
    # docs; stub it (no graph) so this decode-regression test stays focused.
    return DocumentsForCommunitiesResult(doc_ids=[])


@activity.defn(name="map_community_partial")
async def _map_community_partial(p: MapPartialParams) -> MapPartialResult:
    return MapPartialResult(community_id=p.community_id, partial="", score=0.0)


@activity.defn(name="synthesize_answer")
async def _synthesize_answer(p: SynthesizeParams) -> SynthesizeResult:
    # Echo the mode so the test can assert the workflow read drift_mode.
    return SynthesizeResult(text=f"ok:{p.mode}", refinement_rounds=0)


@pytest.mark.asyncio
async def test_global_drift_decodes_params_not_dict(monkeypatch):
    # synthesize_answer is pinned to large_task_queue inside the workflow;
    # point it at the single test queue so one worker hosts everything.
    monkeypatch.setattr(settings.temporal, "large_task_queue", "drift-test-q")

    try:
        env = await WorkflowEnvironment.start_time_skipping(
            data_converter=pydantic_data_converter,
        )
    except Exception as exc:  # pragma: no cover - infra-dependent
        pytest.skip(f"time-skipping test server unavailable: {exc}")

    async with env:
        async with Worker(
            env.client,
            task_queue="drift-test-q",
            workflows=[GlobalSearchWorkflow],
            activities=[
                _map_communities, _map_community_partial, _synthesize_answer,
                _documents_for_communities,
            ],
            # Bypass the workflow sandbox: we're testing arg DECODING through
            # the pydantic converter, not sandbox safety.  The sandbox
            # re-imports modules and is sensitive to global state left by
            # earlier tests in a full-suite run — unsandboxed keeps this
            # regression guard deterministic regardless of test order.
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            outcome = await env.client.execute_workflow(
                GlobalSearchWorkflow.run,
                args=[
                    GlobalSearchParams(query="q", drift_mode=True),
                    [SerializedNode(chunk_id="c1", text="seed", score=0.9)],
                ],
                id="drift-rt-1",
                task_queue="drift-test-q",
            )

    # Would be "global" (or raise AttributeError) if drift_mode were lost.
    assert outcome.mode == "drift"
    assert outcome.answer == "ok:simple"

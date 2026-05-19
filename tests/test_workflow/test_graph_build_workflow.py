"""GraphBuildWorkflow integration test.

Runs against the docker-compose Temporal at localhost:7233 if reachable,
otherwise skipped — same pattern as other workflow tests in this dir.
Activities are stubbed at the worker so we don't touch Neo4j or any
LLM; we only assert the workflow chains them in the right order and
returns a populated ``GraphBuildResult``.
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
    Ctx,
    GraphBuildResult,
    GraphBuilt,
    KGExtracted,
    Merged,
    Parsed,
)
from src.workflow.graph_build import GraphBuildWorkflow


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


@activity.defn(name="merge_and_resolve")
async def _merge_stub(kg: KGExtracted) -> Merged:
    return Merged(
        kg=kg,
        merged_entities_uri="s3://kb-staging/run-test/merged.pkl",
    )


@activity.defn(name="build_property_graph")
async def _build_pg_stub(merged: Merged) -> GraphBuilt:
    return GraphBuilt(entities=3, relations=2)


@pytest.mark.asyncio
async def test_graph_build_workflow_chains_and_returns_both_pieces():
    client = await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )
    queue = f"gb-test-{uuid.uuid4()}"
    ctx = Ctx(doc_id="d-test", local_path="/x", cleanup_dir=None,
              workflow_run_id="run-test")
    parsed = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/run-test/parsed.pkl",
                    chunk_count=1)
    kg = KGExtracted(parsed=parsed,
                     nodes_with_kg_uri="s3://kb-staging/run-test/kg.pkl")

    async with Worker(
        client, task_queue=queue,
        workflows=[GraphBuildWorkflow],
        activities=[_merge_stub, _build_pg_stub],
    ):
        # In this test the stubs need to run on the SAME queue as
        # the child workflow (since we override task_queue on the
        # real build_property_graph in production but not in the
        # test stub).  Pass the queue's name as both child + activity
        # task_queue via the worker registration above.
        result = await client.execute_workflow(
            GraphBuildWorkflow.run, kg,
            id=f"graph-{uuid.uuid4()}", task_queue=queue,
        )

    assert isinstance(result, GraphBuildResult)
    assert result.merged.merged_entities_uri == "s3://kb-staging/run-test/merged.pkl"
    assert result.built.entities == 3
    assert result.built.relations == 2

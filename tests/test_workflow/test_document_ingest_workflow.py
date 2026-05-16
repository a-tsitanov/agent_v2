"""Workflow-level tests with mocked activities.

Connects to the project's docker-compose Temporal (port 7233) and
runs the workflow with stubbed activities so failure cases finish
in real time (non-retryable errors fail fast).  Each test uses a
unique task queue + workflow id to stay hermetic.
"""

from __future__ import annotations

import socket
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    GraphBuilt,
    Indexed,
    IngestParams,
    IngestResult,
    Injected,
    KGExtracted,
    MarkFailedIn,
    Merged,
    Parsed,
)
from src.workflow.document_ingest import DocumentIngestWorkflow


def _temporal_up(host: str = "localhost", port: int = 7233) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _temporal_up(),
    reason="docker-compose Temporal (localhost:7233) not reachable",
)


async def _connect() -> Client:
    return await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )


# ── canned activity stubs ─────────────────────────────────────────


@activity.defn(name="fetch_source")
async def fetch_source_stub(params: IngestParams) -> Ctx:
    return Ctx(
        doc_id=params.doc_id, local_path="/tmp/x", cleanup_dir=None,
        workflow_run_id="run-test",
    )


@activity.defn(name="parse_and_chunk")
async def parse_and_chunk_stub(ctx: Ctx) -> Parsed:
    return Parsed(ctx=ctx, nodes_uri="s3://kb-staging/run-test/parsed.pkl",
                  chunk_count=3)


@activity.defn(name="index_vector")
async def index_vector_stub(parsed: Parsed) -> Indexed:
    return Indexed(node_ids=["a", "b", "c"], count=3)


@activity.defn(name="inject_canonical")
async def inject_canonical_stub(parsed: Parsed) -> Injected:
    return Injected(count=parsed.chunk_count)


@activity.defn(name="extract_kg")
async def extract_kg_stub(parsed: Parsed) -> KGExtracted:
    return KGExtracted(parsed=parsed, nodes_with_kg_uri="s3://kb-staging/run-test/kg.pkl")


@activity.defn(name="merge_and_resolve")
async def merge_and_resolve_stub(kg: KGExtracted) -> Merged:
    return Merged(kg=kg, merged_entities_uri="s3://kb-staging/run-test/merged.pkl")


@activity.defn(name="build_property_graph")
async def build_pg_stub(merged: Merged) -> GraphBuilt:
    return GraphBuilt(entities=2, relations=1)


@activity.defn(name="finalize")
async def finalize_stub(payload: FinalizeIn) -> IngestResult:
    return IngestResult(
        doc_id=payload.ctx.doc_id, chunk_count=payload.indexed.count,
        graph_status=payload.graph_status,
    )


@activity.defn(name="mark_failed")
async def mark_failed_stub(payload: MarkFailedIn) -> None:
    return None


HAPPY_ACTIVITIES = [
    fetch_source_stub, parse_and_chunk_stub, index_vector_stub,
    inject_canonical_stub, extract_kg_stub, merge_and_resolve_stub,
    build_pg_stub, finalize_stub, mark_failed_stub,
]


# ── tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_happy_path_completed():
    client = await _connect()
    queue = f"wf-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[DocumentIngestWorkflow],
        activities=HAPPY_ACTIVITIES,
    ):
        params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
        result = await client.execute_workflow(
            DocumentIngestWorkflow.run, params,
            id=f"ingest-{params.doc_id}", task_queue=queue,
        )
    assert result.graph_status == "completed"
    assert result.chunk_count == 3


@pytest.mark.asyncio
async def test_graph_failure_downgrades_to_vector_only():
    @activity.defn(name="extract_kg")
    async def boom(parsed: Parsed) -> KGExtracted:
        raise ApplicationError("LLM 503", non_retryable=True)

    activities = [
        fetch_source_stub, parse_and_chunk_stub, index_vector_stub,
        inject_canonical_stub, boom, merge_and_resolve_stub,
        build_pg_stub, finalize_stub, mark_failed_stub,
    ]

    client = await _connect()
    queue = f"wf-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[DocumentIngestWorkflow], activities=activities,
    ):
        params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
        result = await client.execute_workflow(
            DocumentIngestWorkflow.run, params,
            id=f"ingest-{params.doc_id}", task_queue=queue,
        )
    assert result.graph_status == "vector_only"


@pytest.mark.asyncio
async def test_vector_failure_runs_mark_failed_and_raises():
    mark_failed_calls: list[MarkFailedIn] = []

    @activity.defn(name="mark_failed")
    async def record_failure(payload: MarkFailedIn) -> None:
        mark_failed_calls.append(payload)

    @activity.defn(name="index_vector")
    async def boom(parsed: Parsed) -> Indexed:
        raise ApplicationError("milvus down", non_retryable=True)

    activities = [
        fetch_source_stub, parse_and_chunk_stub, boom,
        inject_canonical_stub, extract_kg_stub, merge_and_resolve_stub,
        build_pg_stub, finalize_stub, record_failure,
    ]

    client = await _connect()
    queue = f"wf-test-{uuid.uuid4()}"
    async with Worker(
        client, task_queue=queue,
        workflows=[DocumentIngestWorkflow], activities=activities,
    ):
        params = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x")
        with pytest.raises(Exception):
            await client.execute_workflow(
                DocumentIngestWorkflow.run, params,
                id=f"ingest-{params.doc_id}", task_queue=queue,
            )

    assert len(mark_failed_calls) == 1
    assert mark_failed_calls[0].params.doc_id == params.doc_id

"""End-to-end test against docker-compose Temporal + live Milvus /
Neo4j / MinIO / Postgres.

Skipped when any of the infra ports isn't reachable.  The test takes
~30-90 s on a warm machine (LLM-bound during extract_kg).
"""

from __future__ import annotations

import socket
import uuid
from pathlib import Path

import psycopg
import pytest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import (
    EXTRACT_ACTIVITIES,
    MAIN_ACTIVITIES,
    MERGE_ACTIVITIES,
)
from src.workflow.contracts import IngestParams
from src.workflow.document_ingest import DocumentIngestWorkflow
from src.workflow.graph_build import GraphBuildWorkflow


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not all([
        _port_open("localhost", 19530),  # milvus
        _port_open("localhost", 7687),   # neo4j
        _port_open("localhost", 9000),   # minio
        _port_open("localhost", 5432),   # postgres
        _port_open("localhost", 7233),   # temporal
    ]),
    reason="live infra (milvus/neo4j/minio/postgres/temporal) not reachable",
)


@pytest.mark.asyncio
async def test_full_pipeline_happy_path(monkeypatch):
    fixture = (
        Path(__file__).parent.parent / "test_ingestion" / "fixtures" / "sample.txt"
    )
    if not fixture.exists():
        pytest.skip("sample fixture missing")

    doc_id = uuid.uuid4()
    with psycopg.connect(settings.postgres.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO documents (id, path, status) VALUES (%s, %s, 'pending')",
                (str(doc_id), str(fixture)),
            )
        conn.commit()

    client = await Client.connect(
        "localhost:7233", namespace="default",
        data_converter=pydantic_data_converter,
    )
    queue = f"kb-ingest-it-{doc_id}"
    llm_queue = f"{queue}-llm"
    merge_queue = f"{queue}-merge"
    monkeypatch.setattr(
        settings.temporal, "llm_task_queue", llm_queue, raising=False,
    )
    monkeypatch.setattr(
        settings.temporal, "merge_task_queue", merge_queue, raising=False,
    )
    main_worker = Worker(
        client, task_queue=queue,
        workflows=[DocumentIngestWorkflow],
        activities=MAIN_ACTIVITIES,
        max_concurrent_activities=2,
    )
    # extract lane
    llm_worker = Worker(
        client, task_queue=llm_queue,
        activities=EXTRACT_ACTIVITIES,
        max_concurrent_activities=1,
    )
    # merge lane: hosts the GraphBuildWorkflow child + its activities
    merge_worker = Worker(
        client, task_queue=merge_queue,
        workflows=[GraphBuildWorkflow],
        activities=MERGE_ACTIVITIES,
        max_concurrent_activities=1,
    )
    async with main_worker, llm_worker, merge_worker:
        params = IngestParams(doc_id=str(doc_id), path=str(fixture))
        result = await client.execute_workflow(
            DocumentIngestWorkflow.run, params,
            id=f"ingest-{doc_id}", task_queue=queue,
        )

    assert result.doc_id == str(doc_id)
    assert result.graph_status in ("completed", "vector_only")
    assert result.chunk_count > 0

    # Verify Postgres terminal state.
    with psycopg.connect(settings.postgres.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM documents WHERE id = %s", (str(doc_id),),
            )
            (status,) = cur.fetchone()
    assert status in ("completed", "vector_only")

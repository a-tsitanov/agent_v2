"""`parse_and_chunk` runs the LlamaIndex IngestionPipeline + writes
the resulting nodes to a staging blob."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.parse_and_chunk import parse_and_chunk
from src.workflow.contracts import Ctx


@pytest.mark.asyncio
async def test_writes_nodes_blob_and_returns_uri(tmp_path: Path):
    local = tmp_path / "doc.pdf"
    local.write_bytes(b"PDF")
    ctx = Ctx(
        doc_id="d",
        local_path=str(local),
        cleanup_dir=str(tmp_path),
        workflow_run_id="run-x",
    )

    node = MagicMock()
    node.node_id = "n1"
    fake_pipeline = MagicMock()
    fake_pipeline.arun = AsyncMock(return_value=[node, node])

    staging = MagicMock()
    staging.write_pickle.return_value = "s3://kb-staging/run-x/parsed.pkl"

    doc = MagicMock()
    doc.metadata = {"file_path": str(local)}

    with patch(
        "src.workflow.activities.parse_and_chunk.read_documents",
        return_value=[doc],
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_ingestion_pipeline",
        return_value=fake_pipeline,
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.parse_and_chunk.activity"
    ) as mock_activity:
        mock_activity.heartbeat = MagicMock()
        out = await parse_and_chunk(ctx)

    assert out.nodes_uri == "s3://kb-staging/run-x/parsed.pkl"
    assert out.chunk_count == 2
    staging.write_pickle.assert_called_once()
    args = staging.write_pickle.call_args.args
    assert args[0] == "run-x"
    assert args[1] == "parsed"


@pytest.mark.asyncio
async def test_raises_when_reader_does_not_find_file(tmp_path: Path):
    local = tmp_path / "doc.pdf"
    local.write_bytes(b"PDF")
    ctx = Ctx(
        doc_id="d", local_path=str(local), cleanup_dir=None,
        workflow_run_id="run-y",
    )

    other = MagicMock()
    other.metadata = {"file_path": "/somewhere/else.pdf"}

    with patch(
        "src.workflow.activities.parse_and_chunk.read_documents",
        return_value=[other],
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_ingestion_pipeline",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_llm",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_embedding_model",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.build_staging_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.parse_and_chunk.activity"
    ):
        with pytest.raises(FileNotFoundError):
            await parse_and_chunk(ctx)

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.finalize import finalize, mark_failed
from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    Indexed,
    IngestParams,
    MarkFailedIn,
)


@pytest.mark.asyncio
async def test_finalize_writes_completed_and_cleans(tmp_path):
    cleanup_dir = tmp_path / "cache" / "doc-1"
    cleanup_dir.mkdir(parents=True)
    (cleanup_dir / "x.pdf").write_bytes(b"x")

    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()

    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path=str(cleanup_dir / "x.pdf"),
        cleanup_dir=str(cleanup_dir),
        workflow_run_id="run-x",
    )
    indexed = Indexed(node_ids=["a", "b"], count=2)
    fin = FinalizeIn(ctx=ctx, indexed=indexed, graph_status="completed")

    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.finalize.activity"
    ):
        out = await finalize(fin)

    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="completed",
    )
    staging.delete_prefix.assert_called_once_with("run-x")
    assert not cleanup_dir.exists()
    assert out.graph_status == "completed"
    assert out.chunk_count == 2


@pytest.mark.asyncio
async def test_finalize_writes_vector_only_status():
    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()
    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path="/tmp/x.pdf", cleanup_dir=None, workflow_run_id="run-y",
    )
    fin = FinalizeIn(
        ctx=ctx, indexed=Indexed(node_ids=[], count=0),
        graph_status="vector_only",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.finalize.activity"
    ):
        out = await finalize(fin)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="vector_only",
    )
    assert out.graph_status == "vector_only"


@pytest.mark.asyncio
async def test_mark_failed_with_ctx(tmp_path):
    cleanup_dir = tmp_path / "doc"
    cleanup_dir.mkdir()
    (cleanup_dir / "x.pdf").write_bytes(b"x")

    pg = MagicMock()
    pg.update_status = AsyncMock()
    staging = MagicMock()
    ctx = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path=str(cleanup_dir / "x.pdf"),
        cleanup_dir=str(cleanup_dir),
        workflow_run_id="run-z",
    )
    payload = MarkFailedIn(
        ctx=ctx,
        params=IngestParams(doc_id=ctx.doc_id, path="s3://kb-uploads/x"),
        error="boom",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=staging,
    ), patch(
        "src.workflow.activities.finalize.activity"
    ):
        await mark_failed(payload)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(ctx.doc_id), status="failed", error="boom",
    )
    staging.delete_prefix.assert_called_once_with("run-z")
    assert not cleanup_dir.exists()


@pytest.mark.asyncio
async def test_mark_failed_without_ctx_still_writes_pg():
    """fetch_source crashed before producing a Ctx — mark_failed must
    still write `failed` status by using params.doc_id."""
    pg = MagicMock()
    pg.update_status = AsyncMock()
    payload = MarkFailedIn(
        ctx=None,
        params=IngestParams(
            doc_id="11111111-1111-1111-1111-111111111111",
            path="s3://kb-uploads/x",
        ),
        error="boom",
    )
    with patch(
        "src.workflow.activities.finalize.AsyncPostgres", return_value=pg,
    ), patch(
        "src.workflow.activities.finalize.build_staging_store",
        return_value=MagicMock(),
    ), patch(
        "src.workflow.activities.finalize.activity"
    ):
        await mark_failed(payload)
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(payload.params.doc_id), status="failed", error="boom",
    )

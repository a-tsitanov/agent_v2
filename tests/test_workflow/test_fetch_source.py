"""`fetch_source` resolves an s3:// path to a local file, updates PG
to `processing`, and is idempotent: re-run with the file already on
disk skips download."""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflow.activities.fetch_source import fetch_source
from src.workflow.contracts import IngestParams


@pytest.fixture
def fake_minio(tmp_path: Path):
    storage = MagicMock()
    storage.parse_s3_uri.return_value = ("kb-uploads", "doc-1/file.pdf")
    storage.download_dir = tmp_path / "cache"

    def _download(uri: str, local: Path) -> Path:
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(b"PDF")
        return local

    storage.get_object_to_path.side_effect = _download
    return storage


@pytest.mark.asyncio
async def test_s3_path_downloads_and_marks_processing(fake_minio, tmp_path):
    pg = MagicMock()
    pg.update_status = AsyncMock()

    params = IngestParams(
        doc_id="11111111-1111-1111-1111-111111111111",
        path="s3://kb-uploads/doc-1/file.pdf",
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
        return_value=fake_minio,
    ), patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        info = MagicMock(workflow_run_id="run-1")
        mock_activity.info.return_value = info
        ctx = await fetch_source(params)

    assert ctx.local_path == str(tmp_path / "cache" / params.doc_id / "file.pdf")
    assert ctx.cleanup_dir == str(tmp_path / "cache" / params.doc_id)
    assert ctx.workflow_run_id == "run-1"
    pg.update_status.assert_awaited_once_with(
        uuid.UUID(params.doc_id), status="processing",
    )


@pytest.mark.asyncio
async def test_legacy_local_path_passes_through(tmp_path):
    pg = MagicMock()
    pg.update_status = AsyncMock()

    local = tmp_path / "old.txt"
    local.write_text("legacy")
    params = IngestParams(
        doc_id="11111111-1111-1111-1111-111111111111",
        path=str(local),
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
    ) as build, patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        mock_activity.info.return_value = MagicMock(workflow_run_id="run-2")
        ctx = await fetch_source(params)

    assert ctx.local_path == str(local)
    assert ctx.cleanup_dir is None
    build.assert_not_called()


@pytest.mark.asyncio
async def test_s3_path_skips_download_when_file_present(fake_minio, tmp_path):
    """Second activity attempt after worker crash: file already on
    disk → no second MinIO GET."""
    pg = MagicMock()
    pg.update_status = AsyncMock()

    doc_id = "11111111-1111-1111-1111-111111111111"
    target = tmp_path / "cache" / doc_id / "file.pdf"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"PDF-cached")

    params = IngestParams(
        doc_id=doc_id,
        path="s3://kb-uploads/doc-1/file.pdf",
    )

    with patch(
        "src.workflow.activities.fetch_source.build_minio_storage",
        return_value=fake_minio,
    ), patch(
        "src.workflow.activities.fetch_source.AsyncPostgres",
        return_value=pg,
    ), patch(
        "src.workflow.activities.fetch_source.activity"
    ) as mock_activity:
        mock_activity.info.return_value = MagicMock(workflow_run_id="run-3")
        await fetch_source(params)

    fake_minio.get_object_to_path.assert_not_called()

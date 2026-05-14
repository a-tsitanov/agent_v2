"""Unit tests for `_resolve_source_path` (worker's MinIO download
branch).

Verifies that:
  * `s3://...` paths trigger a download into MINIO_DOWNLOAD_DIR with a
    cleanup target,
  * legacy `/tmp/...` paths are passed through verbatim with no
    cleanup,
  * the local download path layout uses `<download_dir>/<doc_id>/<filename>`
    so concurrent jobs don't collide on filenames.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.tasks import _resolve_source_path


@pytest.mark.asyncio
async def test_s3_path_triggers_download(tmp_path: Path) -> None:
    """For `s3://...` paths we should fetch the object into the
    configured staging dir and report a `cleanup_dir`.
    """
    storage = MagicMock()
    storage.parse_s3_uri.return_value = ("kb-uploads", "doc-id-x/file.txt")
    storage.download_dir = tmp_path / "cache"

    def _fake_download(uri: str, local: Path) -> Path:
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(b"hello")
        return local

    storage.get_object_to_path.side_effect = _fake_download

    with patch(
        "src.ingestion.tasks.build_minio_storage", return_value=storage,
    ):
        target, cleanup = await _resolve_source_path(
            "doc-id-x", "s3://kb-uploads/doc-id-x/file.txt",
        )

    assert target == tmp_path / "cache" / "doc-id-x" / "file.txt"
    assert target.exists()
    assert cleanup == target.parent
    storage.get_object_to_path.assert_called_once()


@pytest.mark.asyncio
async def test_legacy_local_path_skips_minio() -> None:
    """Bare filesystem paths must not touch the MinIO client."""
    with patch(
        "src.ingestion.tasks.build_minio_storage",
    ) as build:
        target, cleanup = await _resolve_source_path(
            "doc-id-y", "/tmp/kb-uploads/old.txt",
        )

    assert target == Path("/tmp/kb-uploads/old.txt")
    assert cleanup is None
    build.assert_not_called()


@pytest.mark.asyncio
async def test_s3_path_uses_doc_id_prefixed_local_dir(tmp_path: Path) -> None:
    """Two parallel ingests of the same filename must land in different
    directories so cleanup of one doesn't clobber the other.
    """
    storage = MagicMock()
    storage.download_dir = tmp_path / "cache"
    storage.parse_s3_uri.side_effect = lambda uri: (
        "kb-uploads", uri.split("kb-uploads/", 1)[1],
    )
    storage.get_object_to_path.side_effect = (
        lambda uri, local: local.parent.mkdir(parents=True, exist_ok=True)
        or local.write_bytes(b"x") or local
    )

    with patch(
        "src.ingestion.tasks.build_minio_storage", return_value=storage,
    ):
        a, _ = await _resolve_source_path(
            "doc-a", "s3://kb-uploads/doc-a/contract.pdf",
        )
        b, _ = await _resolve_source_path(
            "doc-b", "s3://kb-uploads/doc-b/contract.pdf",
        )

    assert a.parent != b.parent
    assert a.parent.name == "doc-a"
    assert b.parent.name == "doc-b"

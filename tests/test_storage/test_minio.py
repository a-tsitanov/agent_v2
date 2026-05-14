"""Unit tests for `src.storage.minio.MinioStorage`.

The MinIO SDK is mocked — we only verify that our wrapper calls the
right SDK methods, returns the expected S3 URI shape, and survives
`bucket_exists()` returning either True or False.
"""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.config import MinioSettings
from src.storage.minio import MinioStorage


def _cfg(tmp_path: Path) -> MinioSettings:
    return MinioSettings(
        endpoint="localhost:9000",
        access_key="ak",
        secret_key="sk",
        bucket="test-bucket",
        secure=False,
        region="us-east-1",
        download_dir=str(tmp_path / "cache"),
    )


def test_ensure_bucket_creates_when_absent(tmp_path: Path) -> None:
    with patch("src.storage.minio.Minio") as MinioCls:
        client = MinioCls.return_value
        client.bucket_exists.return_value = False

        storage = MinioStorage(_cfg(tmp_path))
        storage.ensure_bucket()

        client.bucket_exists.assert_called_once_with("test-bucket")
        client.make_bucket.assert_called_once_with("test-bucket")


def test_ensure_bucket_noop_when_present(tmp_path: Path) -> None:
    with patch("src.storage.minio.Minio") as MinioCls:
        client = MinioCls.return_value
        client.bucket_exists.return_value = True

        storage = MinioStorage(_cfg(tmp_path))
        storage.ensure_bucket()

        client.bucket_exists.assert_called_once_with("test-bucket")
        client.make_bucket.assert_not_called()


def test_put_object_returns_s3_uri(tmp_path: Path) -> None:
    with patch("src.storage.minio.Minio") as MinioCls:
        storage = MinioStorage(_cfg(tmp_path))
        body = io.BytesIO(b"hello world")
        uri = storage.put_object(
            "abc/doc.txt", body, length=11, content_type="text/plain",
        )

        assert uri == "s3://test-bucket/abc/doc.txt"
        MinioCls.return_value.put_object.assert_called_once_with(
            "test-bucket", "abc/doc.txt", body, 11,
            content_type="text/plain",
        )


def test_get_object_to_path_creates_parent_and_calls_fget(tmp_path: Path) -> None:
    with patch("src.storage.minio.Minio") as MinioCls:
        client = MinioCls.return_value

        # Touch the file path the SDK would normally produce so the
        # `local.exists()` check downstream behaves the same way.
        def _fget(bucket: str, key: str, local: str) -> None:
            Path(local).parent.mkdir(parents=True, exist_ok=True)
            Path(local).write_bytes(b"x")

        client.fget_object.side_effect = _fget

        storage = MinioStorage(_cfg(tmp_path))
        local = tmp_path / "out" / "doc.txt"
        result = storage.get_object_to_path(
            "s3://test-bucket/abc/doc.txt", local,
        )

        assert result == local
        assert local.exists()
        client.fget_object.assert_called_once_with(
            "test-bucket", "abc/doc.txt", str(local),
        )


def test_parse_s3_uri_round_trip() -> None:
    bucket, key = MinioStorage.parse_s3_uri("s3://kb-uploads/uuid/file.pdf")
    assert bucket == "kb-uploads"
    assert key == "uuid/file.pdf"


def test_parse_s3_uri_rejects_local_path() -> None:
    with pytest.raises(ValueError, match="not an s3 uri"):
        MinioStorage.parse_s3_uri("/tmp/kb-uploads/file.pdf")


def test_parse_s3_uri_rejects_missing_key() -> None:
    with pytest.raises(ValueError, match="malformed s3 uri"):
        MinioStorage.parse_s3_uri("s3://bucket-only")


def test_build_minio_storage_is_cached(tmp_path: Path, monkeypatch) -> None:
    """`build_minio_storage` is `functools.cache`-decorated — repeated
    calls must return the same instance and only call `ensure_bucket`
    once per process."""
    from src.storage import minio as minio_mod

    minio_mod.build_minio_storage.cache_clear()
    with patch("src.storage.minio.Minio") as MinioCls:
        MinioCls.return_value.bucket_exists.return_value = True
        s1 = minio_mod.build_minio_storage()
        s2 = minio_mod.build_minio_storage()
        assert s1 is s2
        MinioCls.return_value.bucket_exists.assert_called_once()
    minio_mod.build_minio_storage.cache_clear()

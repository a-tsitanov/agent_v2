"""MinIO storage wrapper for user-uploaded documents.

`/api/v1/ingest` writes each upload here synchronously, and the worker
downloads the object back to a local staging directory before feeding
it to the LlamaIndex pipeline.  Postgres `documents.path` stores the
resulting ``s3://<bucket>/<doc_id>/<filename>`` URI.

The underlying ``minio`` SDK is synchronous; we expose a small typed
wrapper and let callers handle async via ``asyncio.to_thread``.
"""

from __future__ import annotations

import functools
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from loguru import logger
from minio import Minio
from minio.error import S3Error

from src.config import MinioSettings, settings


class MinioStorage:
    """Thin sync wrapper around the official MinIO client.

    Keeps the bucket name + download dir close to the actual API
    calls so callers (`/ingest` endpoint, Temporal worker activities)
    don't need to thread settings through themselves.
    """

    def __init__(self, cfg: MinioSettings) -> None:
        self._client = Minio(
            cfg.endpoint,
            access_key=cfg.access_key.get_secret_value(),
            secret_key=cfg.secret_key.get_secret_value(),
            secure=cfg.secure,
            region=cfg.region,
        )
        self._bucket = cfg.bucket
        self._download_dir = Path(cfg.download_dir)

    # ── bucket bootstrap ───────────────────────────────────────────

    def ensure_bucket(self) -> None:
        """Create the configured bucket if it doesn't already exist.

        Idempotent — safe to call on every worker / app startup.
        """
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)
            logger.info("minio  bucket created  name={b}", b=self._bucket)

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def download_dir(self) -> Path:
        return self._download_dir

    # ── put / get ──────────────────────────────────────────────────

    def put_object(
        self,
        key: str,
        stream: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload `stream` (already positioned at start) to ``bucket/key``.

        Returns the full ``s3://bucket/key`` URI suitable for storing
        in ``documents.path``.
        """
        self._client.put_object(
            self._bucket, key, stream, length, content_type=content_type,
        )
        return f"s3://{self._bucket}/{key}"

    def get_object_to_path(self, s3_uri: str, local: Path) -> Path:
        """Download the object identified by `s3_uri` to `local`.

        Creates parent directories as needed.  Returns the resolved
        local path on success.
        """
        bucket, key = self.parse_s3_uri(s3_uri)
        local.parent.mkdir(parents=True, exist_ok=True)
        self._client.fget_object(bucket, key, str(local))
        return local

    def stat_object(self, s3_uri: str) -> tuple[str, int, str]:
        """Return (filename, size_bytes, content_type) for an s3:// object.

        filename is the last path segment of the key.  Raises ``S3Error``
        (NoSuchKey) when the object is missing — the caller maps that to 404."""
        bucket, key = self.parse_s3_uri(s3_uri)
        info = self._client.stat_object(bucket, key)
        filename = key.rsplit("/", 1)[-1] or key
        content_type = info.content_type or "application/octet-stream"
        return filename, info.size, content_type

    def stream_object(
        self, s3_uri: str, *, chunk_size: int = 1 << 20,
    ) -> Iterator[bytes]:
        """Yield the object's bytes in chunks, releasing the HTTP
        connection in a finally block (minio's get_object response must
        be closed + released or the pool leaks)."""
        bucket, key = self.parse_s3_uri(s3_uri)
        resp = self._client.get_object(bucket, key)
        try:
            yield from resp.stream(chunk_size)
        finally:
            resp.close()
            resp.release_conn()

    # ── parsing helpers ────────────────────────────────────────────

    @staticmethod
    def parse_s3_uri(uri: str) -> tuple[str, str]:
        """Split an ``s3://bucket/key`` URI into ``(bucket, key)``.

        Raises ``ValueError`` for malformed input — callers should
        route legacy local paths (``/tmp/...``) through their own
        branch before reaching this helper.
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"not an s3 uri: {uri!r}")
        rest = uri[len("s3://") :]
        bucket, sep, key = rest.partition("/")
        if not sep or not bucket or not key:
            raise ValueError(f"malformed s3 uri: {uri!r}")
        return bucket, key


@functools.cache
def build_minio_storage() -> MinioStorage:
    """Module-level singleton.  Calls `ensure_bucket()` on first build
    so the rest of the app can assume the bucket exists.
    """
    storage = MinioStorage(settings.minio)
    storage.ensure_bucket()
    return storage


__all__ = [
    "MinioStorage",
    "S3Error",
    "build_minio_storage",
]

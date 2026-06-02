"""Unit tests for MinioStorage stat/stream (stub minio client)."""

from __future__ import annotations

from types import SimpleNamespace

from src.config import settings
from src.storage.minio import MinioStorage


class _FakeResp:
    def __init__(self, chunks):
        self._chunks = chunks
        self.closed = False
        self.released = False

    def stream(self, n):
        yield from self._chunks

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class _FakeClient:
    def __init__(self, resp):
        self._resp = resp
        self.stat_args = None
        self.get_args = None

    def stat_object(self, bucket, key):
        self.stat_args = (bucket, key)
        return SimpleNamespace(size=11, content_type="application/pdf")

    def get_object(self, bucket, key):
        self.get_args = (bucket, key)
        return self._resp


def _storage(client):
    s = MinioStorage(settings.minio)
    s._client = client
    return s


def test_stat_object_returns_name_size_type():
    s = _storage(_FakeClient(_FakeResp([])))
    name, size, ctype = s.stat_object("s3://b/doc-1/report.pdf")
    assert name == "report.pdf"
    assert size == 11
    assert ctype == "application/pdf"
    assert s._client.stat_args == ("b", "doc-1/report.pdf")


def test_stream_object_yields_and_releases():
    resp = _FakeResp([b"hello ", b"world"])
    s = _storage(_FakeClient(resp))
    out = b"".join(s.stream_object("s3://b/doc-1/report.pdf"))
    assert out == b"hello world"
    assert resp.closed and resp.released  # connection released in finally

"""Tests for `src.storage.chunk_repository.ChunkRepository`.

Stubs Milvus + Postgres; verifies the JSON-row normalisation, the
position-based sort, the file-size cap, and the soft-fail behavior
when documents aren't registered.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.storage.chunk_repository import (
    DEFAULT_CHUNK_PAGE_LIMIT,
    ChunkRepository,
    _normalise_chunk_row,
    _read_file_capped,
)
from src.storage.postgres import DocumentRow


# ── stubs ────────────────────────────────────────────────────────────


@dataclass
class _StubMilvus:
    rows: list[dict]
    calls: list[dict] = field(default_factory=list)

    def query(self, *, collection_name, filter, output_fields, limit, offset):
        self.calls.append({
            "collection_name": collection_name,
            "filter": filter, "limit": limit, "offset": offset,
        })
        return self.rows


@dataclass
class _StubPG:
    docs: dict[str, str]  # doc_id (str) → path

    async def get(self, doc_id: uuid.UUID) -> DocumentRow | None:
        path = self.docs.get(str(doc_id))
        if path is None:
            return None
        import datetime as dt

        return DocumentRow(
            id=doc_id, path=path, department="", doc_type="txt",
            status="completed", error="", summary="",
            created_at=dt.datetime.now(), updated_at=dt.datetime.now(),
        )


# ── row normalisation ───────────────────────────────────────────────


def test_normalise_top_level_fields() -> None:
    row = {
        "id": "c1",
        "text": "hello",
        "doc_id": "d1",
        "position": 3,
        "file_path": "/tmp/x.txt",
    }
    out = _normalise_chunk_row(row)
    assert out["chunk_id"] == "c1"
    assert out["text"] == "hello"
    assert out["position"] == 3


def test_normalise_metadata_json_string() -> None:
    """LlamaIndex sometimes stores metadata as a JSON string."""
    row = {
        "id": "c2",
        "text": "world",
        "metadata": '{"doc_id": "d2", "position": 7, "file_path": "/x"}',
    }
    out = _normalise_chunk_row(row)
    assert out["doc_id"] == "d2"
    assert out["position"] == 7


def test_normalise_falls_back_when_metadata_corrupt() -> None:
    row = {"id": "c3", "text": "x", "metadata": "{not-json"}
    out = _normalise_chunk_row(row)
    # No raise; missing fields default sensibly.
    assert out["chunk_id"] == "c3"
    assert out["doc_id"] == ""


# ── file capping ────────────────────────────────────────────────────


def test_read_file_capped_returns_full_when_under_limit(tmp_path: Path) -> None:
    p = tmp_path / "x.txt"
    p.write_text("hello world", encoding="utf-8")
    assert _read_file_capped(p, max_chars=100) == "hello world"


def test_read_file_capped_truncates_when_over_limit(tmp_path: Path) -> None:
    p = tmp_path / "x.txt"
    p.write_text("a" * 1000, encoding="utf-8")
    out = _read_file_capped(p, max_chars=100)
    assert out.startswith("a" * 100)
    assert "truncated" in out
    assert "1,000 chars" in out


def test_read_file_capped_handles_non_utf8(tmp_path: Path) -> None:
    p = tmp_path / "x.bin"
    p.write_bytes(b"\xff\xfe hello \xff")
    out = _read_file_capped(p, max_chars=100)
    assert "hello" in out


# ── ChunkRepository integration ─────────────────────────────────────


@pytest.mark.asyncio
async def test_get_chunks_by_doc_id_sorts_by_position() -> None:
    milvus = _StubMilvus(rows=[
        {"id": "c3", "text": "third", "doc_id": "d1", "position": 2},
        {"id": "c1", "text": "first", "doc_id": "d1", "position": 0},
        {"id": "c2", "text": "second", "doc_id": "d1", "position": 1},
    ])
    repo = ChunkRepository(
        milvus_client=milvus, collection="kb_llamaindex", pg=_StubPG(docs={}),
    )
    chunks = await repo.aget_chunks_by_doc_id("d1")
    positions = [c["position"] for c in chunks]
    assert positions == [0, 1, 2]
    assert [c["text"] for c in chunks] == ["first", "second", "third"]


@pytest.mark.asyncio
async def test_get_chunks_by_doc_id_uses_filter() -> None:
    milvus = _StubMilvus(rows=[])
    repo = ChunkRepository(
        milvus_client=milvus, collection="some_coll", pg=_StubPG(docs={}),
    )
    await repo.aget_chunks_by_doc_id("d42", limit=5, offset=10)
    assert milvus.calls[0]["filter"] == 'doc_id == "d42"'
    assert milvus.calls[0]["limit"] == 5
    assert milvus.calls[0]["offset"] == 10
    assert milvus.calls[0]["collection_name"] == "some_coll"


@pytest.mark.asyncio
async def test_get_document_path_returns_postgres_value() -> None:
    valid_id = str(uuid.uuid4())
    repo = ChunkRepository(
        milvus_client=_StubMilvus(rows=[]),
        collection="x",
        pg=_StubPG(docs={valid_id: "/tmp/file.txt"}),
    )
    assert await repo.aget_document_path(valid_id) == "/tmp/file.txt"
    # Bogus id (not a UUID) → None, no raise
    assert await repo.aget_document_path("not-a-uuid") is None
    # Unknown UUID → None
    assert await repo.aget_document_path(str(uuid.uuid4())) is None


@pytest.mark.asyncio
async def test_read_document_text_returns_capped(tmp_path: Path) -> None:
    f = tmp_path / "doc.txt"
    f.write_text("a" * 500)
    valid_id = str(uuid.uuid4())
    repo = ChunkRepository(
        milvus_client=_StubMilvus(rows=[]),
        collection="x",
        pg=_StubPG(docs={valid_id: str(f)}),
    )
    out = await repo.aread_document_text(valid_id, max_chars=200)
    assert out is not None
    assert out.startswith("a" * 200)
    assert "truncated" in out


@pytest.mark.asyncio
async def test_read_document_text_missing_file_returns_none(tmp_path: Path) -> None:
    valid_id = str(uuid.uuid4())
    repo = ChunkRepository(
        milvus_client=_StubMilvus(rows=[]),
        collection="x",
        pg=_StubPG(docs={valid_id: str(tmp_path / "ghost.txt")}),
    )
    assert await repo.aread_document_text(valid_id) is None

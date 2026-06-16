"""Doc-id / file-path access layer for chunks and source files.

Wraps three lower-level stores into one ergonomic interface used by
the ReAct agent tools `get_chunks_by_doc_id` and `read_full_document`:

* **Milvus** — the chunk store.  Filter-query by `doc_id` metadata
  returns every chunk of one document in deterministic order.
* **Postgres** — the document-status table (`documents`).  Maps a
  `doc_id` UUID to the on-disk `path` that the worker saved.
* **Filesystem** — the API upload directory (`API_UPLOAD_DIR`).
  Streams the source file back when the agent asks for the full
  document, with a `max_chars` guard against blowing up the LLM
  context.

The repository is async-friendly throughout.  Real Milvus + Postgres
sit behind it in production; tests inject stub implementations.
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger
from pymilvus import MilvusClient

from src.config import settings
from src.storage.postgres import AsyncPostgres

if TYPE_CHECKING:
    from src.storage.minio import MinioStorage


# Hard cap on `read_full_document` output to avoid context blowup
# even when the agent asks for a 5 MB document.  Calibrated for
# gpt-4o-mini 128k ctx — leaves headroom for the system prompt,
# previously-accumulated tool observations, and the upcoming
# synthesizer call.
DEFAULT_MAX_DOC_CHARS = 60_000

# Max chunks returned by `get_chunks_by_doc_id` per call.  The agent
# can paginate via `offset` if it really needs more.
DEFAULT_CHUNK_PAGE_LIMIT = 200


class ChunkRepository:
    """Read-only access to chunks (Milvus) and source files
    (Postgres + filesystem) by `doc_id`.

    Constructed once in DI; injected into `agentic_react_search` so
    the agent can call `get_chunks_by_doc_id` / `read_full_document`
    without each tool re-opening clients.
    """

    def __init__(
        self,
        *,
        milvus_client: MilvusClient | None = None,
        collection: str | None = None,
        pg: AsyncPostgres | None = None,
        minio: "MinioStorage | None" = None,
    ) -> None:
        self._collection = collection or settings.milvus.collection
        self._client = milvus_client or MilvusClient(
            uri=settings.milvus.uri,
            timeout=settings.milvus.timeout_s,
        )
        self._pg = pg or AsyncPostgres()
        # Lazily resolved on first s3:// read so tests / Milvus-only
        # callers don't pay for a MinIO client + bucket bootstrap.
        self._minio = minio

    # ── Milvus chunk fetch ──────────────────────────────────────────

    async def aget_chunks_by_doc_id(
        self,
        doc_id: str,
        *,
        limit: int = DEFAULT_CHUNK_PAGE_LIMIT,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Return chunks belonging to one document, ordered by
        `position` metadata if present (LlamaIndex SentenceSplitter
        sets it; otherwise Milvus returns its own ordering).

        Result shape per chunk::

            {
                "chunk_id": str,
                "text":     str,
                "doc_id":   str,
                "position": int,
                "file_path": str,
            }
        """
        # MilvusClient.query is sync; offload so the worker event loop
        # isn't blocked on a slow filter scan.
        return await asyncio.to_thread(
            self._query_chunks, doc_id, limit, offset,
        )

    def _query_chunks(
        self, doc_id: str, limit: int, offset: int,
    ) -> list[dict[str, Any]]:
        # `doc_id` is client-controlled (MCP-2 read_full_document /
        # get_chunks_by_doc_id tools pass it through).  It is
        # interpolated into a Milvus filter expression below, so it MUST
        # be a well-formed UUID — otherwise an attacker could inject
        # filter syntax (e.g. `" or chunk_id != "`).  Mirror the guard
        # in `aget_document_path`.  `_escape` stays as defense in depth.
        try:
            uuid.UUID(str(doc_id))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(
                f"doc_id must be a valid UUID, got: {doc_id!r}"
            ) from exc
        # LlamaIndex's MilvusVectorStore stores chunk text under
        # `text` and metadata fields are flat top-level columns when
        # the schema was created with metadata_field_names; with the
        # default dynamic-fields schema metadata lives under
        # `metadata` (JSON).  We probe both — the cheaper top-level
        # filter is tried first.
        try:
            rows = self._client.query(
                collection_name=self._collection,
                filter=f'doc_id == "{_escape(doc_id)}"',
                output_fields=["*"],
                limit=limit,
                offset=offset,
            )
        except Exception as exc:  # noqa: BLE001 — try fallback
            logger.debug(
                "milvus top-level filter failed, falling back to JSON: {err}",
                err=exc,
            )
            rows = self._client.query(
                collection_name=self._collection,
                filter=(
                    f'metadata["doc_id"] == "{_escape(doc_id)}"'
                ),
                output_fields=["*"],
                limit=limit,
                offset=offset,
            )

        normalised = [_normalise_chunk_row(r) for r in rows]
        # Sort by metadata.position when available (SentenceSplitter
        # puts an int there) so callers see chunks in source order.
        normalised.sort(key=lambda c: (c.get("position") or 0))
        return normalised

    # ── Postgres → filesystem read ──────────────────────────────────

    async def aget_document_path(self, doc_id: str) -> str | None:
        """Return on-disk path for the source file of `doc_id` or
        None if the document isn't registered."""
        try:
            row = await self._pg.get(uuid.UUID(doc_id))
        except (ValueError, TypeError):
            return None
        return row.path if row else None

    async def aread_document_text(
        self,
        doc_id: str,
        *,
        max_chars: int = DEFAULT_MAX_DOC_CHARS,
    ) -> str | None:
        """Read the source file from disk, capped to `max_chars`.

        Returns None if the document isn't registered or the file
        is missing.  Caller is responsible for surfacing that as a
        clean tool-output ("document not found").
        """
        path_str = await self.aget_document_path(doc_id)
        if path_str is None:
            return None
        # documents.path is normally an ``s3://`` URI (MinIO); legacy
        # rows may hold a local filesystem path.  Mirror the download
        # route (api/routes/documents.py) instead of stat-ing the URI
        # as if it were a local file.
        if path_str.startswith("s3://"):
            return await asyncio.to_thread(
                self._read_s3_capped, path_str, max_chars,
            )
        path = Path(path_str)
        if not path.is_file():
            logger.warning(
                "read_document doc_id={d} path missing on disk: {p}",
                d=doc_id, p=path_str,
            )
            return None
        return await asyncio.to_thread(_read_file_capped, path, max_chars)

    def _read_s3_capped(self, s3_uri: str, max_chars: int) -> str | None:
        """Download an ``s3://`` object to the local cache and read it,
        capped.  Soft-fails to None (→ "document not found") on any
        storage error, matching the tool contract."""
        storage = self._minio
        if storage is None:
            from src.storage.minio import build_minio_storage

            storage = self._minio = build_minio_storage()
        try:
            _, key = storage.parse_s3_uri(s3_uri)
            local = Path(storage.download_dir) / key
            storage.get_object_to_path(s3_uri, local)
        except Exception as exc:  # noqa: BLE001 — soft-fail to not-found
            logger.warning(
                "read_document s3 fetch failed uri={u}: {e}", u=s3_uri, e=exc,
            )
            return None
        return _read_file_capped(local, max_chars)


def _escape(value: str) -> str:
    """Minimal escaper for Milvus filter expressions — double-quotes
    only since we already wrap the value in double quotes."""
    return value.replace('"', '\\"')


def _normalise_chunk_row(row: dict[str, Any]) -> dict[str, Any]:
    """Project a Milvus row into the chunk shape ReAct tools expect."""
    metadata = row.get("metadata") or {}
    if isinstance(metadata, str):
        # Some Milvus versions return JSON as string; parse once.
        import json

        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}

    def _pick(*keys: str) -> Any:
        for k in keys:
            v = row.get(k) if k in row else metadata.get(k)
            if v not in (None, ""):
                return v
        return None

    return {
        "chunk_id": str(_pick("node_id", "id", "_node_id") or ""),
        "text": str(_pick("text", "_node_content") or "")[:5000],
        "doc_id": str(_pick("doc_id") or ""),
        "position": int(_pick("position") or 0),
        "file_path": str(_pick("file_path") or ""),
    }


def _read_file_capped(path: Path, max_chars: int) -> str:
    """Read a file with a soft cap on size.  Tries utf-8 first,
    falls back to latin-1 for any byte stream."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="latin-1", errors="replace")
    if len(text) > max_chars:
        suffix = (
            f"\n\n…(truncated; full document is {len(text):,} chars, "
            f"showing first {max_chars:,})"
        )
        text = text[:max_chars] + suffix
    return text


__all__ = [
    "DEFAULT_CHUNK_PAGE_LIMIT",
    "DEFAULT_MAX_DOC_CHARS",
    "ChunkRepository",
]

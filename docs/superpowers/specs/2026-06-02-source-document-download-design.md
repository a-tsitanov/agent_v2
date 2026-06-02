# Source document download — design

**Date:** 2026-06-02
**Status:** approved (design), pending implementation plan

## Goal

Let an operator download the **original uploaded file** of an ingested
document for manual reading, on demand, keyed by the `doc_id` that
search responses already expose in `sources[]`.

## Context (verified)

- Ingest uploads the original file to MinIO at
  `s3://{MINIO_BUCKET}/{doc_id}/{filename}` and stores that **s3:// URI**
  in Postgres `documents.path` (`src/api/routes/ingest.py`).
- The worker's `finalize` activity cleans up only the **staging** pickles
  (`staging.delete_prefix(workflow_run_id)`) and the local download dir —
  it does **not** delete the original upload. So the original is retained
  and retrievable post-ingest.
- `MinioStorage` (`src/storage/minio.py`) currently exposes `put_object`,
  `get_object_to_path`, `parse_s3_uri` — no streaming-read or stat method
  yet.
- `ChunkRepository.aread_document_text` is **broken** for MinIO docs
  (does `Path(s3_uri).is_file()`). Out of scope here (separate bug); this
  feature reads MinIO directly.

## Decisions

| Question | Decision |
|---|---|
| What to return | The **original file** as stored in MinIO (PDF/DOCX/TXT/…) |
| Delivery | **Stream through the API** (MinIO stays internal; auth via X-API-Key) |
| doc_id discovery | From search `sources[].doc_id` — **no** list/browse endpoint |
| HEAD endpoint | **Dropped** (YAGNI; can add later) |
| MCP tool | **Out of scope** (manual reading by a human, not an LLM tool) |

## Surface

New router `src/api/routes/documents.py`, mounted in `src/api/main.py`
under prefix `/api/v1`:

```
GET /api/v1/documents/{doc_id}    # download the original file (attachment)
```

- Dependency: `Depends(require_api_key)` (`src/api/auth.py`).
- Response 200: streamed bytes with
  `Content-Disposition: attachment; filename="<original filename>"`,
  `Content-Type` from the stored object, `Content-Length` from stat.

## Data flow

```
doc_id
  → AsyncPostgres.get(UUID(doc_id))            # documents row
  → row.path  ("s3://bucket/doc_id/filename")
  → MinioStorage.parse_s3_uri → (bucket, key)
  → MinioStorage.stat_object  → (filename, size, content_type)
  → MinioStorage.stream_object → Iterator[bytes]  (chunked)
  → fastapi.responses.StreamingResponse(..., headers=...)
```

The route injects `pg: FromDishka[AsyncPostgres]` (same as ingest) and
builds the MinIO singleton via `build_minio_storage()`.

## New `MinioStorage` methods (`src/storage/minio.py`)

```python
def stat_object(self, s3_uri: str) -> tuple[str, int, str]:
    """Return (filename, size_bytes, content_type) for an s3:// object.
    filename is the last path segment of the key."""

def stream_object(self, s3_uri: str, *, chunk_size: int = 1 << 20) -> Iterator[bytes]:
    """Yield the object's bytes in chunks. Wraps minio get_object();
    guarantees response.close()/release_conn() in a finally block."""
```

Both reuse `parse_s3_uri`. `stat_object` → minio `client.stat_object`;
`stream_object` → minio `client.get_object` (urllib3 response, streamed
then released).

## Error handling

| Condition | Status | Body |
|---|---|---|
| `doc_id` not a UUID / not in Postgres | 404 | `document not found` |
| `path` is `s3://` but object missing (`S3Error NoSuchKey`) | 404 | `document source not available` |
| `path` is a legacy **local** path (pre-MinIO) | stream from disk if it exists, else 404 |
| MinIO unreachable | 503 | `storage unavailable` |

Mirror the ingest route's MinIO error handling (`S3Error` → mapped HTTP;
unexpected connection errors → 503).

## Testing (`tests/test_api/test_documents.py`)

Follow the existing `tests/test_api` pattern (FastAPI `TestClient` +
stubbed Postgres/MinIO via dependency overrides / monkeypatch):

- **200** — stub Postgres returns an s3 path, stub MinIO returns bytes →
  assert body equals the bytes, `Content-Disposition` carries the
  filename, `Content-Type` propagated.
- **404** — unknown `doc_id`; and `doc_id` present but object missing.
- **401** — no/invalid API key.
- **(unit)** `parse_s3_uri` + `stat_object`/`stream_object` against a stub
  minio client (filename extraction, chunking, connection release).

## Out of scope (separate tasks if needed)

- MCP download tool.
- List/browse documents endpoint.
- Fixing `ChunkRepository.aread_document_text` for s3:// paths.
- HEAD endpoint for metadata-only.

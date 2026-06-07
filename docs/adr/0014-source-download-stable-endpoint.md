# ADR-0014: Source-document download via a stable API endpoint (not presigned URLs)

- Status: Accepted
- Date: 2026-06-07

## Context

Search responses expose `doc_id`s in `sources[]`/`documents[]`, and the
continuous wiki editor (ADR-0012) needs to link each article back to the
original source files (the `== Источники ==` section). Original files live in
MinIO (the URI is stored in Postgres `documents.path`). Presigned object-store
URLs would be a natural download mechanism but they expire, leak the storage
backend, bypass the application's auth, and would change every time they are
regenerated — unstable to embed in long-lived wiki pages.

## Decision

Serve downloads through a **stable, auth-guarded API endpoint**
`GET /api/v1/documents/{doc_id}`. It looks up `documents.path` in Postgres and
streams the file from MinIO (`stat_object` + `stream_object`), with an RFC 6266
sanitized `Content-Disposition`, a 404 for missing docs/objects, and a 503 when
storage is unreachable; legacy local-path docs stream from disk. The wiki
editor builds the `== Источники ==` links **deterministically** (not
LLM-generated) as `{docs_base_url}/documents/{doc_id}` pointing at this
endpoint.

## Consequences

- One stable, permanent, auth-checked URL per document that is safe to embed in
  wiki pages and search results; the storage backend stays hidden behind the API.
- The API streams every download (no offloading to object-store CDNs / presigned
  direct fetch); accepted for a controlled internal KB.
- The source links are deterministic and outside the LLM-owned bot prose, so
  they cannot be hallucinated or drift.

## Alternatives considered

- **Presigned MinIO URLs** — expire, leak the backend, bypass app auth, and
  change on regeneration → unstable for wiki embedding.
- **LLM-generated source links** — risk hallucinated/incorrect URLs; the links
  are built deterministically instead.

## References

- `src/api/routes/documents.py`, `src/workflow/wiki/article.py` (`_fmt_sources`,
  `render_bot_section`), `src/storage/minio.py`, `src/storage/postgres.py`
- CONCEPTS.md → "Source download & provenance links"

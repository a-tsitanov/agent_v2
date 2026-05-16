"""Idempotent DB initialisation.

Stage 1 scope:
  * Postgres — create ``documents`` table tracking ingestion job
    status (mirrors enterprise-kb schema for easy diffing).
  * Milvus — connectivity ping.  The actual collection is created
    by ``llama_index.vector_stores.milvus.MilvusVectorStore`` on
    first insert (Stage 3).

Neo4j and RabbitMQ provisioning land in later stages.

Usage::

    python -m scripts.setup_db
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from loguru import logger  # noqa: E402

from src.config import settings  # noqa: E402
from src.utils.logging import configure_logging  # noqa: E402


# ── Postgres ─────────────────────────────────────────────────────────


_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS documents (
    id           UUID PRIMARY KEY,
    path         TEXT NOT NULL,
    department   TEXT DEFAULT '',
    doc_type     TEXT DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'completed',
                                   'vector_only', 'failed')),
    error        TEXT DEFAULT '',
    summary      TEXT DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status);

CREATE INDEX IF NOT EXISTS documents_department_idx
    ON documents (department);
"""


def setup_postgres() -> None:
    pg = settings.postgres
    logger.info(
        "postgres setup  host={h}:{p}  db={d}",
        h=pg.host, p=pg.port, d=pg.db,
    )
    with psycopg.connect(
        pg.dsn, connect_timeout=pg.connect_timeout_s, autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(_DOCUMENTS_DDL)
    logger.info("postgres setup  done")


# ── Milvus ───────────────────────────────────────────────────────────


def setup_milvus() -> None:
    """Verify Milvus reachable; collection lifecycle owned by
    ``MilvusVectorStore`` (Stage 3)."""
    from pymilvus import MilvusClient

    mv = settings.milvus
    logger.info("milvus connectivity  uri={u}", u=mv.uri)
    client = MilvusClient(uri=mv.uri, timeout=mv.timeout_s)
    try:
        collections = client.list_collections()
        logger.info(
            "milvus reachable  existing_collections={c}",
            c=collections,
        )
    finally:
        client.close()


# ── MinIO ────────────────────────────────────────────────────────────


def setup_minio() -> None:
    """Ensure the user-upload bucket exists.  Same MinIO instance the
    Milvus backend uses; this just adds an extra bucket alongside it.
    """
    from src.storage.minio import build_minio_storage

    mn = settings.minio
    logger.info("minio bucket  endpoint={e}  bucket={b}", e=mn.endpoint, b=mn.bucket)
    storage = build_minio_storage()
    storage.ensure_bucket()
    logger.info("minio bucket  done")


# ── entry point ──────────────────────────────────────────────────────


def main() -> None:
    configure_logging(level=settings.api.log_level, json_output=False)
    setup_postgres()
    setup_milvus()
    setup_minio()
    logger.info("setup_db  all done")


if __name__ == "__main__":
    main()

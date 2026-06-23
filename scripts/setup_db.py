"""Idempotent DB initialisation.

Stage 1 scope:
  * Postgres — create ``documents`` table tracking ingestion job
    status (mirrors enterprise-kb schema for easy diffing).
  * Milvus — connectivity ping.  The actual collection is created
    by ``llama_index.vector_stores.milvus.MilvusVectorStore`` on
    first insert (Stage 3).

Neo4j provisioning lands in later stages.

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
    doc_date     DATE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent add for pre-existing deployments (CREATE TABLE IF NOT EXISTS
-- does not add a new column to an already-created table).
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date DATE;

CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status);

CREATE INDEX IF NOT EXISTS documents_department_idx
    ON documents (department);

CREATE INDEX IF NOT EXISTS documents_doc_date_idx
    ON documents (doc_date);
"""


_INGEST_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS ingest_metrics (
    id              BIGSERIAL PRIMARY KEY,
    doc_id          UUID        NOT NULL,
    workflow_id     TEXT        NOT NULL,
    workflow_run_id TEXT        NOT NULL,
    activity_name   TEXT        NOT NULL,
    attempt         INT         NOT NULL DEFAULT 1,
    duration_ms     BIGINT      NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL,
    version_tag     TEXT,
    model           TEXT,
    env             TEXT,
    UNIQUE (workflow_run_id, activity_name, attempt)
);

CREATE INDEX IF NOT EXISTS ingest_metrics_doc_id_idx
    ON ingest_metrics (doc_id);
CREATE INDEX IF NOT EXISTS ingest_metrics_version_tag_idx
    ON ingest_metrics (version_tag);
CREATE INDEX IF NOT EXISTS ingest_metrics_completed_at_idx
    ON ingest_metrics (completed_at);
CREATE INDEX IF NOT EXISTS ingest_metrics_activity_idx
    ON ingest_metrics (activity_name);
"""


_ANALYTICS_SEARCH_ATTRS: dict[str, str] = {
    "VersionTag":      "KEYWORD",
    "Model":           "KEYWORD",   # global default snapshot
    "ExtractionModel": "KEYWORD",   # per-role snapshot
    "JudgeModel":      "KEYWORD",
    "SearchModel":     "KEYWORD",
    "Env":             "KEYWORD",
}


def setup_temporal_search_attributes() -> None:
    """Register the three custom Search Attributes used by the
    analytics layer (Stage 4 of the Grafana plan) via the Temporal
    SDK OperatorService.  Idempotent — AlreadyExists is logged as
    a no-op; other failures are warnings (the workflow falls back
    to no search-attribute filtering in Temporal UI, but the
    Postgres ingest_metrics path is unaffected).
    """
    import asyncio
    from temporalio.api.enums.v1 import IndexedValueType
    from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
    from temporalio.client import Client

    name_to_enum = {
        name: getattr(IndexedValueType, f"INDEXED_VALUE_TYPE_{kind}")
        for name, kind in _ANALYTICS_SEARCH_ATTRS.items()
    }

    async def _register() -> None:
        client = await Client.connect(
            settings.temporal.target,
            namespace=settings.temporal.namespace,
        )
        op = client.operator_service
        req = AddSearchAttributesRequest(
            namespace=settings.temporal.namespace,
            search_attributes=name_to_enum,
        )
        try:
            await op.add_search_attributes(req)
            logger.info(
                "search-attrs registered  names={n}",
                n=list(_ANALYTICS_SEARCH_ATTRS.keys()),
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "already exists" in msg or "alreadyexist" in msg:
                logger.info("search-attrs already registered (no-op)")
            elif "advanced visibility" in msg or "not supported" in msg:
                logger.warning(
                    "Temporal visibility store does not support custom "
                    "search attrs (Postgres visibility — needs ES for advanced).  "
                    "Skipping — analytics still works via ingest_metrics. "
                    "err={e}", e=exc,
                )
            else:
                logger.warning("search-attr register failed  err={e}", e=exc)

    asyncio.run(_register())


def setup_temporal_retention() -> None:
    """Enforce the configured closed-workflow retention on the namespace.

    Bounds Postgres history growth: without this, per-document
    DocumentIngest / GraphBuild histories pile up in the shared DB.
    Idempotent (UpdateNamespace is a no-op when the TTL already
    matches); failures are warnings so init never blocks on it.
    ``namespace_retention_days <= 0`` leaves the namespace untouched.
    """
    days = settings.temporal.namespace_retention_days
    if days <= 0:
        logger.info("temporal retention unset (days<=0) — leaving namespace as-is")
        return

    import asyncio
    from google.protobuf.duration_pb2 import Duration
    from temporalio.api.namespace.v1 import NamespaceConfig
    from temporalio.api.workflowservice.v1 import UpdateNamespaceRequest
    from temporalio.client import Client

    async def _update() -> None:
        client = await Client.connect(
            settings.temporal.target,
            namespace=settings.temporal.namespace,
        )
        req = UpdateNamespaceRequest(
            namespace=settings.temporal.namespace,
            config=NamespaceConfig(
                workflow_execution_retention_ttl=Duration(
                    seconds=days * 24 * 3600,
                ),
            ),
        )
        try:
            await client.workflow_service.update_namespace(req)
            logger.info(
                "temporal retention set  namespace={ns}  days={d}",
                ns=settings.temporal.namespace, d=days,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("temporal retention update failed  err={e}", e=exc)

    asyncio.run(_update())


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
            cur.execute(_INGEST_METRICS_DDL)
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
    setup_temporal_search_attributes()
    setup_temporal_retention()
    logger.info("setup_db  all done")


if __name__ == "__main__":
    main()

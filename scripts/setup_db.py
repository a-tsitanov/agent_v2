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

import psycopg
from loguru import logger

from src.config import settings
from src.utils.logging import configure_logging

# ── Postgres ─────────────────────────────────────────────────────────


_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS documents (
    id           UUID PRIMARY KEY,
    path         TEXT NOT NULL,
    department   TEXT DEFAULT '',
    doc_type     TEXT DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'completed',
                                   'vector_only', 'failed', 'skipped')),
    error        TEXT DEFAULT '',
    summary      TEXT DEFAULT '',
    doc_date     DATE,
    source_channel TEXT NOT NULL DEFAULT '',
    source_group   TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent adds for pre-existing deployments (CREATE TABLE IF NOT EXISTS
-- does not add a new column to an already-created table).
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date DATE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_channel TEXT NOT NULL DEFAULT '';
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_group   TEXT NOT NULL DEFAULT '';

-- Widen the status domain to include 'skipped' (classifier skip, written by
-- finalize.mark_skipped) on already-created tables. The inline CHECK above is
-- auto-named documents_status_check; drop + re-add makes this idempotent.
ALTER TABLE documents DROP CONSTRAINT IF EXISTS documents_status_check;
ALTER TABLE documents ADD CONSTRAINT documents_status_check
    CHECK (status IN ('pending', 'processing', 'completed',
                      'vector_only', 'failed', 'skipped'));

CREATE INDEX IF NOT EXISTS documents_status_idx
    ON documents (status);

CREATE INDEX IF NOT EXISTS documents_department_idx
    ON documents (department);

CREATE INDEX IF NOT EXISTS documents_doc_date_idx
    ON documents (doc_date);

CREATE INDEX IF NOT EXISTS documents_source_channel_idx
    ON documents (source_channel);

CREATE INDEX IF NOT EXISTS documents_source_group_idx
    ON documents (source_group);
"""


# Backfill source_channel for historical Telegram rows from the filename
# embedded in `path` ({doc_id}/tg_<channel>_<msgid>.txt). Greedy .+ so
# tg_a_b_123.txt -> channel 'a_b', msgid '123'. Only touches still-empty TG
# rows, so re-running setup is cheap and idempotent. source_group cannot be
# recovered (never recorded historically) and stays ''.
_BACKFILL_SOURCE_CHANNEL_SQL = r"""
UPDATE documents
   SET source_channel = substring(path from 'tg_(.+)_[0-9]+\.txt$')
 WHERE source_channel = ''
   AND path ~ 'tg_.+_[0-9]+\.txt$';
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


_PG_TRGM_DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

_STAT_INDICATOR_DDL = """
CREATE TABLE IF NOT EXISTS stat_indicator (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source         TEXT    NOT NULL,
    code           TEXT    NOT NULL,
    title          TEXT    NOT NULL,
    question_text  TEXT    NOT NULL DEFAULT '',
    unit           TEXT    NOT NULL,
    value_kind     TEXT    NOT NULL,
    granularity    TEXT    NOT NULL,
    dims_schema    JSONB   NOT NULL DEFAULT '{}'::jsonb,
    entity_vid     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, code),
    CONSTRAINT stat_indicator_value_kind_check
        CHECK (value_kind IN ('share','level','rate','index')),
    CONSTRAINT stat_indicator_granularity_check
        CHECK (granularity IN ('day','week','month','quarter','year'))
);
"""

# `entity_vid` and `source_doc_id` are WEAK links on purpose — no
# foreign keys — so a graph rebuild or a document re-ingest can never
# invalidate a stored number.
_STAT_OBSERVATION_DDL = """
CREATE TABLE IF NOT EXISTS stat_observation (
    indicator_id   BIGINT  NOT NULL REFERENCES stat_indicator(id) ON DELETE CASCADE,
    period_start   DATE    NOT NULL,
    period_end     DATE    NOT NULL,
    dims           JSONB   NOT NULL DEFAULT '{}'::jsonb,
    value          NUMERIC NOT NULL,
    sample_n       INTEGER,
    revision       INTEGER NOT NULL DEFAULT 0,
    source_doc_id  UUID,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (indicator_id, period_start, dims, revision)
);
"""

_STAT_INDEXES_DDL = """
CREATE INDEX IF NOT EXISTS stat_observation_series_idx
    ON stat_observation (indicator_id, period_start);
CREATE INDEX IF NOT EXISTS stat_observation_dims_idx
    ON stat_observation USING GIN (dims);
CREATE INDEX IF NOT EXISTS stat_indicator_title_trgm_idx
    ON stat_indicator USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS stat_indicator_question_trgm_idx
    ON stat_indicator USING GIN (question_text gin_trgm_ops);
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
        except Exception as exc:
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
        except Exception as exc:
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
    ) as conn, conn.cursor() as cur:
        cur.execute(_DOCUMENTS_DDL)
        cur.execute(_INGEST_METRICS_DDL)
        cur.execute(_BACKFILL_SOURCE_CHANNEL_SQL)
        cur.execute(_PG_TRGM_DDL)
        cur.execute(_STAT_INDICATOR_DDL)
        cur.execute(_STAT_OBSERVATION_DDL)
        cur.execute(_STAT_INDEXES_DDL)
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

"""DESTRUCTIVE — wipe all data stores.

Drops:
  * Temporal   — terminate running + delete every workflow execution.
  * RabbitMQ   — purge every configured ingest queue + the DLQ.
  * Postgres   — TRUNCATE app tables (`documents`, `ingest_metrics`).
  * Milvus     — drop EVERY collection (chunks, community, ER, any `_bak*`).
  * Graph      — Nebula: DROP SPACE; Neo4j: DETACH DELETE (dispatched on
                 ``settings.graph.backend``).
  * MinIO      — empty the upload bucket + the workflow staging bucket.
                 Milvus's own segment bucket (`a-bucket`) is left alone.
  * MediaWiki  — delete wiki-editor article pages (except `Main Page`).
  * Filesystem — clear API upload dir + ingestion cache (if local).

Then runs `setup_db.py` so the next ingest finds clean schemas.  The
Milvus collection + Nebula space are re-created LAZILY on the next write
(``MilvusVectorStore`` on first insert; ``nebula_schema.ensure_schema``
on first graph write), so they come back at the CURRENT ``MILVUS_DIM``.

For a race-free wipe, stop the live writers first so nothing re-creates a
store mid-wipe::

    docker compose -f docker-compose.prod.yml stop ingest-consumer worker
    # ... run this script ...
    docker compose -f docker-compose.prod.yml start ingest-consumer worker

(the ``worker`` container is the graph/vector writer; ``ingest-consumer``
pulls the RabbitMQ queue).  Any in-flight ingest left running will fail
mid-flight (its `documents` row disappears).  Note the stack re-ingests
from the LIVE Telegram feed after restart — a full wipe does not stop new
messages arriving.

Every step is fail-open: an unreachable store logs a warning and the wipe
continues.

Usage::

    uv run python -m scripts.wipe_db                 # interactive confirm
    uv run python -m scripts.wipe_db --yes           # no prompt (CI / scripts)
    uv run python -m scripts.wipe_db --keep-files    # skip uploads/cache
    uv run python -m scripts.wipe_db --keep-wiki     # skip MediaWiki pages
    uv run python -m scripts.wipe_db --keep-minio    # skip MinIO buckets
    uv run python -m scripts.wipe_db --keep-rabbit   # skip RabbitMQ purge
    uv run python -m scripts.wipe_db --keep-temporal # skip Temporal delete
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from src.config import settings
from src.utils.logging import configure_logging


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--yes", "-y", action="store_true",
                   help="skip the interactive confirmation prompt.")
    p.add_argument("--keep-files", action="store_true",
                   help="don't touch upload dir / ingestion cache.")
    p.add_argument("--keep-wiki", action="store_true",
                   help="don't delete MediaWiki wiki-editor article pages.")
    p.add_argument("--keep-minio", action="store_true",
                   help="don't empty the MinIO upload / staging buckets.")
    p.add_argument("--keep-rabbit", action="store_true",
                   help="don't purge the RabbitMQ ingest queues / DLQ.")
    p.add_argument("--keep-temporal", action="store_true",
                   help="don't terminate/delete Temporal workflow executions.")
    p.add_argument("--no-setup", action="store_true",
                   help="skip running setup_db after the wipe.")
    return p.parse_args()


# ── Postgres ─────────────────────────────────────────────────────────

# App tables (public schema).  Temporal's own databases live in the same
# Postgres server but a DIFFERENT database, so this never touches them.
_PG_TABLES = ("documents", "ingest_metrics")


def wipe_postgres() -> None:
    import psycopg

    pg = settings.postgres
    logger.info("postgres wipe  host={h}:{p}  db={d}",
                h=pg.host, p=pg.port, d=pg.db)
    with psycopg.connect(
        pg.dsn, connect_timeout=pg.connect_timeout_s, autocommit=True,
    ) as conn, conn.cursor() as cur:
        for table in _PG_TABLES:
            try:
                cur.execute(f"TRUNCATE TABLE {table}")
                logger.info("postgres  TRUNCATE {t}  ok", t=table)
            except psycopg.errors.UndefinedTable:
                logger.info("postgres  {t} table absent — nothing to wipe",
                            t=table)


# ── Milvus ───────────────────────────────────────────────────────────


def wipe_milvus() -> None:
    """Drop EVERY collection, not just ``settings.milvus.collection`` —
    the stack has chunks (`kb_llamaindex`), `community_report_vec`,
    `entity_er_vec`, plus any `_bak*` rollback clones from migrations."""
    from pymilvus import MilvusClient

    mv = settings.milvus
    client = MilvusClient(uri=mv.uri, timeout=mv.timeout_s)
    try:
        existing = client.list_collections()
        if not existing:
            logger.info("milvus  no collections — nothing to wipe")
            return
        for coll in existing:
            client.drop_collection(coll)
            logger.info("milvus  dropped collection={c}", c=coll)
        logger.info("milvus  dropped {n} collection(s)", n=len(existing))
    finally:
        client.close()


# ── Graph (Nebula / Neo4j) ───────────────────────────────────────────


def wipe_graph() -> None:
    """Dispatch on the configured graph backend (mirrors
    ``src.graph.store.build_graph_store``)."""
    backend = settings.graph.backend
    if backend == "nebula":
        wipe_nebula()
    else:
        wipe_neo4j()


def wipe_nebula() -> None:
    """DROP the whole Nebula space.  ``nebula_schema.ensure_schema``
    re-creates the space + tags + edges + indexes (all ``IF NOT EXISTS``)
    on the next graph write, so the app self-heals.  Nebula vectors are
    stored dim-agnostically (as strings), so there is no dim to change."""
    from nebula3.Config import Config
    from nebula3.gclient.net import ConnectionPool

    n = settings.nebula
    logger.info("nebula wipe  {h}:{p}  space={s}", h=n.host, p=n.port, s=n.space)
    pool = ConnectionPool()
    if not pool.init([(n.host, n.port)], Config()):
        logger.warning("nebula wipe  pool init failed — skipping graph wipe")
        return
    try:
        sess = pool.get_session(n.user, n.password.get_secret_value())
        try:
            r = sess.execute(f"DROP SPACE IF EXISTS `{n.space}`;")
            if r.is_succeeded():
                logger.info("nebula  dropped space={s}", s=n.space)
            else:
                logger.warning("nebula  DROP SPACE {s} failed: {e}",
                               s=n.space, e=r.error_msg())
        finally:
            sess.release()
    except Exception as exc:
        logger.warning("nebula wipe  skipped (graphd unreachable?): {e}", e=exc)
    finally:
        pool.close()


def wipe_neo4j() -> None:
    from neo4j import GraphDatabase

    nj = settings.neo4j
    logger.info("neo4j wipe  uri={u}  db={d}", u=nj.uri, d=nj.database)
    auth = (nj.user, nj.password.get_secret_value())
    with GraphDatabase.driver(nj.uri, auth=auth) as driver:
        with driver.session(database=nj.database) as s:
            res = s.run("MATCH (n) DETACH DELETE n RETURN count(*) AS n")
            row = res.single()
            n = row["n"] if row else 0
            logger.info("neo4j  detach-deleted nodes (sweep total: {n})", n=n)
            try:
                idx_res = s.run("SHOW INDEXES YIELD name")
                names = [r["name"] for r in idx_res]
                for name in names:
                    if name.startswith("__org_neo4j") or name == "index_343aff4e":
                        continue
                    try:
                        s.run(f"DROP INDEX `{name}`")
                        logger.info("neo4j  dropped index {n}", n=name)
                    except Exception as exc:
                        logger.warning("neo4j  drop index {n} failed: {e}",
                                       n=name, e=exc)
            except Exception as exc:
                logger.warning("neo4j  list indexes failed: {e}", e=exc)


# ── MinIO (upload + staging buckets) ─────────────────────────────────


def wipe_minio() -> None:
    """Empty the app buckets: user uploads (``settings.minio.bucket``) and
    the workflow staging bucket (``settings.temporal.staging_bucket``).

    Does NOT touch Milvus's own segment bucket (`a-bucket`) — Milvus shares
    this MinIO instance and manages that bucket itself (dropping the Milvus
    collections already reclaims it)."""
    from minio import Minio
    from minio.deleteobjects import DeleteObject

    m = settings.minio
    client = Minio(
        m.endpoint,
        access_key=m.access_key.get_secret_value(),
        secret_key=m.secret_key.get_secret_value(),
        secure=m.secure,
        region=m.region,
    )
    buckets = [m.bucket, settings.temporal.staging_bucket]
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                logger.info("minio  bucket {b} absent — nothing to wipe", b=bucket)
                continue
            dels = [DeleteObject(o.object_name)
                    for o in client.list_objects(bucket, recursive=True)]
            errors = list(client.remove_objects(bucket, dels))
            for err in errors:
                logger.warning("minio  delete error in {b}: {e}", b=bucket, e=err)
            logger.info("minio  emptied {b}: {n} object(s)", b=bucket, n=len(dels))
        except Exception as exc:
            logger.warning("minio  wipe {b} skipped (unreachable?): {e}",
                           b=bucket, e=exc)


# ── RabbitMQ (ingest queues + DLQ) ───────────────────────────────────


async def _wipe_rabbitmq_async() -> list[tuple[str, object]]:
    import aio_pika

    cfg = settings.rabbitmq
    purged: list[tuple[str, object]] = []
    conn = await aio_pika.connect_robust(cfg.url)
    async with conn:
        channel = await conn.channel()
        for name in [*cfg.queues, cfg.dlq]:
            try:
                queue = await channel.declare_queue(name, durable=True, passive=True)
                res = await queue.purge()
                count = getattr(res, "message_count", "?")
                purged.append((name, count))
                logger.info("rabbit  purged {q}  messages={n}", q=name, n=count)
            except Exception as exc:
                # Passive declare fails if the queue doesn't exist yet; also
                # covers a closed channel — reopen for the next queue.
                logger.warning("rabbit  purge {q} skipped: {e}", q=name, e=exc)
                if channel.is_closed:
                    channel = await conn.channel()
    return purged


def wipe_rabbitmq() -> None:
    """Purge every configured ingest queue + the DLQ.  Fail-open if the
    broker is unreachable (e.g. the Temporal ingest backend is in use)."""
    import asyncio

    try:
        asyncio.run(_wipe_rabbitmq_async())
    except Exception as exc:
        logger.warning("rabbit wipe  skipped (broker unreachable?): {e}", e=exc)


# ── files ────────────────────────────────────────────────────────────


def wipe_files() -> None:
    targets = [
        Path(settings.api.upload_dir),
        Path(settings.ingestion.cache_dir),
    ]
    for t in targets:
        if t.exists():
            shutil.rmtree(t, ignore_errors=True)
            logger.info("files  removed {p}", p=t)
        else:
            logger.info("files  {p} absent — nothing to wipe", p=t)


# ── MediaWiki (wiki-editor article pages) ────────────────────────────

# Pages never deleted by the wiki wipe (MediaWiki defaults, not KB output).
_WIKI_KEEP = {"Main Page"}


def _pages_to_delete(allpages, keep=_WIKI_KEEP) -> list[str]:
    """Titles to delete: every listed (main-namespace) page except keep-list."""
    return [p["title"] for p in allpages if p["title"] not in keep]


def wipe_wiki() -> None:
    """Best-effort delete of wiki-editor article pages from MediaWiki.

    Logs in with the MediaWiki admin account (``WIKIBASE_ADMIN_USER`` /
    ``WIKIBASE_ADMIN_PASS`` — sysop rights are needed to delete) and removes
    every main-namespace page except ``Main Page``.  Fail-open: if MediaWiki
    is unreachable or the admin login fails, logs a warning and returns, so a
    wipe still succeeds on stacks that don't run the wiki.

    Does NOT touch Wikibase Items/Properties (the ``setup_wikibase`` bootstrap
    owns those; per-entity QIDs were already removed with the graph wipe).
    """
    import httpx

    base = settings.wikibase.base_url.rstrip("/")
    api = f"{base}/w/api.php"
    user = os.environ.get("WIKIBASE_ADMIN_USER", "WikibaseAdmin")
    # The dev compose ships this admin password; override via env in real envs.
    password = os.environ.get("WIKIBASE_ADMIN_PASS", "ChangeMe-Wb-Admin-2026")
    try:
        with httpx.Client(timeout=settings.wikibase.timeout_s) as c:
            ltoken = c.get(api, params={
                "action": "query", "meta": "tokens", "type": "login",
                "format": "json"}).json()["query"]["tokens"]["logintoken"]
            login = c.post(api, data={
                "action": "login", "lgname": user, "lgpassword": password,
                "lgtoken": ltoken, "format": "json"}).json()
            if login.get("login", {}).get("result") != "Success":
                logger.warning(
                    "wiki wipe  admin login failed (user={u}) — skipping "
                    "MediaWiki page delete", u=user)
                return
            csrf = c.get(api, params={
                "action": "query", "meta": "tokens", "format": "json"
            }).json()["query"]["tokens"]["csrftoken"]
            allpages = c.get(api, params={
                "action": "query", "list": "allpages", "aplimit": 500,
                "format": "json"}).json()["query"]["allpages"]
            deleted = 0
            for title in _pages_to_delete(allpages):
                resp = c.post(api, data={
                    "action": "delete", "title": title, "reason": "wipe_db",
                    "token": csrf, "format": "json"}).json()
                if "delete" in resp:
                    deleted += 1
                else:
                    logger.warning("wiki wipe  delete failed title={t}: {e}",
                                   t=title,
                                   e=resp.get("error", {}).get("code", "?"))
            logger.info("wiki wipe  deleted {n} MediaWiki page(s) (kept {k})",
                        n=deleted, k=sorted(_WIKI_KEEP))
    except Exception as exc:
        logger.warning("wiki wipe  skipped (MediaWiki unreachable?): {e}", e=exc)


# ── Temporal (workflow executions) ───────────────────────────────────


async def _wipe_temporal_async() -> tuple[int, int]:
    from temporalio.api.common.v1 import WorkflowExecution as PbWorkflowExecution
    from temporalio.api.workflowservice.v1 import DeleteWorkflowExecutionRequest
    from temporalio.client import Client, WorkflowExecutionStatus
    from temporalio.contrib.pydantic import pydantic_data_converter

    t = settings.temporal
    client = await Client.connect(
        t.target, namespace=t.namespace,
        data_converter=pydantic_data_converter)
    terminated = deleted = 0
    async for wf in client.list_workflows():
        wid, rid = wf.id, wf.run_id
        if wf.status == WorkflowExecutionStatus.RUNNING:
            try:
                await client.get_workflow_handle(
                    wid, run_id=rid).terminate("wipe_db")
                terminated += 1
            except Exception as exc:
                logger.warning("temporal terminate {w}: {e}", w=wid, e=exc)
        try:
            await client.workflow_service.delete_workflow_execution(
                DeleteWorkflowExecutionRequest(
                    namespace=t.namespace,
                    workflow_execution=PbWorkflowExecution(
                        workflow_id=wid, run_id=rid)))
            deleted += 1
        except Exception as exc:
            logger.warning("temporal delete {w}: {e}", w=wid, e=exc)
    return terminated, deleted


def wipe_temporal() -> None:
    """Best-effort: terminate running workflows and delete every execution
    (open + closed) in the namespace, so Temporal matches the wiped stores.
    Fail-open if the Temporal server is unreachable."""
    import asyncio

    try:
        terminated, deleted = asyncio.run(_wipe_temporal_async())
        logger.info("temporal wipe  terminated={t}  deleted={d}",
                    t=terminated, d=deleted)
    except Exception as exc:
        logger.warning("temporal wipe  skipped (server unreachable?): {e}",
                       e=exc)


def confirm() -> bool:
    graph_line = (
        f"Nebula space `{settings.nebula.space}` @ "
        f"{settings.nebula.host}:{settings.nebula.port}"
        if settings.graph.backend == "nebula"
        else f"Neo4j graph @ {settings.neo4j.uri} db={settings.neo4j.database}"
    )
    print(
        "\n  Will WIPE:\n"
        f"    * Temporal  workflows @ {settings.temporal.target} ns={settings.temporal.namespace}\n"
        f"    * RabbitMQ  queues {[*settings.rabbitmq.queues, settings.rabbitmq.dlq]} @ {settings.rabbitmq.url}\n"
        f"    * Postgres  tables {list(_PG_TABLES)} @ {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}\n"
        f"    * Milvus    ALL collections @ {settings.milvus.uri}\n"
        f"    * {graph_line}\n"
        f"    * MinIO     buckets [{settings.minio.bucket!r}, {settings.temporal.staging_bucket!r}] @ {settings.minio.endpoint}\n"
        f"    * MediaWiki pages (except Main Page) @ {settings.wikibase.base_url}\n"
        f"    * Files: {settings.api.upload_dir}, {settings.ingestion.cache_dir}\n"
    )
    ans = input("  Type 'yes' to continue: ").strip().lower()
    return ans == "yes"


def main() -> int:
    args = _parse_args()
    configure_logging(level=settings.api.log_level, json_output=False)

    if not args.yes and not confirm():
        print("aborted.")
        return 1

    # Terminate in-flight workflows + stop new queue work first, so nothing
    # writes back into a store we're about to (or just did) wipe.
    if not args.keep_temporal:
        wipe_temporal()
    if not args.keep_rabbit:
        wipe_rabbitmq()
    wipe_postgres()
    wipe_milvus()
    wipe_graph()
    if not args.keep_minio:
        wipe_minio()
    if not args.keep_wiki:
        wipe_wiki()
    if not args.keep_files:
        wipe_files()

    if not args.no_setup:
        # Recreate Postgres schema + verify Milvus/MinIO connectivity.
        from scripts.setup_db import main as setup_main
        logger.info("running setup_db to recreate schemas")
        setup_main()

    logger.info("wipe complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())

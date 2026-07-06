"""DESTRUCTIVE — wipe all data stores.

Drops:
  * Postgres   — TRUNCATE `documents` (job-status table).
  * Milvus     — drop the configured collection.
  * Neo4j      — MATCH (n) DETACH DELETE n (all nodes + relations).
  * MediaWiki  — delete wiki-editor article pages (except `Main Page`).
  * Filesystem — clear API upload dir + ingestion cache (if local).

Then runs `setup_db.py` so the next ingest finds clean schemas.

The API / worker do not need to be stopped — they'll see empty
stores on the next request.  But any in-flight ingest will fail
mid-flight (its row in `documents` disappears).

The MediaWiki wipe is best-effort: if the wiki stack is down or the
admin login fails it logs a warning and continues (so wipes still work
on stacks that don't run the wiki). It does NOT touch Wikibase
Items/Properties (the `setup_wikibase` bootstrap owns those; per-entity
QIDs are removed with the Neo4j node wipe).

Usage::

    uv run python -m scripts.wipe_db                 # interactive confirm
    uv run python -m scripts.wipe_db --yes           # no prompt (CI / scripts)
    uv run python -m scripts.wipe_db --keep-files    # skip uploads/cache
    uv run python -m scripts.wipe_db --keep-wiki     # skip MediaWiki pages
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
    p.add_argument("--keep-temporal", action="store_true",
                   help="don't terminate/delete Temporal workflow executions.")
    p.add_argument("--no-setup", action="store_true",
                   help="skip running setup_db after the wipe.")
    return p.parse_args()


# ── Postgres ─────────────────────────────────────────────────────────


def wipe_postgres() -> None:
    import psycopg

    pg = settings.postgres
    logger.info("postgres wipe  host={h}:{p}  db={d}",
                h=pg.host, p=pg.port, d=pg.db)
    with psycopg.connect(
        pg.dsn, connect_timeout=pg.connect_timeout_s, autocommit=True,
    ) as conn, conn.cursor() as cur:
        try:
            cur.execute("TRUNCATE TABLE documents")
            logger.info("postgres  TRUNCATE documents  ok")
        except psycopg.errors.UndefinedTable:
            logger.info("postgres  documents table absent — nothing to wipe")


# ── Milvus ───────────────────────────────────────────────────────────


def wipe_milvus() -> None:
    from pymilvus import MilvusClient

    mv = settings.milvus
    client = MilvusClient(uri=mv.uri, timeout=mv.timeout_s)
    try:
        existing = client.list_collections()
        if mv.collection in existing:
            client.drop_collection(mv.collection)
            logger.info("milvus  dropped collection={c}", c=mv.collection)
        else:
            logger.info("milvus  collection={c} absent — nothing to wipe",
                        c=mv.collection)
    finally:
        client.close()


# ── Neo4j ────────────────────────────────────────────────────────────


def wipe_neo4j() -> None:
    from neo4j import GraphDatabase

    nj = settings.neo4j
    logger.info("neo4j wipe  uri={u}  db={d}", u=nj.uri, d=nj.database)
    auth = (nj.user, nj.password.get_secret_value())
    with GraphDatabase.driver(nj.uri, auth=auth) as driver:
        with driver.session(database=nj.database) as s:
            # CALL APOC if available is faster on big graphs;
            # default Cypher works on stock Neo4j too.
            res = s.run("MATCH (n) DETACH DELETE n RETURN count(*) AS n")
            row = res.single()
            n = row["n"] if row else 0
            logger.info("neo4j  detach-deleted nodes (sweep total: {n})", n=n)
            # Drop any persisted indexes / constraints LightRAG /
            # PropertyGraphIndex created so the next build is clean.
            try:
                idx_res = s.run("SHOW INDEXES YIELD name")
                names = [r["name"] for r in idx_res]
                for name in names:
                    # Skip system / lookup indexes Neo4j ships with.
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
    owns those; per-entity QIDs were already removed with the Neo4j wipe).
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
    print(
        "\n  Will WIPE:\n"
        f"    * Temporal  workflows @ {settings.temporal.target} ns={settings.temporal.namespace}\n"
        f"    * Postgres `documents` table @ {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.db}\n"
        f"    * Milvus collection `{settings.milvus.collection}` @ {settings.milvus.uri}\n"
        f"    * Neo4j graph @ {settings.neo4j.uri} db={settings.neo4j.database}\n"
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

    # Terminate in-flight workflows first so nothing writes back post-wipe.
    if not args.keep_temporal:
        wipe_temporal()
    wipe_postgres()
    wipe_milvus()
    wipe_neo4j()
    if not args.keep_wiki:
        wipe_wiki()
    if not args.keep_files:
        wipe_files()

    if not args.no_setup:
        # Recreate Postgres schema + verify Milvus connectivity.
        from scripts.setup_db import main as setup_main
        logger.info("running setup_db to recreate schemas")
        setup_main()

    logger.info("wipe complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())

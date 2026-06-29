"""Temporal worker entry point.

Run with::

    uv run python -m src.workflow.worker

Each worker pool runs in its OWN OS process (forked by the launcher),
NOT as coroutines on one shared event loop.  This isolates the pools:
a blocking/CPU-heavy stretch in one pool (e.g. a Neo4j upsert or a slow
Ollama generation) can no longer freeze the others — each process has
its own event loop AND its own GIL.

Pools (one process each):

* **main** — ``task_queue`` IO / embedding activities + DocumentIngestWorkflow.
* **llm**  — ``llm_task_queue``: ONLY ``extract_kg`` (GPU-bound LLM).
* **merge** — ``merge_task_queue``: GraphBuildWorkflow + merge activities.
* **search** — ``search_task_queue``: plan-execute + GraphRAG search WFs.
* **large** — ``large_task_queue``: large-tier ``synthesize_answer`` only.
* **graph_build** — ``graph_build_task_queue``: offline community build.
* **wiki** — ``wiki.task_queue``: WikiSweepWorkflow.

Process layout:

* Default (``WORKER_GROUPS`` unset): the launcher forks one child per
  pool — full isolation on a single host.
* ``WORKER_GROUPS=llm,merge`` runs only those pools (each in its own
  process) — point the GPU box at the LLM lanes, other hosts at search,
  etc.  A single selected group runs inline (no fork).

Each process binds its own Prometheus port (``METRICS_BIND_ADDRESS`` base
port + the pool's canonical index) so 7 exporters don't collide.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import signal
from collections.abc import Sequence

from loguru import logger
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from src.config import settings
from src.workflow.activities import (
    EXTRACT_ACTIVITIES,
    MAIN_ACTIVITIES,
    MERGE_ACTIVITIES,
    SEARCH_ACTIVITIES,
    synthesize_answer,
)
from src.workflow.analytics.activities import ANALYTICS_ACTIVITIES, ANALYTICS_LARGE_ACTIVITIES
from src.workflow.analytics.materialize_workflow import AnalyticsMaterializeWorkflow
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow
from src.workflow.document_ingest import DocumentIngestWorkflow
from src.workflow.graph_build import GraphBuildWorkflow
from src.workflow.ingest_scheduler import IngestSchedulerWorkflow
from src.workflow.search.activities import (
    GRAPH_BUILD_ACTIVITIES,
    SEARCH_V2_ACTIVITIES,
)
from src.workflow.search.community_wf import CommunityBuildWorkflow
from src.workflow.search.global_wf import GlobalSearchWorkflow
from src.workflow.search.orchestrator import SearchOrchestratorWorkflow
from src.workflow.search.router_wf import AutoSearchWorkflow, DriftSearchWorkflow
from src.workflow.search.subquery_wf import SubQueryRetrievalWorkflow
from src.workflow.wiki.wiki_sweep import (
    WikiSweepWorkflow,
    select_dirty_entities,
    write_entity_article,
)

# Canonical pool groups, in fork order.  The index also fixes each
# process's Prometheus port offset.
WORKER_GROUPS: list[str] = [
    "main",
    "llm",
    "merge",
    "search",
    "large",
    "graph_build",
    "wiki",
    "scheduler",
]

# Module-level workflow/activity lists — referenced in _build_worker and
# inspected by tests without a live Temporal connection.
SEARCH_WORKFLOWS = [
    SearchOrchestratorWorkflow,
    SubQueryRetrievalWorkflow,
    GlobalSearchWorkflow,
    DriftSearchWorkflow,
    AutoSearchWorkflow,
    AnalyticalQueryWorkflow,
]


def select_groups(spec: str | None) -> list[str]:
    """Resolve the ``WORKER_GROUPS`` env spec to a canonical-ordered list.

    Empty / unset → all groups.  Unknown names raise (fail fast at boot
    rather than silently skipping a pool)."""
    if not spec or not spec.strip():
        return list(WORKER_GROUPS)
    requested = {p.strip() for p in spec.split(",") if p.strip()}
    unknown = requested - set(WORKER_GROUPS)
    if unknown:
        raise ValueError(
            f"unknown WORKER_GROUPS: {sorted(unknown)} (valid: {WORKER_GROUPS})",
        )
    return [g for g in WORKER_GROUPS if g in requested]


def metrics_port_for(base_port: int, group: str) -> int:
    """Per-process Prometheus port: base + the pool's canonical index, so
    the forked pools never fight over one port."""
    if group not in WORKER_GROUPS:
        raise ValueError(f"unknown group: {group!r}")
    return base_port + WORKER_GROUPS.index(group)


def _metrics_host_base() -> tuple[str, int]:
    addr = settings.metrics.bind_address
    host, _, port = addr.rpartition(":")
    return (host or "0.0.0.0"), int(port)


def _build_runtime(group: str) -> Runtime | None:
    """Process-wide Temporal Runtime with a Prometheus exporter on this
    pool's own port.  ``None`` when metrics are disabled.

    Fail-soft: a bind failure (port already held by another pool / a stale
    worker / the API) logs a warning and returns ``None`` so the pool runs
    WITHOUT metrics — observability must never crash the worker itself.
    """
    if not settings.metrics.enabled:
        return None
    host, base = _metrics_host_base()
    bind = f"{host}:{metrics_port_for(base, group)}"
    try:
        runtime = Runtime(
            telemetry=TelemetryConfig(
                metrics=PrometheusConfig(
                    bind_address=bind,
                    durations_as_seconds=True,
                ),
            ),
        )
    except Exception as exc:
        logger.warning(
            "worker[{g}]  prometheus bind {a} failed ({e}) — running WITHOUT metrics",
            g=group,
            a=bind,
            e=exc,
        )
        return None
    logger.info("worker[{g}]  prometheus exporter  addr={a}", g=group, a=bind)
    return runtime


def _build_worker(client: Client, group: str) -> Worker:
    """Construct the single Worker pool for ``group``."""
    t = settings.temporal
    if group == "main":
        return Worker(
            client,
            task_queue=t.task_queue,
            workflows=[DocumentIngestWorkflow],
            activities=MAIN_ACTIVITIES,
            max_concurrent_activities=t.activity_concurrency,
        )
    if group == "scheduler":
        # Singleton admission scheduler, isolated from the `main` pool so its
        # signal + continue_as_new churn doesn't starve DocumentIngestWorkflow
        # task processing. Workflow-only (no activities); it starts the
        # DocumentIngestWorkflow CHILDREN on `task_queue` (main).
        return Worker(
            client,
            task_queue=t.scheduler_task_queue,
            workflows=[IngestSchedulerWorkflow],
        )
    if group == "llm":
        # extract_kg only — concurrency throttled further by the LLMPool.
        return Worker(
            client,
            task_queue=t.llm_task_queue,
            activities=EXTRACT_ACTIVITIES,
            max_concurrent_activities=t.llm_activity_concurrency,
        )
    if group == "merge":
        return Worker(
            client,
            task_queue=t.merge_task_queue,
            workflows=[GraphBuildWorkflow],
            activities=MERGE_ACTIVITIES,
            max_concurrent_activities=t.merge_activity_concurrency,
        )
    if group == "search":
        return Worker(
            client,
            task_queue=t.search_task_queue,
            workflows=SEARCH_WORKFLOWS,
            activities=SEARCH_ACTIVITIES + SEARCH_V2_ACTIVITIES + ANALYTICS_ACTIVITIES,
            max_concurrent_activities=t.search_activity_concurrency,
        )
    if group == "large":
        # large-tier synthesis only; the orchestrator pins synthesize_answer
        # here via execute_activity(task_queue=large_task_queue).
        return Worker(
            client,
            task_queue=t.large_task_queue,
            activities=[synthesize_answer, *ANALYTICS_LARGE_ACTIVITIES],
            max_concurrent_activities=t.large_activity_concurrency,
        )
    if group == "graph_build":
        return Worker(
            client,
            task_queue=t.graph_build_task_queue,
            workflows=[CommunityBuildWorkflow, AnalyticsMaterializeWorkflow],
            activities=GRAPH_BUILD_ACTIVITIES,
            max_concurrent_activities=t.graph_build_activity_concurrency,
        )
    if group == "wiki":
        return Worker(
            client,
            task_queue=settings.wiki.task_queue,
            workflows=[WikiSweepWorkflow],
            activities=[select_dirty_entities, write_entity_article],
            max_concurrent_activities=settings.wiki.activity_concurrency,
        )
    raise ValueError(f"unknown group: {group!r}")


async def _run_one(group: str) -> None:
    """Connect a client and run THIS process's single pool to completion."""
    # Surface LiteLLM model-config mistakes at boot, not at the first
    # activity that hits the proxy and gets a 500.
    from src.observability.litellm_models import validate_litellm_models

    validate_litellm_models(source=f"worker:{group}")

    if group == "main":
        from src.config import settings as _settings

        problems = _settings.preflight(_settings)
        if problems:
            msg = "preflight: " + "; ".join(problems)
            if _settings.api.env == "production":
                raise RuntimeError(msg)
            logger.warning(msg)

    runtime = _build_runtime(group)
    client = await Client.connect(
        settings.temporal.target,
        namespace=settings.temporal.namespace,
        data_converter=pydantic_data_converter,
        runtime=runtime,
    )
    worker = _build_worker(client, group)
    logger.info(
        "worker[{g}]  started  target={t}  pid={p}",
        g=group,
        t=settings.temporal.target,
        p=os.getpid(),
    )
    try:
        await worker.run()
    finally:
        from src.storage.pg_pool import close_pg_pool

        await close_pg_pool()


def _child_main(group: str) -> None:
    """multiprocessing child entrypoint (must be top-level for spawn)."""
    try:
        asyncio.run(_run_one(group))
    except KeyboardInterrupt:
        pass
    except Exception:
        # Surface the real cause in the log stream — otherwise the
        # launcher's "stopping siblings" buries it.
        logger.opt(exception=True).error(
            "worker[{g}]  crashed during startup/run",
            g=group,
        )
        raise


def _run_launcher(groups: Sequence[str]) -> None:
    """Fork one child process per pool and supervise them.

    On SIGINT/SIGTERM (or any child exiting) terminate the rest and exit,
    so the process manager (systemd/k8s/compose) restarts the whole set.
    """
    mp.set_start_method("spawn", force=True)
    procs: list[mp.Process] = []
    for g in groups:
        p = mp.Process(target=_child_main, args=(g,), name=f"worker-{g}")
        p.start()
        procs.append(p)
    logger.info(
        "worker launcher  pools={n}  groups={g}  pid={p}",
        n=len(procs),
        g=list(groups),
        p=os.getpid(),
    )

    stopping = {"flag": False}

    def _shutdown(signum, _frame):
        if stopping["flag"]:
            return
        stopping["flag"] = True
        logger.info("worker launcher  signal={s}  stopping {n} pools", s=signum, n=len(procs))
        for p in procs:
            if p.is_alive():
                p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Wait until any child exits (crash or shutdown), then bring the rest
    # down so the supervisor restarts a clean set.
    while not stopping["flag"]:
        for p in procs:
            p.join(timeout=1.0)
            if not p.is_alive():
                logger.warning(
                    "worker[{n}]  exited code={c} — stopping siblings",
                    n=p.name,
                    c=p.exitcode,
                )
                _shutdown(signal.SIGTERM, None)
                break
    for p in procs:
        p.join()


def main() -> None:
    groups = select_groups(os.environ.get("WORKER_GROUPS"))
    if len(groups) == 1:
        # Single pool selected → run inline, no fork (e.g. a per-pool
        # deploy unit, or a one-host single-lane setup).
        asyncio.run(_run_one(groups[0]))
        return
    _run_launcher(groups)


if __name__ == "__main__":
    main()

"""Phase 4(a): the IngestSchedulerWorkflow singleton runs on its OWN task
queue / worker pool, isolated from DocumentIngestWorkflow.

Why: at a high admission K the singleton churns (a submit signal per doc +
frequent continue_as_new). Sharing the `main` pool with DocumentIngestWorkflow
lets that churn contend with / starve per-document task processing. A
dedicated `scheduler` pool removes the contention.

Children must still run on the MAIN queue — the scheduler pool doesn't
register DocumentIngestWorkflow, so _start_child has to pin the child queue
explicitly (else it inherits the scheduler queue and never runs).
"""

from __future__ import annotations

import inspect


def test_scheduler_has_its_own_task_queue():
    from src.config import TemporalSettings
    t = TemporalSettings()
    assert t.scheduler_task_queue == "kb-ingest-scheduler"
    assert t.scheduler_task_queue != t.task_queue


def test_scheduler_is_a_worker_group():
    from src.workflow.worker import WORKER_GROUPS
    assert "scheduler" in WORKER_GROUPS


def test_scheduler_isolated_from_main_pool():
    from src.workflow import worker
    src = inspect.getsource(worker._build_worker)
    # main pool registers ONLY DocumentIngestWorkflow now (scheduler moved out)
    assert "workflows=[DocumentIngestWorkflow]," in src
    # dedicated scheduler pool registers the singleton
    assert "workflows=[IngestSchedulerWorkflow]," in src


def test_scheduler_starts_children_on_main_queue():
    from src.workflow.ingest_scheduler import IngestSchedulerWorkflow
    src = inspect.getsource(IngestSchedulerWorkflow._start_child)
    assert "task_queue=settings.temporal.task_queue" in src


def test_scheduler_started_on_scheduler_queue():
    from src.api.routes import ingest
    src = inspect.getsource(ingest)
    assert "task_queue=settings.temporal.scheduler_task_queue" in src

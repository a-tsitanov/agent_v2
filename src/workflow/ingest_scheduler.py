"""``IngestSchedulerWorkflow`` — long-lived document admission control
(Track 5, variant A).

Always on.  /ingest signal-with-starts this SINGLETON workflow (fixed id
``ingest-scheduler``); it owns the launch of every ``DocumentIngestWorkflow``
child.  The scheduler admits at most
``max_inflight`` documents at once and runs each to completion before
admitting the next (FIFO) — so a document's tail (merge) is never starved
behind dozens of newer documents' extract bursts (see
``worker_hang_congestion_collapse``).

All the scheduling LOGIC is in the pure, unit-tested ``AdmissionState``;
this shell just wires it to Temporal signals + child workflows +
``continue_as_new`` for history bounding.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy

with workflow.unsafe.imports_passed_through():
    from src.config import settings
    from src.workflow.admission import AdmissionState
    from src.workflow.contracts import IngestParams, SchedulerParams
    from src.workflow.document_ingest import DocumentIngestWorkflow

# Recycle (continue_as_new) once history crosses this AND the scheduler is
# quiescent (no running children, nothing pending) — keeps the long-lived
# workflow's event history bounded.
_HISTORY_RECYCLE_THRESHOLD = 4000


@workflow.defn
class IngestSchedulerWorkflow:
    def __init__(self) -> None:
        self._state = AdmissionState(max_inflight=1)
        self._params: dict[str, IngestParams] = {}
        self._running: dict[str, asyncio.Task] = {}

    @workflow.signal
    def submit(self, params: IngestParams) -> None:
        """Enqueue a document for admission (dedup by doc_id)."""
        self._params[params.doc_id] = params
        self._state.submit(params.doc_id)

    @workflow.signal
    def set_max_inflight(self, n: int) -> None:
        """Live-update K (admission ceiling). The run loop's wait_condition
        re-evaluates _has_free_slot() and admits more docs immediately when
        raised; lowering it just stops admitting until inflight drains.
        Persists across continue_as_new (run() carries _state.max_inflight)."""
        self._state.max_inflight = max(1, n)

    @workflow.run
    async def run(self, cfg: SchedulerParams) -> None:
        self._state.max_inflight = max(1, cfg.max_inflight)
        # Re-absorb documents carried over from the previous run's recycle.
        for p in cfg.pending:
            self._params[p.doc_id] = p
            self._state.submit(p.doc_id)

        while True:
            await workflow.wait_condition(
                lambda: self._has_free_slot()
                or self._should_recycle(
                    workflow.info().get_current_history_length()
                )
            )

            if self._should_recycle(
                workflow.info().get_current_history_length()
            ):
                # Drain before recycling: stop admitting and let the
                # in-flight children finish so continue_as_new doesn't
                # orphan/terminate them (default ParentClosePolicy).  The
                # wait is bounded by the remaining runtime of at most
                # ``max_inflight`` documents — NOT the unbounded "no new
                # work ever arrives" quiescence the old guard required.
                # While we drain, no children start, so history accrues
                # only the few child-completion events.
                await workflow.wait_condition(lambda: not self._running)
                workflow.continue_as_new(
                    SchedulerParams(
                        max_inflight=self._state.max_inflight,
                        pending=self._carry_forward(),
                    )
                )

            for doc_id in self._state.admit_ready():
                self._start_child(doc_id)

    # ── helpers ──────────────────────────────────────────────────────

    def _has_free_slot(self) -> bool:
        free = self._state.max_inflight - len(self._state.inflight)
        return free > 0 and bool(self._state.pending)

    def _should_recycle(self, history_len: int) -> bool:
        """Recycle (continue_as_new) once event history crosses the
        threshold — regardless of in-flight/pending work.

        The run loop DRAINS in-flight children first, then carries the
        still-queued docs forward via :meth:`_carry_forward`, so nothing
        is lost and the K ceiling is never exceeded.  Recycling on the
        threshold alone (not on full quiescence) is what keeps the
        always-on singleton's history bounded under sustained load — an
        unbounded history makes a cold replay exceed the workflow-task
        timeout and wedges the workflow (see module docstring)."""
        return history_len >= _HISTORY_RECYCLE_THRESHOLD

    def _carry_forward(self) -> list[IngestParams]:
        """Still-queued documents handed to the next run on recycle.
        In-flight docs are NOT included — they finish during the drain
        before continue_as_new fires."""
        return [
            self._params[d] for d in self._state.pending
            if d in self._params
        ]

    def _start_child(self, doc_id: str) -> None:
        params = self._params[doc_id]

        async def _go() -> None:
            try:
                await workflow.execute_child_workflow(
                    DocumentIngestWorkflow.run,
                    params,
                    id=f"ingest-{doc_id}",
                    # Pin children to the MAIN queue: the scheduler now runs on
                    # its own queue, whose pool does NOT register
                    # DocumentIngestWorkflow. Without this the child would
                    # inherit the scheduler queue and never get picked up.
                    task_queue=settings.temporal.task_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                )
            except Exception as exc:
                # A failed document must still free its slot so the queue
                # keeps moving — the child already recorded its own failure.
                workflow.logger.warning(
                    "ingest child failed doc=%s: %s", doc_id, exc,
                )
            finally:
                self._state.complete(doc_id)
                self._running.pop(doc_id, None)
                self._params.pop(doc_id, None)

        self._running[doc_id] = asyncio.ensure_future(_go())

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
                lambda: self._has_free_slot() or self._ready_to_recycle()
            )
            for doc_id in self._state.admit_ready():
                self._start_child(doc_id)

            if self._ready_to_recycle():
                carry = [
                    self._params[d] for d in self._state.pending
                    if d in self._params
                ]
                workflow.continue_as_new(
                    SchedulerParams(
                        max_inflight=self._state.max_inflight, pending=carry,
                    )
                )

    # ── helpers ──────────────────────────────────────────────────────

    def _has_free_slot(self) -> bool:
        free = self._state.max_inflight - len(self._state.inflight)
        return free > 0 and bool(self._state.pending)

    def _ready_to_recycle(self) -> bool:
        # Only recycle when fully quiescent so no in-flight child is lost.
        return (
            not self._running
            and not self._state.pending
            and workflow.info().get_current_history_length()
            >= _HISTORY_RECYCLE_THRESHOLD
        )

    def _start_child(self, doc_id: str) -> None:
        params = self._params[doc_id]

        async def _go() -> None:
            try:
                await workflow.execute_child_workflow(
                    DocumentIngestWorkflow.run,
                    params,
                    id=f"ingest-{doc_id}",
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                )
            except Exception as exc:  # noqa: BLE001
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

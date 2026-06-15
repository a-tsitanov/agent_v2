"""Pure admission-control state machine (Track 5).

Bounds how many documents ingest CONCURRENTLY so a document, once
started, runs to completion as a priority unit instead of its tail
(merge) queuing behind dozens of newer documents' extracts.  All the
scheduling logic lives here (deterministic, unit-tested); the Temporal
``IngestSchedulerWorkflow`` is just a thin shell around it.

Keyed by ``doc_id`` (a string) — the workflow maps ids back to the full
``IngestParams`` it carries.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AdmissionState:
    """FIFO admission with a hard ``max_inflight`` ceiling.

    ``submit`` enqueues (dedup against pending + in-flight); ``admit_ready``
    moves as many front-of-queue documents into flight as there are free
    slots and returns them to start; ``complete`` frees one slot.
    """

    max_inflight: int
    pending: list[str] = field(default_factory=list)
    inflight: set[str] = field(default_factory=set)

    def submit(self, doc_id: str) -> None:
        if doc_id in self.inflight or doc_id in self.pending:
            return
        self.pending.append(doc_id)

    def admit_ready(self) -> list[str]:
        """Pop up to ``free_slots`` documents off the front of the queue
        into flight and return them (the ones to start now)."""
        free = max(0, self.max_inflight - len(self.inflight))
        ready = self.pending[:free]
        self.pending = self.pending[free:]
        self.inflight.update(ready)
        return ready

    def complete(self, doc_id: str) -> None:
        self.inflight.discard(doc_id)


__all__ = ["AdmissionState"]

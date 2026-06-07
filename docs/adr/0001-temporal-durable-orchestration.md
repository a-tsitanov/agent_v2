# ADR-0001: Temporal for durable orchestration

- Status: Accepted
- Date: 2026-06-07

## Context

Document ingestion is a long, multi-stage pipeline (fetch → parse/chunk →
inject canonicals → vector index → extract KG → merge/resolve → build property
graph → push Wikibase → finalize), and search is a multi-step plan-execute
flow. These stages are LLM- and IO-bound, fail transiently, and must survive
worker restarts without losing or double-processing work. The project
originally drove ingest through taskiq on RabbitMQ.

## Decision

Orchestrate both ingest and search with **Temporal**. `/ingest` starts a
`DocumentIngestWorkflow` directly (no broker); each stage is a Temporal
activity with explicit retry profiles (`_FAST_FOREVER` for IO/embedding,
`_HEAVY_FOREVER` for LLM-bound), `schedule_to_close_timeout` as the wall-clock
budget, and structured heartbeats. The graph half is best-effort
(`graph_status='vector_only'` on exhaustion); the vector half failing triggers
`mark_failed` and fails the workflow. taskiq and RabbitMQ were removed.

## Consequences

- Durable, resumable execution: a worker crash mid-stage resumes from the last
  completed activity; retries and timeouts are declarative, not hand-rolled.
- Visibility (workflow histories) is reused for `ingest_metrics` (see ADR-0013).
- Commits us to running a Temporal cluster + worker process and to writing
  workflow code under determinism constraints (`workflow.unsafe.imports_passed_through`).
- Permanent input errors must be raised as `ApplicationError(non_retryable=True)`
  or they loop for the full retry budget.

## Alternatives considered

- **taskiq + RabbitMQ** (the prior design): a message broker gives queueing but
  not durable workflow state, built-in retries/timeouts, or history-based
  observability; orchestration logic had to live in application code. Removed.

## References

- `src/workflow/document_ingest.py`, `src/workflow/worker.py`,
  `src/workflow/client.py`, `src/api/routes/ingest.py`
- Commit `6ea90b5` ("remove taskiq + RabbitMQ now that ingest runs on Temporal")
- `docs/ARCHITECTURE.md`; CONCEPTS.md → "Durable orchestration with Temporal"

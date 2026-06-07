# ADR-0003: Task-queue isolation to avoid head-of-line blocking

- Status: Accepted
- Date: 2026-06-07

## Context

Running every activity on one Temporal task queue lets one workload starve
another. Concretely, a burst of `extract_kg` tasks filled a single FIFO queue
and a document's `merge_and_resolve` — enqueued behind all pending extracts —
starved (the vector half finished fast while the graph half waited out the
extract backlog). Search sessions, the heavyweight final synthesis, and the
offline community rebuild each have very different concurrency and tier needs.

## Decision

Host **several Worker pools in one process, each polling its own task queue**,
sized by `TEMPORAL_*_ACTIVITY_CONCURRENCY`:

- `kb-ingest` (main IO/embedding + `DocumentIngestWorkflow`),
- `kb-ingest-llm` (`extract_kg` only),
- `kb-ingest-merge` (`GraphBuildWorkflow` + merge/build activities),
- `kb-search-small` (plan/retrieve/coverage/rerank/route + search workflows),
- `kb-search-large` (`synthesize_answer` only, low cap),
- `kb-graph-build` (offline Leiden + summarize, low cap),
- `kb-wiki` (continuous wiki editor).

Merge inherits its child workflow's queue (no `task_queue` override) while
`extract_kg` stays pinned, so they poll independent queues and interleave.

## Consequences

- A flood in one lane no longer blocks a sibling lane; ingest vs search vs
  offline rebuild GPU budgets are tunable independently.
- More queues to configure and restart on rename/upgrade (documented per-queue
  in QUEUES.md). Queue caps are an **isolation** boundary, not the real GPU
  ceiling — that is the LLMPool (ADR-0004), and the caps must be ≥ the matching
  pool lane ceiling so the pool binds first.

## Alternatives considered

- **Single task queue** — simplest, but suffers head-of-line blocking between
  extract and merge and offers no per-workload concurrency control.

## References

- `src/workflow/worker.py`, `docs/QUEUES.md`, `src/config.py` (`TemporalSettings`)
- CONCEPTS.md → "Task-queue isolation"

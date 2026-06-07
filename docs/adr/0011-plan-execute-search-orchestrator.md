# ADR-0011: Plan-execute SearchOrchestratorWorkflow (Self-RAG/ReAct removed) + bounded coverage loop

- Status: Accepted
- Date: 2026-06-07

## Context

The original local search was an open-ended ReAct loop where the LLM decided
the next tool each step. That is slow (serial LLM turns), non-deterministic in
cost/latency, and hard to reason about under Temporal's determinism model. A
RAG question usually decomposes into a small fixed set of sub-questions that can
be retrieved in parallel.

## Decision

Replace the ReAct loop with a **plan-execute-synthesize**
`SearchOrchestratorWorkflow`:
1. optional history `contextualize_query`;
2. `plan_subquestions` (small planner) splits the query into atomic sub-Qs
   (`[query]` if atomic);
3. fan-out one `SubQueryRetrievalWorkflow` child **per sub-question in parallel**
   (`asyncio.gather` over child workflows); merge + dedup by chunk_id;
4. a **bounded coverage loop** — `coverage_check` asks if evidence fully covers
   the question; a named gap issues one extra sub-query round, bounded by
   `max_coverage_rounds` and **fail-open** (any error → straight to synthesis);
5. a unified graph+vector cross-encoder `rerank_sources` (fail-open, bounded);
6. **one** large-tier `synthesize_answer`, pinned to `kb-search-large`.

The only LLM "decisions" are the up-front planner and the final synthesizer —
there is no "LLM picks next tool" step. The legacy ReAct `SearchWorkflow` (and
its `agent_reasoning_step`/`tool_execution`/`distill_observation` activities)
was removed in the R7b cutover; orphaned reflective-synthesis paths are dead.

## Consequences

- Bounded, parallel, mostly-deterministic latency and cost; fewer serial LLM
  turns than ReAct.
- Multiple fail-open gates (coverage, rerank) ensure the answer is never blocked
  by a flaky auxiliary call; synthesis always runs large-tier on its own queue.
- Loses ReAct's open-ended tool exploration — accepted, since fixed plan-execute
  covers the RAG workload and is far easier to operate.

## Alternatives considered

- **ReAct / Self-RAG open-ended agent loop** (the prior design) — slow,
  unbounded, non-deterministic; removed.

## References

- `src/workflow/search/orchestrator.py`, `src/workflow/search/subquery_wf.py`,
  `src/workflow/search/_coverage.py`, `_merge.py`; `src/workflow/worker.py`
  (R7b cutover note)
- `docs/SEARCH.md`, `docs/SEARCH-FLOW.md`; CONCEPTS.md → "Plan-execute search"

# ADR-0004: Per-process LLMPool (tier + role lanes) owns LLM concurrency

- Status: Accepted
- Date: 2026-06-07

## Context

The true scarce resource is upstream LLM capacity (local GPU for the small
tier, OpenAI budget for the large tier), shared across ingest AND search in the
same process. Temporal per-queue caps isolate workloads but cannot arbitrate a
global GPU budget, and the earlier scattered `BoundedLLM` instances each held
their own ad-hoc semaphore with no shared ceiling.

## Decision

Introduce a **per-process `LLMPool`** (process singleton) that owns all LLM
gating. It holds a per-tier global semaphore (`small`, `large`) plus a per-role
**lane** ceiling. Each role resolves to one wrapped `BoundedLLM`; a call
acquires its lane first, then the tier-global (consistent order → no deadlock).
Small-tier lanes deliberately over-subscribe the tier total (sum of ceilings >
tier total) so one role can fill the GPU while none monopolizes it, and a
`judge_floor` reserves capacity so merge/judge never starves under an extraction
flood. This supersedes a single global cap and the scattered bounded LLMs.

## Consequences

- One place arbitrates GPU/upstream concurrency for the whole process; roles
  interleave dynamically and the GPU stays utilized.
- Temporal queue caps (ADR-0003) must be ≥ the matching pool lane ceiling or
  Temporal throttles before the pool can arbitrate — this is why `kb-ingest-llm`
  / `kb-ingest-merge` were raised to 18/14.
- It is a **per-process** control, not distributed: the cross-process ceiling
  still belongs at the LiteLLM proxy (out of scope here).

## Alternatives considered

- **A single global semaphore / one cap** — cannot express per-role floors or
  let one role fill spare capacity; coarse and prone to starvation.
- **Per-call-site `BoundedLLM` instances** — no shared ceiling; the real GPU
  limit was unenforced. Collapsed into one gate per role.

## References

- `src/retrieval/llm_pool.py`, `src/retrieval/llm_semaphore.py`,
  `src/config.py` (`LLMPoolSettings`); `docs/QUEUES.md` ("Queue caps vs the LLMPool")
- CONCEPTS.md → "The LLM pool: tiers and lanes"

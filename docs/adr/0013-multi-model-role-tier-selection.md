# ADR-0013: Multi-model role/tier selection + submit-time model snapshots into ingest_metrics

- Status: Accepted
- Date: 2026-06-07

## Context

Different workloads have different cost/quality needs: high-volume extraction,
judging, search, planning, and routing are cheap and parallel; only the final
user-facing synthesis warrants an expensive model. Operators should manage few
model names, not one per call site. Separately, when benchmarking ingest we must
know exactly which model produced each run's metrics, even if the configured
model changes later.

## Decision

A **declarative role→tier map**. Seven logical roles (`extraction`, `judge`,
`search`, `route`, `plan`, `retrieve`, `synthesis`) each map to one of two
physical tiers (`small`, `large`) via `_DEFAULT_ROLE_TIERS`; everything is
`small` except `synthesis` (`large`). Operators manage two model names
(`LITELLM_MODEL_SMALL` / `_LARGE`) and can escalate any single role with
`LITELLM_ROLE_TIERS='{"plan":"large"}'` (overrides merge onto the full default
map). `build_llm(role)` → `tier_for(role)` → physical model; the LLMPool
(ADR-0004) gates by tier+role.

For benchmarking, the per-role model names are **snapshotted at submit time**
onto the workflow payload (`extraction_model` / `judge_model` / `search_model`,
`version_tag`) and persisted by `finalize` into the Postgres `ingest_metrics`
table alongside per-activity durations derived from the workflow histories.

## Consequences

- Two-tier model management with per-role escalation; cheap tier handles volume,
  large tier reserved for the one synthesis per session.
- `ingest_metrics` rows are reproducible — each run records the exact models and
  `version_tag` used, so later config changes don't corrupt historical
  benchmarks.
- Metrics persistence is best-effort (Temporal/Postgres hiccups are logged, not
  fatal); finalize's own duration is recorded only on the next ingest's read.

## Alternatives considered

- **One global model** — no cost/quality split between volume work and final
  synthesis.
- **Read the live config when querying metrics** — a later model change would
  misattribute past runs; submit-time snapshots avoid this.

## References

- `src/config.py` (`LLMRole`/`LLMTier`, `_DEFAULT_ROLE_TIERS`,
  `LiteLLMSettings.tier_for`/`model_for`), `src/retrieval/llm.py`,
  `src/workflow/activities/finalize.py` (`_persist_ingest_metrics`),
  `src/workflow/contracts.py` (model snapshot fields),
  `src/storage/ingest_metrics.py`
- `docs/MODELS.md`, `docs/runbook/multimodel.md`, `docs/runbook/analytics.md`;
  CONCEPTS.md → "Model tiers & ingest metrics"

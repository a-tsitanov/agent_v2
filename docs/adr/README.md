# Architecture Decision Records

This directory records the **significant architectural decisions** behind
`kb-llamaindex` — a Temporal-orchestrated RAG system over a Neo4j knowledge
graph, Milvus chunk index, and a local Wikibase canonical anchor.

## What an ADR is

An Architecture Decision Record captures a single decision: the **context**
(the forces in play), the **decision** itself, its **consequences** (what it
commits us to, good and bad), the **alternatives** rejected, and **references**
to the code that implements it. ADRs are immutable once Accepted — a later
decision that overturns an earlier one gets its own record (and the old one is
marked Superseded), so the log reads as the project's reasoning over time.

Each ADR is grounded in the actual codebase, not aspiration. Where an ADR
describes something still partly latent or unverified against live infra, it
says so explicitly.

## How an ADR relates to CONCEPTS.md

ADRs answer **"why did we choose this?"**. `docs/CONCEPTS.md` (the educational
companion) answers **"what is this and how does it work?"** — it is the place
to learn entity resolution, GraphRAG communities, claim-check staging, the LLM
pool, etc. Read CONCEPTS.md to understand a mechanism; read the matching ADR to
understand why it was adopted over the alternatives.

> Note: `docs/CONCEPTS.md` is the planned educational companion for these
> records. Until it lands, the deep-dive docs it will draw on already exist:
> `docs/ARCHITECTURE.md`, `docs/QUEUES.md`, `docs/SEARCH.md`,
> `docs/MODELS.md`, `docs/INGEST.md`, and the `docs/runbook/` guides.

## Template

```
# ADR-NNNN: <title>
- Status: Accepted
- Date: YYYY-MM-DD
## Context
<the forces/problem — 2-5 sentences>
## Decision
<what we chose, specifically>
## Consequences
<positive + negative/tradeoffs; what this commits us to>
## Alternatives considered
<what we rejected and why>
## References
<source files + the related concept in docs/CONCEPTS.md + any runbook>
```

## Index

| #    | Title                                                                          | Status   |
| ---- | ------------------------------------------------------------------------------ | -------- |
| [0001](0001-temporal-durable-orchestration.md) | Temporal for durable orchestration            | Accepted |
| [0002](0002-claim-check-staging-minio.md)       | Claim-check staging via MinIO for heavy state | Accepted |
| [0003](0003-task-queue-isolation.md)            | Task-queue isolation to avoid head-of-line blocking | Accepted |
| [0004](0004-per-process-llm-pool.md)            | Per-process LLMPool owns LLM concurrency        | Accepted |
| [0005](0005-deterministic-identifier-canonicalization.md) | Deterministic identifier canonicalization before LLM extraction | Accepted |
| [0006](0006-milvus-hnsw-default-index.md)       | Milvus HNSW as the default chunk index          | Accepted |
| [0007](0007-entity-resolution-pipeline.md)      | Entity Resolution = candidate-gen + LLM-judge + cache + union-find | Accepted |
| [0008](0008-native-vector-knn-er.md)            | Opt-in native-vector kNN ER over the 5000-row window | Accepted |
| [0009](0009-hierarchical-leiden-communities.md) | Hierarchical Leiden communities + structured reports | Accepted |
| [0010](0010-dynamic-community-selection.md)     | Dynamic community selection (lexical/semantic/descent) | Accepted |
| [0011](0011-plan-execute-search-orchestrator.md)| Plan-execute SearchOrchestratorWorkflow         | Accepted |
| [0012](0012-wikibase-anchor-continuous-wiki.md) | Wikibase canonical anchor + continuous wiki editor | Accepted |
| [0013](0013-multi-model-role-tier-selection.md) | Multi-model role/tier selection + submit-time snapshots | Accepted |
| [0014](0014-source-download-stable-endpoint.md) | Source download via stable API endpoint         | Accepted |

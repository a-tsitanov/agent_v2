# Search subsystem

> Status: refactor in progress (Plan #2, agentic-search). The monolithic
> `src/workflow/search_workflow.py` is being decomposed into a
> `src/workflow/search/` package: a thin orchestrator + per-mode child
> workflows (sub-query retrieval, global search) + an offline community
> builder. Legacy search remains the default until cutover.

## Workflows (target)
- `SearchOrchestratorWorkflow` — route → plan → fan-out → rerank → coverage → synthesize
- `SubQueryRetrievalWorkflow` — plan-execute retrieval for one sub-question
- `GlobalSearchWorkflow` — GraphRAG community map-reduce
- `CommunityBuildWorkflow` — offline GDS Leiden + batch community summaries

## Endpoints (target)
- `POST /api/v1/search/local`, `/search/global`, `/search/drift`, `/search/auto`
- `POST /api/v1/admin/communities/rebuild`

(Populated phase by phase; see `docs/superpowers/plans/2026-05-25-agentic-search.md`.)

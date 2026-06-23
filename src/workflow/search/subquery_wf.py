"""``SubQueryRetrievalWorkflow`` — retrieval for ONE sub-question (R2).

Deterministic by design: invokes the single ``retrieve_subquestion``
activity (hybrid vector + graph) and returns its sources deduped by
chunk_id.  There is NO ``agent_reasoning_step`` and no tool-selection
LLM call here — the plan-execute flow fixes the tools up front, so a
sub-query is just "retrieve for this sub-question".

Run as a child workflow, one per sub-question, by
``SearchOrchestratorWorkflow``.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.workflow.contracts import (
        RetrieveParams,
        RetrieveResult,
        SubQueryParams,
        SubQueryResult,
    )
    from src.workflow.search._merge import dedup_by_chunk_id
    from src.workflow.search._retry import (
        FAST_RETRY, LLM_SCHEDULE_TO_CLOSE, LLM_START_TO_CLOSE,
    )


@workflow.defn
class SubQueryRetrievalWorkflow:
    """Retrieve sources for one sub-question — deterministic, no agent."""

    @workflow.run
    async def run(self, params: SubQueryParams) -> SubQueryResult:
        log = workflow.logger
        log.info("subquery_wf  sub=%s", params.subquestion[:80])

        result: RetrieveResult = await workflow.execute_activity(
            "retrieve_subquestion",
            RetrieveParams(
                subquestion=params.subquestion,
                top_k=params.top_k,
                doc_date_after_epoch=params.doc_date_after_epoch,
                doc_date_before_epoch=params.doc_date_before_epoch,
                inserted_after_epoch=params.inserted_after_epoch,
                inserted_before_epoch=params.inserted_before_epoch,
            ),
            result_type=RetrieveResult,
            start_to_close_timeout=LLM_START_TO_CLOSE,
            schedule_to_close_timeout=LLM_SCHEDULE_TO_CLOSE,
            retry_policy=FAST_RETRY,
        )

        # Dedup by chunk_id — vector + graph can return the same chunk.
        sources = dedup_by_chunk_id(result.sources)
        return SubQueryResult(
            subquestion=params.subquestion,
            sources=sources,
        )

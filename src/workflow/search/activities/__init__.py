"""Temporal activities for the plan-execute search subsystem (R2).

``SEARCH_V2_ACTIVITIES`` is the set the orchestrator + SubQuery child
need on the search task queue, ON TOP of the legacy ``synthesize_answer``
they reuse for the final answer.  The worker registers both sets on the
same queue (legacy ReAct activities + these) during the parity window.
"""

from src.workflow.search.activities.plan import plan_subquestions
from src.workflow.search.activities.rerank import rerank_sources
from src.workflow.search.activities.retrieve import retrieve_subquestion

SEARCH_V2_ACTIVITIES = [
    plan_subquestions,
    retrieve_subquestion,
    rerank_sources,
]

__all__ = [
    "SEARCH_V2_ACTIVITIES",
    "plan_subquestions",
    "rerank_sources",
    "retrieve_subquestion",
]

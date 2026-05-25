"""Temporal activities for the plan-execute search subsystem (R2).

``SEARCH_V2_ACTIVITIES`` is the set the orchestrator + SubQuery child
need on the search task queue, ON TOP of the legacy ``synthesize_answer``
they reuse for the final answer.  The worker registers both sets on the
same queue (legacy ReAct activities + these) during the parity window.
"""

from src.workflow.search.activities.community import (
    detect_communities_activity,
    summarize_community_activity,
)
from src.workflow.search.activities.plan import plan_subquestions
from src.workflow.search.activities.rerank import rerank_sources
from src.workflow.search.activities.retrieve import retrieve_subquestion

SEARCH_V2_ACTIVITIES = [
    plan_subquestions,
    retrieve_subquestion,
    rerank_sources,
]

# Offline graph-community build (Search R6) — registered ONLY on the
# dedicated ``kb-graph-build`` queue, never alongside the query-path
# search activities.
GRAPH_BUILD_ACTIVITIES = [
    detect_communities_activity,
    summarize_community_activity,
]

__all__ = [
    "GRAPH_BUILD_ACTIVITIES",
    "SEARCH_V2_ACTIVITIES",
    "detect_communities_activity",
    "plan_subquestions",
    "rerank_sources",
    "retrieve_subquestion",
    "summarize_community_activity",
]

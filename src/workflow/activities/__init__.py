"""Activity functions invoked by `DocumentIngestWorkflow`.

Activities are split into two pools by GPU/LLM pressure:

* ``LLM_ACTIVITIES`` — talk to the project LLM (LightRAG extraction
  and cross-chunk merge / ER).  Run on the dedicated
  ``settings.temporal.llm_task_queue`` queue with concurrency
  capped at ``settings.temporal.llm_activity_concurrency`` (default 1)
  so simultaneous workflows can't dogpile the local GPU.

* ``MAIN_ACTIVITIES`` — IO-bound or embedding-only.  Run on the main
  ``settings.temporal.task_queue`` queue with normal concurrency.

``ALL_ACTIVITIES`` is kept for tests + small deployments that prefer
a single worker pool.
"""

from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.activities.coverage_check import coverage_check
from src.workflow.activities.extract_kg import extract_kg
from src.workflow.activities.fetch_source import fetch_source
from src.workflow.activities.finalize import finalize, mark_failed
from src.workflow.activities.index_vector import index_vector
from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.activities.parse_and_chunk import parse_and_chunk
from src.workflow.activities.push_wikibase import push_wikibase
from src.workflow.activities.synthesize_answer import synthesize_answer

LLM_ACTIVITIES = [
    extract_kg,
    merge_and_resolve,
    # build_property_graph is registered on BOTH queues so the
    # GraphBuildWorkflow child (running on kb-ingest-llm) can claim it
    # locally without a cross-queue dispatch override.  The Neo4j-write
    # itself isn't LLM-bound, so the LLM concurrency cap doesn't
    # starve it under normal load.
    build_property_graph,
]

MAIN_ACTIVITIES = [
    fetch_source,
    parse_and_chunk,
    index_vector,
    inject_canonical,
    build_property_graph,
    push_wikibase,
    finalize,
    mark_failed,
]

# R7b cutover: the legacy ReAct SearchWorkflow was removed, so its
# exclusive activities (agent_reasoning_step, tool_execution,
# distill_observation) are gone.  Only the SHARED search activities the
# plan-execute / GraphRAG paths use remain here: coverage_check (R4
# pre-synthesis gate, reused by the orchestrator) and synthesize_answer
# (final synthesis, also pinned to the large queue).
SEARCH_ACTIVITIES = [
    coverage_check,
    synthesize_answer,
]

ALL_ACTIVITIES = MAIN_ACTIVITIES + LLM_ACTIVITIES + SEARCH_ACTIVITIES

__all__ = [
    "ALL_ACTIVITIES",
    "LLM_ACTIVITIES",
    "MAIN_ACTIVITIES",
    "SEARCH_ACTIVITIES",
    "build_property_graph",
    "coverage_check",
    "extract_kg",
    "fetch_source",
    "finalize",
    "index_vector",
    "inject_canonical",
    "mark_failed",
    "merge_and_resolve",
    "parse_and_chunk",
    "push_wikibase",
    "synthesize_answer",
]

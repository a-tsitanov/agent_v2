"""Activity functions invoked by `DocumentIngestWorkflow`.

LLM-bound activities are split across TWO dedicated queues so a burst
of extract_kg can't starve a document's merge (head-of-line blocking on
a single FIFO queue with concurrency 1):

* ``EXTRACT_ACTIVITIES`` — ``extract_kg`` only.  Runs on
  ``settings.temporal.llm_task_queue`` (``kb-ingest-llm``), capped at
  ``settings.temporal.llm_activity_concurrency`` (default 1).

* ``MERGE_ACTIVITIES`` — ``merge_and_resolve`` + ``build_property_graph``
  (the GraphBuildWorkflow stage).  Runs on its OWN
  ``settings.temporal.merge_task_queue`` (``kb-ingest-merge``), capped at
  ``settings.temporal.merge_activity_concurrency`` (default 1).  Giving
  merge its own lane lets it interleave with extract instead of queueing
  behind a flood of extracts → up to ~2 concurrent LLM tasks in flight.

* ``LLM_ACTIVITIES`` — the union of the two LLM lanes; kept as an alias
  for tests + small single-pool deployments.

* ``MAIN_ACTIVITIES`` — IO-bound or embedding-only.  Run on the main
  ``settings.temporal.task_queue`` queue with normal concurrency.

``ALL_ACTIVITIES`` is kept for tests + small deployments that prefer
a single worker pool.
"""

from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.activities.classify_document import classify_document
from src.workflow.activities.coverage_check import coverage_check
from src.workflow.activities.extract_kg import extract_kg
from src.workflow.activities.fetch_source import fetch_source
from src.workflow.activities.finalize import finalize, mark_failed, mark_skipped
from src.workflow.activities.index_vector import index_vector
from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.activities.mark_dirty import mark_entities_dirty
from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.activities.parse_and_chunk import parse_and_chunk
from src.workflow.activities.push_wikibase import push_wikibase
from src.workflow.activities.synthesize_answer import synthesize_answer

# extract_kg lives alone on kb-ingest-llm so its burst can't push a
# document's merge to the back of the queue.
EXTRACT_ACTIVITIES = [
    extract_kg,
]

# merge_and_resolve + build_property_graph are the GraphBuildWorkflow
# stage; they run together on kb-ingest-merge.  build_property_graph is
# ALSO registered in MAIN_ACTIVITIES (Neo4j-write, not LLM-bound) so a
# single-pool deployment can still claim it locally.
MERGE_ACTIVITIES = [
    merge_and_resolve,
    build_property_graph,
]

# Union alias — kept for tests + small single-pool deployments that host
# every LLM activity on one worker.
LLM_ACTIVITIES = EXTRACT_ACTIVITIES + MERGE_ACTIVITIES

MAIN_ACTIVITIES = [
    fetch_source,
    classify_document,
    parse_and_chunk,
    index_vector,
    inject_canonical,
    build_property_graph,
    push_wikibase,
    finalize,
    mark_failed,
    mark_skipped,
    mark_entities_dirty,
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
    "EXTRACT_ACTIVITIES",
    "LLM_ACTIVITIES",
    "MAIN_ACTIVITIES",
    "MERGE_ACTIVITIES",
    "SEARCH_ACTIVITIES",
    "build_property_graph",
    "classify_document",
    "coverage_check",
    "extract_kg",
    "fetch_source",
    "finalize",
    "index_vector",
    "inject_canonical",
    "mark_entities_dirty",
    "mark_failed",
    "mark_skipped",
    "merge_and_resolve",
    "parse_and_chunk",
    "push_wikibase",
    "synthesize_answer",
]

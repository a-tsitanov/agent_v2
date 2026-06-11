"""Structural tests for the dedicated kb-ingest-merge queue split.

These are pure import/config assertions — no live Temporal needed.  They
pin the activity-list partition (extract on its own lane, merge +
build_property_graph on the merge lane) and that the worker module wires
a pool against ``merge_task_queue`` hosting ``GraphBuildWorkflow``.

Background: extract_kg and the merge stage previously shared
``kb-ingest-llm`` (concurrency 1).  A burst of extract_kg tasks could
starve a document's merge (head-of-line blocking).  Merge now runs on
its own queue + worker so it interleaves with extract instead of
queueing behind it.
"""

from __future__ import annotations

from src.workflow.activities import (
    EXTRACT_ACTIVITIES,
    LLM_ACTIVITIES,
    MERGE_ACTIVITIES,
)
from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.activities.extract_kg import extract_kg
from src.workflow.activities.merge_and_resolve import merge_and_resolve


def test_extract_activities_only_extract() -> None:
    assert [extract_kg] == EXTRACT_ACTIVITIES
    assert merge_and_resolve not in EXTRACT_ACTIVITIES
    assert build_property_graph not in EXTRACT_ACTIVITIES


def test_merge_activities_are_merge_and_build() -> None:
    assert merge_and_resolve in MERGE_ACTIVITIES
    assert build_property_graph in MERGE_ACTIVITIES
    assert extract_kg not in MERGE_ACTIVITIES


def test_llm_activities_alias_is_union() -> None:
    assert LLM_ACTIVITIES == EXTRACT_ACTIVITIES + MERGE_ACTIVITIES


def test_worker_module_wires_merge_queue() -> None:
    """``import src.workflow.worker`` succeeds and the per-pool wiring
    references ``merge_task_queue`` + the merge activity split."""
    import inspect

    import src.workflow.worker as worker_mod

    src = inspect.getsource(worker_mod._build_worker)
    assert "merge_task_queue" in src
    assert "MERGE_ACTIVITIES" in src
    assert "EXTRACT_ACTIVITIES" in src
    # GraphBuildWorkflow hosts on the dedicated "merge" pool, not the llm one.
    assert "GraphBuildWorkflow" in src
    # "merge" is a first-class pool group with its own process.
    assert "merge" in worker_mod.WORKER_GROUPS

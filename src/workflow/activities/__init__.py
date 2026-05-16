"""Activity functions invoked by `DocumentIngestWorkflow`."""

from src.workflow.activities.build_property_graph import build_property_graph
from src.workflow.activities.extract_kg import extract_kg
from src.workflow.activities.fetch_source import fetch_source
from src.workflow.activities.finalize import finalize, mark_failed
from src.workflow.activities.index_vector import index_vector
from src.workflow.activities.inject_canonical import inject_canonical
from src.workflow.activities.merge_and_resolve import merge_and_resolve
from src.workflow.activities.parse_and_chunk import parse_and_chunk

ALL_ACTIVITIES = [
    fetch_source,
    parse_and_chunk,
    index_vector,
    inject_canonical,
    extract_kg,
    merge_and_resolve,
    build_property_graph,
    finalize,
    mark_failed,
]

__all__ = [
    "ALL_ACTIVITIES",
    "build_property_graph",
    "extract_kg",
    "fetch_source",
    "finalize",
    "index_vector",
    "inject_canonical",
    "mark_failed",
    "merge_and_resolve",
    "parse_and_chunk",
]

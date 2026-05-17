"""Payloads exchanged between the workflow and its activities.

Heavy state (list[BaseNode], EntityNode lists) NEVER travels in
payloads — it is pickled to MinIO and referenced by URI.  These
contracts carry only IDs, URIs, and small counters so the Temporal
DataConverter can JSON-serialise everything safely.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

GraphStatus = Literal["completed", "vector_only"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class IngestParams(_Frozen):
    doc_id: str
    path: str


class Ctx(_Frozen):
    doc_id: str
    local_path: str
    cleanup_dir: str | None
    workflow_run_id: str


class Parsed(_Frozen):
    ctx: Ctx
    nodes_uri: str
    chunk_count: int


class Indexed(_Frozen):
    node_ids: list[str]
    count: int


class Injected(_Frozen):
    count: int


class KGExtracted(_Frozen):
    parsed: Parsed
    nodes_with_kg_uri: str


class Merged(_Frozen):
    kg: KGExtracted
    merged_entities_uri: str


class GraphBuilt(_Frozen):
    entities: int
    relations: int


class FinalizeIn(_Frozen):
    ctx: Ctx
    indexed: Indexed
    graph_status: GraphStatus
    entities: int = 0
    relations: int = 0


class MarkFailedIn(_Frozen):
    ctx: Ctx | None
    params: IngestParams
    error: str


class IngestResult(_Frozen):
    doc_id: str
    chunk_count: int
    graph_status: GraphStatus
    entities: int = 0
    relations: int = 0

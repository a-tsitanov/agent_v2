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


class EntitySample(_Frozen):
    """Compact entity representation suitable for inclusion in
    activity results (kept small so Temporal UI doesn't truncate)."""

    name: str
    label: str


class RelationSample(_Frozen):
    source: str
    target: str
    label: str


class DuplicateGroup(_Frozen):
    """Pre-merge entity name that appeared `count` times across
    chunks — exactly the candidates the merger collapses."""

    name: str
    count: int
    labels: list[str] = []


class KGExtracted(_Frozen):
    parsed: Parsed
    nodes_with_kg_uri: str
    entity_count: int = 0
    relation_count: int = 0
    entity_labels_top: dict[str, int] = {}
    relation_labels_top: dict[str, int] = {}
    sample_entities: list[EntitySample] = []
    sample_relations: list[RelationSample] = []


class Merged(_Frozen):
    kg: KGExtracted
    merged_entities_uri: str
    raw_entity_count: int = 0
    merged_entity_count: int = 0
    relation_count: int = 0
    duplicate_groups: list[DuplicateGroup] = []
    phones_collapsed: int = 0
    phone_alias_map: dict[str, str] = {}
    er_merged: int = 0
    er_alias_map: dict[str, str] = {}


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

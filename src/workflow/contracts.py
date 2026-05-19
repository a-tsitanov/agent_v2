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
WikibaseStatus = Literal["ok", "skipped", "failed"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class WikibasePushed(_Frozen):
    status: WikibaseStatus
    created_items: int = 0
    updated_items: int = 0
    external_id_statements: int = 0
    relation_statements: int = 0
    new_properties_created: int = 0


class IngestParams(_Frozen):
    doc_id: str
    path: str
    # Analytics tagging — propagated end-to-end so the finalize hook
    # writes ingest_metrics rows tagged with the same labels that the
    # /ingest endpoint set via Temporal Search Attributes.  Defaults
    # match `AnalyticsSettings` so older callers without the header
    # continue to work.
    version_tag: str = "unspecified"
    model: str = ""           # global default snapshot (LITELLM_LLM_MODEL)
    # Per-role model snapshots taken at submit time so finalize can
    # write the right model into ingest_metrics per activity.  Empty
    # ⇒ that role falls back to ``model``.
    extraction_model: str = ""
    judge_model: str = ""
    search_model: str = ""
    env: str = ""


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


class GraphBuildResult(_Frozen):
    """Output of the ``GraphBuildWorkflow`` child — bundles the
    ``Merged`` staging-blob descriptor (needed by the post-graph
    ``push_wikibase`` activity in the parent) with the ``GraphBuilt``
    counters for ``finalize``.  Composition keeps both upstream fields
    intact instead of grafting fields onto an existing contract."""

    merged: Merged
    built: GraphBuilt


class FinalizeIn(_Frozen):
    ctx: Ctx
    indexed: Indexed
    graph_status: GraphStatus
    entities: int = 0
    relations: int = 0
    wikibase: WikibasePushed | None = None
    # Analytics tags propagated from IngestParams so the finalize-side
    # metrics-extractor hook labels every ingest_metrics row.
    version_tag: str = "unspecified"
    model: str = ""            # default fallback (LITELLM_LLM_MODEL)
    extraction_model: str = ""
    judge_model: str = ""
    search_model: str = ""
    env: str = ""


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
    wikibase_status: WikibaseStatus = "skipped"

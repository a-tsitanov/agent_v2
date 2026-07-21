"""Channel-group search filter — the doc_group analogue of date_filters.

`doc_group` is stamped on each chunk's node.metadata at ingest. The same
GroupFilter drives a Milvus MetadataFilters push-down (vector) and a
post-filter over graph/walk results (which don't go through Milvus).
"""
from __future__ import annotations

from dataclasses import dataclass

from llama_index.core.vector_stores import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

from src.retrieval.date_filters import DateBounds, date_metadata_filters

GROUP_FIELD = "doc_group"


@dataclass(frozen=True)
class GroupFilter:
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @property
    def any_set(self) -> bool:
        return bool(self.include or self.exclude)


def group_metadata_filters(gf: GroupFilter) -> list[MetadataFilter]:
    f: list[MetadataFilter] = []
    if gf.include:
        f.append(MetadataFilter(key=GROUP_FIELD, value=list(gf.include), operator=FilterOperator.IN))
    if gf.exclude:
        f.append(MetadataFilter(key=GROUP_FIELD, value=list(gf.exclude), operator=FilterOperator.NIN))
    return f


def node_group_ok(md: dict, gf: GroupFilter) -> bool:
    if not gf.any_set:
        return True
    g = md.get(GROUP_FIELD)
    if gf.include and g not in gf.include:
        return False
    return not (gf.exclude and g in gf.exclude)


def filter_nodes_by_group(nodes: list, gf: GroupFilter) -> list:
    """Drop NodeWithScore whose node.metadata[doc_group] is out of the
    include-set / in the exclude-set. No-op when nothing set."""
    if not gf.any_set:
        return list(nodes)
    return [n for n in nodes
            if node_group_ok(getattr(n.node, "metadata", {}) or {}, gf)]


def combined_metadata_filters(b: DateBounds, gf: GroupFilter) -> MetadataFilters | None:
    """AND the date-bound filters with the group filter into ONE push-down
    (None when neither is set)."""
    filters = date_metadata_filters(b) + group_metadata_filters(gf)
    if not filters:
        return None
    return MetadataFilters(filters=filters, condition=FilterCondition.AND)

"""Pure date-filter helpers for search (no infra).

Canonical filterable values are epoch-DAYS (int, UTC) stamped on each chunk's
``node.metadata`` at ingest. The same bounds drive a Milvus ``MetadataFilters``
push-down (vector) and a post-filter over graph/walk results — see the
2026-06-22-search-date-filters design.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from llama_index.core.vector_stores import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

DOC_DATE_FIELD = "doc_date_epoch"
INSERTED_AT_FIELD = "inserted_at_epoch"

_EPOCH = date(1970, 1, 1).toordinal()


def iso_to_epoch_days(s: str) -> int:
    """ISO ``YYYY-MM-DD`` → integer days since 1970-01-01. Raises ValueError."""
    return date.fromisoformat(s).toordinal() - _EPOCH


def today_epoch_days() -> int:
    return datetime.now(UTC).date().toordinal() - _EPOCH


@dataclass(frozen=True)
class DateBounds:
    doc_after: int | None = None
    doc_before: int | None = None
    ins_after: int | None = None
    ins_before: int | None = None

    @property
    def any_set(self) -> bool:
        return any(b is not None for b in
                   (self.doc_after, self.doc_before, self.ins_after, self.ins_before))


def bounds_from_iso(
    *, doc_after: str | None = None, doc_before: str | None = None,
    ins_after: str | None = None, ins_before: str | None = None,
) -> DateBounds:
    """Convert optional ISO date strings → epoch-day bounds. Raises ValueError
    on a malformed date."""
    def _c(s: str | None) -> int | None:
        return iso_to_epoch_days(s) if s else None
    return DateBounds(_c(doc_after), _c(doc_before), _c(ins_after), _c(ins_before))


def date_metadata_filters(b: DateBounds) -> list[MetadataFilter]:
    """The per-bound Milvus MetadataFilter list (empty when no bound set)."""
    f: list[MetadataFilter] = []
    if b.doc_after is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_after, operator=FilterOperator.GTE))
    if b.doc_before is not None:
        f.append(MetadataFilter(key=DOC_DATE_FIELD, value=b.doc_before, operator=FilterOperator.LTE))
    if b.ins_after is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_after, operator=FilterOperator.GTE))
    if b.ins_before is not None:
        f.append(MetadataFilter(key=INSERTED_AT_FIELD, value=b.ins_before, operator=FilterOperator.LTE))
    return f


def to_metadata_filters(b: DateBounds) -> MetadataFilters | None:
    """Milvus push-down filter for whichever bounds are set (None if none)."""
    f = date_metadata_filters(b)
    if not f:
        return None
    return MetadataFilters(filters=f, condition=FilterCondition.AND)


def _field_in_range(md: dict, field: str, lo: int | None, hi: int | None) -> bool:
    if lo is None and hi is None:
        return True
    v = md.get(field)
    if not isinstance(v, int):  # missing/non-int → excluded when a bound is set
        return False
    if lo is not None and v < lo:
        return False
    return not (hi is not None and v > hi)


def node_metadata_in_range(md: dict, b: DateBounds) -> bool:
    return (_field_in_range(md, DOC_DATE_FIELD, b.doc_after, b.doc_before)
            and _field_in_range(md, INSERTED_AT_FIELD, b.ins_after, b.ins_before))


def filter_nodes(nodes: list, b: DateBounds) -> list:
    """Drop NodeWithScore whose node.metadata dates fall outside bounds.
    No-op when no bound is set."""
    if not b.any_set:
        return list(nodes)
    return [n for n in nodes
            if node_metadata_in_range(getattr(n.node, "metadata", {}) or {}, b)]


def overfetch_top_k(top_k: int, b: DateBounds, factor: int = 3) -> int:
    """Over-fetch when filtering so post-filtered out-of-range hits don't
    starve the in-range result count."""
    return top_k * factor if b.any_set else top_k

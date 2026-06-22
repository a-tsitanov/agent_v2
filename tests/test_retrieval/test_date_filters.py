from __future__ import annotations

import pytest

from src.retrieval.date_filters import (
    DOC_DATE_FIELD, INSERTED_AT_FIELD, DateBounds, bounds_from_iso,
    filter_nodes, iso_to_epoch_days, node_metadata_in_range,
    overfetch_top_k, to_metadata_filters,
)


class _Node:
    def __init__(self, md): self.metadata = md
class _NWS:
    def __init__(self, md): self.node = _Node(md)


def test_iso_to_epoch_days_roundtrip():
    assert iso_to_epoch_days("1970-01-01") == 0
    assert iso_to_epoch_days("1970-01-02") == 1
    assert iso_to_epoch_days("2024-03-01") == 19783


def test_iso_to_epoch_days_rejects_bad():
    with pytest.raises(ValueError):
        iso_to_epoch_days("01-03-2024")


def test_bounds_from_iso_only_set_fields():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before=None,
                        ins_after=None, ins_before="2024-12-31")
    assert b.doc_after == iso_to_epoch_days("2024-01-01")
    assert b.doc_before is None
    assert b.ins_before == iso_to_epoch_days("2024-12-31")
    assert b.any_set is True
    assert DateBounds().any_set is False


def test_to_metadata_filters_builds_only_set_bounds():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before="2024-12-31",
                        ins_after=None, ins_before=None)
    mf = to_metadata_filters(b)
    keys = {(f.key, f.operator.value) for f in mf.filters}
    assert (DOC_DATE_FIELD, ">=") in keys
    assert (DOC_DATE_FIELD, "<=") in keys
    assert all(k[0] != INSERTED_AT_FIELD for k in keys)
    assert to_metadata_filters(DateBounds()) is None


def test_in_range_excludes_missing_and_out_of_range():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before="2024-12-31",
                        ins_after=None, ins_before=None)
    inside = {DOC_DATE_FIELD: iso_to_epoch_days("2024-06-01")}
    before = {DOC_DATE_FIELD: iso_to_epoch_days("2023-06-01")}
    missing = {"position": 0}
    assert node_metadata_in_range(inside, b) is True
    assert node_metadata_in_range(before, b) is False
    assert node_metadata_in_range(missing, b) is False  # missing field excluded
    assert node_metadata_in_range(missing, DateBounds()) is True  # no bound → keep


def test_filter_nodes_and_overfetch():
    b = bounds_from_iso(doc_after="2024-01-01", doc_before=None,
                        ins_after=None, ins_before=None)
    nodes = [_NWS({DOC_DATE_FIELD: iso_to_epoch_days("2024-06-01")}),
             _NWS({DOC_DATE_FIELD: iso_to_epoch_days("2020-01-01")}),
             _NWS({"position": 1})]
    kept = filter_nodes(nodes, b)
    assert len(kept) == 1
    assert filter_nodes(nodes, DateBounds()) == nodes  # no-op
    assert overfetch_top_k(10, b) == 30
    assert overfetch_top_k(10, DateBounds()) == 10

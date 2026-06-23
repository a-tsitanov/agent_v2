"""Date-filter request validation + ISO→epoch wiring (Phase 2).

Pure: SearchRequest's field validator and the `_local_params` conversion,
no infra.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models.search import SearchRequest
from src.retrieval.date_filters import iso_to_epoch_days


def test_search_request_accepts_iso_date_bounds():
    req = SearchRequest(
        query="q",
        created_after="2025-01-01",
        created_before="2025-12-31",
        doc_date_after="2024-06-01",
        doc_date_before="2024-06-30",
    )
    assert req.created_after == "2025-01-01"
    assert req.doc_date_before == "2024-06-30"


@pytest.mark.parametrize(
    "field",
    ["created_after", "created_before", "doc_date_after", "doc_date_before"],
)
def test_search_request_rejects_malformed_date(field):
    with pytest.raises(ValidationError):
        SearchRequest(query="q", **{field: "31-12-2025"})


def test_search_request_dates_optional():
    req = SearchRequest(query="q")
    assert req.created_after is None and req.doc_date_after is None


def test_local_params_converts_iso_dates_to_epoch_bounds():
    from src.api.routes.search_v2 import _local_params

    req = SearchRequest(
        query="q",
        doc_date_after="2024-01-01",
        doc_date_before="2024-12-31",
        created_after="2025-03-01",
    )
    params = _local_params(req)

    assert params.doc_date_after_epoch == iso_to_epoch_days("2024-01-01")
    assert params.doc_date_before_epoch == iso_to_epoch_days("2024-12-31")
    assert params.inserted_after_epoch == iso_to_epoch_days("2025-03-01")
    assert params.inserted_before_epoch is None  # created_before unset


def test_local_params_no_dates_leaves_bounds_none():
    from src.api.routes.search_v2 import _local_params

    params = _local_params(SearchRequest(query="q"))
    assert params.doc_date_after_epoch is None
    assert params.doc_date_before_epoch is None
    assert params.inserted_after_epoch is None
    assert params.inserted_before_epoch is None

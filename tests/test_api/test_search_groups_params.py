import pytest
from pydantic import ValidationError

from src.api.routes.search_v2 import _local_params
from src.models.search import SearchRequest


def test_valid_groups_accepted_and_threaded():
    req = SearchRequest(query="q", groups=["official", "data"])
    p = _local_params(req)
    assert p.groups == ["official", "data"]
    assert p.exclude_groups == []


def test_unknown_group_rejected():
    with pytest.raises(ValidationError):
        SearchRequest(query="q", groups=["sport"])


def test_include_and_exclude_mutually_exclusive():
    with pytest.raises(ValidationError):
        SearchRequest(query="q", groups=["official"], exclude_groups=["opinion"])

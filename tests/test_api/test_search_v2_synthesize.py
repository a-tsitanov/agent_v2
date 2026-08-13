"""The `synthesize` flag on /api/v1/search/*.

Asserts the flag reaches the workflow parameters; the workflow's own
short-circuit is covered in tests/test_workflow/.
"""

from __future__ import annotations

from src.api.routes.search_v2 import _local_params
from src.models.search import SearchRequest


def test_synthesize_defaults_to_true():
    assert SearchRequest(query="q").synthesize is True


def test_local_params_carry_the_flag():
    assert _local_params(SearchRequest(query="q")).synthesize is True
    assert _local_params(SearchRequest(query="q", synthesize=False)).synthesize is False

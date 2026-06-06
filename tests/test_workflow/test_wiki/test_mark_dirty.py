import pytest
from unittest.mock import MagicMock, patch
from src.workflow.contracts import MarkDirtyIn
from src.workflow.activities.mark_dirty import _dirty_names


def test_dirty_names_includes_entities_and_relation_endpoints():
    payload = MarkDirtyIn(
        entity_names=["A", "B"],
        relation_endpoints=["B", "C", "A"],
    )
    assert _dirty_names(payload) == {"A", "B", "C"}

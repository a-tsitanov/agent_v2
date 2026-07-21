from types import SimpleNamespace

from llama_index.core.vector_stores import FilterOperator
from src.retrieval.date_filters import DateBounds
from src.retrieval.group_filter import (
    GROUP_FIELD,
    GroupFilter,
    combined_metadata_filters,
    filter_nodes_by_group,
    group_metadata_filters,
    node_group_ok,
)


def _node(group):
    return SimpleNamespace(node=SimpleNamespace(metadata={"doc_group": group}))


def test_include_builds_IN_filter():
    fs = group_metadata_filters(GroupFilter(include=("official", "data")))
    assert len(fs) == 1
    assert fs[0].key == GROUP_FIELD
    assert fs[0].operator == FilterOperator.IN
    assert fs[0].value == ["official", "data"]


def test_exclude_builds_NIN_filter():
    fs = group_metadata_filters(GroupFilter(exclude=("opinion",)))
    assert fs[0].operator == FilterOperator.NIN
    assert fs[0].value == ["opinion"]


def test_any_set_and_empty():
    assert GroupFilter().any_set is False
    assert group_metadata_filters(GroupFilter()) == []
    assert GroupFilter(include=("news",)).any_set is True


def test_node_group_ok_include_and_exclude():
    assert node_group_ok({"doc_group": "official"}, GroupFilter(include=("official",)))
    assert not node_group_ok({"doc_group": "opinion"}, GroupFilter(include=("official",)))
    assert not node_group_ok({"doc_group": "opinion"}, GroupFilter(exclude=("opinion",)))
    # missing group: excluded by an include-list, kept by a no-op filter
    assert not node_group_ok({}, GroupFilter(include=("official",)))
    assert node_group_ok({}, GroupFilter())


def test_filter_nodes_by_group_drops_out_of_set():
    nodes = [_node("official"), _node("opinion"), _node("data")]
    kept = filter_nodes_by_group(nodes, GroupFilter(include=("official", "data")))
    assert [n.node.metadata["doc_group"] for n in kept] == ["official", "data"]


def test_combined_filters_ANDs_date_and_group():
    mf = combined_metadata_filters(
        DateBounds(doc_after=50), GroupFilter(include=("official",)),
    )
    keys = {f.key for f in mf.filters}
    assert keys == {"doc_date_epoch", "doc_group"}


def test_combined_filters_none_when_nothing_set():
    assert combined_metadata_filters(DateBounds(), GroupFilter()) is None

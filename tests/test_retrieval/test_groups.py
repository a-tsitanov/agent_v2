from src.retrieval.groups import GROUP_SET, GROUPS, pick_priority


def test_enum_is_the_six_groups():
    assert GROUPS == ("news", "analytics", "digest", "opinion", "official", "data")
    assert frozenset(GROUPS) == GROUP_SET
    assert "official" in GROUP_SET
    assert "sport" not in GROUP_SET


def test_pick_priority_returns_earlier_in_order():
    # order is news < analytics < digest < opinion < official < data
    assert pick_priority("official", "opinion") == "opinion"
    assert pick_priority("data", "news") == "news"
    assert pick_priority("news", "news") == "news"

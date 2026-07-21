from src.retrieval.groups import GROUPS, GROUP_SET, GROUP_PRIORITY, pick_priority


def test_enum_is_the_six_groups():
    assert GROUPS == ("news", "analytics", "digest", "opinion", "official", "data")
    assert GROUP_SET == frozenset(GROUPS)
    assert "official" in GROUP_SET
    assert "sport" not in GROUP_SET


def test_pick_priority_returns_earlier_in_order():
    # order is news < analytics < digest < opinion < official < data
    assert pick_priority("official", "opinion") == "opinion"
    assert pick_priority("data", "news") == "news"
    assert pick_priority("news", "news") == "news"

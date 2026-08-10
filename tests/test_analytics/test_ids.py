from src.analytics.ids import ID_TYPES, clamp_top_n, epoch_days_to_period


def test_id_types_are_the_identifier_entity_labels():
    assert "INN" in ID_TYPES and "PhoneNumber" in ID_TYPES
    assert "Person" not in ID_TYPES and "Organization" not in ID_TYPES
    assert len(ID_TYPES) == 12  # the 12 identifier types in schema.py


def test_clamp_top_n():
    assert clamp_top_n(None) == 20
    assert clamp_top_n(5) == 5
    assert clamp_top_n(0) == 20  # non-positive → default
    assert clamp_top_n(99999) == 200  # hard cap


def test_epoch_days_to_period():
    # 2024-03-15 = date(2024,3,15).toordinal() - date(1970,1,1).toordinal() = 19797
    assert epoch_days_to_period(19797, "month") == "2024-03"
    assert epoch_days_to_period(19797, "year") == "2024"
    assert epoch_days_to_period(19797, "quarter") == "2024-Q1"
    assert epoch_days_to_period(0, "month") == "1970-01"  # epoch origin


def test_epoch_days_to_period_day_and_week():
    # 19797 = 2024-03-15, a Friday; its week bucket is Monday 2024-03-11.
    assert epoch_days_to_period(19797, "day") == "2024-03-15"
    assert epoch_days_to_period(19797, "week") == "2024-03-11"
    # A Monday buckets to itself.
    assert epoch_days_to_period(19793, "week") == "2024-03-11"


def test_epoch_days_to_period_unknown_granularity_still_falls_back_to_month():
    """Unchanged behaviour for unknown input — callers validate upstream."""
    assert epoch_days_to_period(19797, "fortnight") == "2024-03"

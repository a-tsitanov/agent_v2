from src.storage.entity_search import build_entity_search_query


def test_exact_matches_the_whole_name():
    sql, params = build_entity_search_query("Украина", mode="exact", label=None, limit=10)
    assert "name = %s" in sql
    assert params[0] == "Украина"
    assert params[-1] == 10


def test_prefix_uses_ilike_anchored_left():
    sql, params = build_entity_search_query("Украин", mode="prefix", label=None, limit=10)
    assert "name ILIKE %s" in sql
    assert params[0] == "Украин%"


def test_substring_uses_trigram_and_orders_by_similarity():
    sql, _params = build_entity_search_query("Ромаш", mode="substring", label=None, limit=10)
    # `%%` is the psycopg-escaped `%` trigram operator.
    assert "name %% %s" in sql
    assert "similarity(name, %s)" in sql
    assert "ORDER BY" in sql


def test_label_filter_is_added_only_when_given():
    sql_no, _p_no = build_entity_search_query("x", mode="exact", label=None, limit=5)
    assert "label = %s" not in sql_no
    sql_yes, p_yes = build_entity_search_query("x", mode="exact", label="Person", limit=5)
    assert "label = %s" in sql_yes
    assert "Person" in p_yes


def test_mention_count_breaks_ties():
    """Frequent entities surface first among equal matches."""
    sql, _ = build_entity_search_query("x", mode="prefix", label=None, limit=5)
    assert "mention_count DESC" in sql


def test_unknown_mode_is_rejected():
    import pytest
    with pytest.raises(ValueError, match="unknown mode"):
        build_entity_search_query("x", mode="fuzzy", label=None, limit=5)

from scripts.setup_db import _ENTITY_DDL, _ENTITY_INDEXES_DDL


def test_table_is_created_idempotently():
    assert "CREATE TABLE IF NOT EXISTS entity" in _ENTITY_DDL


def test_vid_is_the_primary_key():
    assert "vid           TEXT PRIMARY KEY" in _ENTITY_DDL


def test_name_is_not_nullable():
    assert "name          TEXT NOT NULL" in _ENTITY_DDL


def test_no_pagerank_column():
    """Centrality is offline and would drift — deliberately absent."""
    assert "pagerank" not in _ENTITY_DDL
    assert "betweenness" not in _ENTITY_DDL


def test_trigram_index_on_name_for_substring():
    assert "entity_name_trgm_idx" in _ENTITY_INDEXES_DDL
    assert "gin_trgm_ops" in _ENTITY_INDEXES_DDL


def test_label_index_for_filtering():
    assert "entity_label_idx" in _ENTITY_INDEXES_DDL

"""Unit tests for the pure helpers in scripts/wipe_db.py.

The I/O wipe functions (Postgres/Milvus/Neo4j/MediaWiki/Temporal) need live
infra and are exercised by running the script; here we only cover the pure
page-selection helper, which decides what MediaWiki content gets deleted.
"""
from __future__ import annotations

from scripts.wipe_db import _WIKI_KEEP, _pages_to_delete


def test_pages_to_delete_excludes_keep_list():
    allpages = [{"title": "Main Page"}, {"title": "ООО Альфа"},
                {"title": "Сергей Волков"}]
    assert _pages_to_delete(allpages) == ["ООО Альфа", "Сергей Волков"]


def test_pages_to_delete_keeps_main_page_by_default():
    assert "Main Page" in _WIKI_KEEP
    assert _pages_to_delete([{"title": "Main Page"}]) == []


def test_pages_to_delete_custom_keep():
    allpages = [{"title": "A"}, {"title": "B"}, {"title": "C"}]
    assert _pages_to_delete(allpages, keep={"B"}) == ["A", "C"]


def test_pages_to_delete_empty():
    assert _pages_to_delete([]) == []

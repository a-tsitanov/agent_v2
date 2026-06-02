"""Tests for the full-text entity-name lookup helpers."""

from __future__ import annotations


def test_build_fulltext_query_or_tokens_escaped():
    from src.graph.retriever import build_fulltext_query

    assert build_fulltext_query("Иванов Иван") == "Иванов OR Иван"
    # Lucene special chars are escaped per token.
    assert build_fulltext_query("a:b (x)") == r"a\:b OR \(x\)"
    # Blank input → empty query (caller short-circuits).
    assert build_fulltext_query("   ") == ""
    assert build_fulltext_query("") == ""

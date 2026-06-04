"""Validates the example ~/.hermes/config.yaml ships a correct,
complete mcp_servers block for both kb-llamaindex servers."""

from __future__ import annotations

from pathlib import Path

import yaml

_CFG = Path("integrations/hermes/config.example.yaml")

_KBTOOLS_INCLUDE = {
    "vector_search",
    "graph_search",
    "graph_walk",
    "find_entity_by_id",
    "find_entity_by_name",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
}


def test_config_example_exists():
    assert _CFG.is_file(), f"missing {_CFG}"


def test_config_has_both_servers_with_auth_and_tools():
    cfg = yaml.safe_load(_CFG.read_text(encoding="utf-8"))
    servers = cfg["mcp_servers"]

    kbtools = servers["kbtools"]
    assert kbtools["url"].endswith("/sse")
    assert kbtools["headers"]["Authorization"].startswith("Bearer ")
    assert set(kbtools["tools"]["include"]) == _KBTOOLS_INCLUDE

    kbsearch = servers["kbsearch"]
    assert kbsearch["url"].endswith("/sse")
    assert kbsearch["headers"]["Authorization"].startswith("Bearer ")
    assert set(kbsearch["tools"]["include"]) == {
        "kb_search", "kb_global_search", "kb_drift_search", "kb_auto_search",
    }

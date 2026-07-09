"""build_graph_store dispatches on settings.graph.backend."""
from __future__ import annotations

import pytest

import src.graph.store as store_mod


def test_dispatch_neo4j_returns_neo4j_store(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(store_mod, "build_neo4j_graph_store", lambda: sentinel)
    monkeypatch.setattr(store_mod.settings.graph, "backend", "neo4j", raising=False)

    assert store_mod.build_graph_store() is sentinel


def test_dispatch_nebula_imports_nebula_builder(monkeypatch):
    # Phase 0: nebula backend selected but src.graph.nebula_store not yet
    # present -> the dispatch must attempt the import (proves the branch is
    # wired), which raises ModuleNotFoundError until Phase 1 lands it.
    monkeypatch.setattr(store_mod.settings.graph, "backend", "nebula", raising=False)

    with pytest.raises(ModuleNotFoundError):
        store_mod.build_graph_store()

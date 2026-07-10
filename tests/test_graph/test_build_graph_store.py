"""build_graph_store dispatches on settings.graph.backend."""
from __future__ import annotations

import src.graph.store as store_mod


def test_dispatch_neo4j_returns_neo4j_store(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(store_mod, "build_neo4j_graph_store", lambda: sentinel)
    monkeypatch.setattr(store_mod.settings.graph, "backend", "neo4j", raising=False)

    assert store_mod.build_graph_store() is sentinel


def test_dispatch_nebula_returns_nebula_store(monkeypatch):
    # Phase 1+ : nebula backend dispatches to build_nebula_graph_store.
    # (Phase 0 asserted ModuleNotFoundError here, before nebula_store existed.)
    import src.graph.nebula_store as nebula_mod

    sentinel = object()
    monkeypatch.setattr(nebula_mod, "build_nebula_graph_store", lambda: sentinel)
    monkeypatch.setattr(store_mod.settings.graph, "backend", "nebula", raising=False)

    assert store_mod.build_graph_store() is sentinel

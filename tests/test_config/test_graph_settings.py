"""GRAPH_BACKEND toggle: default neo4j, env override to nebula."""
from __future__ import annotations

import importlib


def test_graph_backend_defaults_to_neo4j(monkeypatch):
    monkeypatch.delenv("GRAPH_BACKEND", raising=False)
    from src.config import GraphSettings

    assert GraphSettings().backend == "neo4j"


def test_graph_backend_env_override(monkeypatch):
    monkeypatch.setenv("GRAPH_BACKEND", "nebula")
    from src.config import GraphSettings

    assert GraphSettings().backend == "nebula"

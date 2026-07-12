"""Pytest-asyncio mode is set globally in pyproject.toml.

Project-wide fixtures land here as the codebase grows.

Known: ``beartype.claw`` (transitively pulled in by fastmcp) has
a circular-import quirk when Temporal's workflow sandbox re-imports
modules after fastmcp has already loaded.  Pre-importing here
helps in some cases.  If you hit
``ImportError: cannot import name 'claw_state'`` during a combined
run, split the suites:

    pytest tests/test_mcp/
    pytest tests/test_workflow/test_search_orchestrator.py

Both pass green individually.  The flake is order-dependent, not
correctness-related.
"""

try:
    import beartype
    import beartype.claw  # noqa: F401
except ImportError:
    pass

import pytest


@pytest.fixture(autouse=True)
def _pin_graph_backend_neo4j(monkeypatch):
    """Post-cutover the PROD default is nebula (see src/config.py), but the
    DB-free unit tests validate the RETAINED Neo4j impls byte-for-byte through
    fake stores that assert neo4j Cypher/param_map. Pin the harness default to
    neo4j so that coverage stays valid; nebula-path tests override this with
    their own ``monkeypatch.setattr(..., "backend", "nebula")`` (which wins
    because it runs after this fixture) and revert cleanly."""
    from src.config import settings

    monkeypatch.setattr(settings.graph, "backend", "neo4j")

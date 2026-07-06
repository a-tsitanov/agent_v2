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

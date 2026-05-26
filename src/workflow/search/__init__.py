"""Search subsystem: orchestrator + per-mode child workflows.

Replaces the monolithic ``src/workflow/search_workflow.py`` incrementally.
Until cutover (final phase) the legacy workflow stays the default; new
workflows here are wired behind feature flags / new endpoints.
"""

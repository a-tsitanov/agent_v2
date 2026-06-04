"""Validates the Hermes knowledge-base skill: frontmatter is present
and the body references every MCP tool the agent can call (so the
decision tree stays exhaustive as tools change)."""

from __future__ import annotations

from pathlib import Path

import yaml

_SKILL = Path("integrations/hermes/knowledge-base/SKILL.md")

# The 8 atomic tools + the four orchestrated kb_*search escape hatches.
_REQUIRED_TOOL_NAMES = {
    "vector_search",
    "graph_search",
    "graph_walk",
    "find_entity_by_id",
    "find_entity_by_name",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
    "kb_search",
    "kb_global_search",
    "kb_drift_search",
    "kb_auto_search",
}


def _split_frontmatter(text: str) -> tuple[dict, str]:
    assert text.startswith("---\n"), "SKILL.md must start with YAML frontmatter"
    _, fm, body = text.split("---\n", 2)
    return yaml.safe_load(fm), body


def test_skill_file_exists():
    assert _SKILL.is_file(), f"missing {_SKILL}"


def test_skill_frontmatter_has_name_and_description():
    fm, _ = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    assert fm["name"] == "knowledge-base"
    assert isinstance(fm["description"], str) and len(fm["description"]) > 20


def test_skill_body_references_every_tool():
    _, body = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    missing = {name for name in _REQUIRED_TOOL_NAMES if name not in body}
    assert not missing, f"skill body omits tools: {missing}"


def test_skill_covers_the_four_pillars():
    _, body = _split_frontmatter(_SKILL.read_text(encoding="utf-8"))
    lowered = body.lower()
    for marker in ("tool selection", "response template", "memory", "follow-up"):
        assert marker in lowered, f"skill body missing section marker: {marker!r}"

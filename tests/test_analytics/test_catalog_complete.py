"""Wave 0 catalog completeness test.

Importing src.analytics.primitives triggers all family-module registrations via
side-effect imports in that package's __init__.py.  Once all six family modules
are imported we expect CATALOG to contain the full set of 28 Wave-0 primitives
and render_catalog_for_planner() to mention each one.
"""

import src.analytics.primitives  # noqa: F401 — triggers all registrations
from src.analytics.catalog import CATALOG, render_catalog_for_planner

_EXPECTED = {
    # Family 1 — aggregations
    "count_entities",
    "count_relationships",
    "distribution_by_type",
    "distribution_by_relation_type",
    "distribution_by_polarity",
    "top_entities_by_mentions",
    "top_entities_by_degree",
    # Family 2 — connections
    "entity_dossier",
    "neighbors_by_relation",
    "cooccurrence",
    "common_connections",
    "connection_path",
    "shared_identifier_entities",
    "identifier_lookup",
    # Family 3 (online subset) — communities
    "community_overview",
    "entity_communities",
    "personalized_pagerank",
    # Family 4 — dynamics
    "relationship_timeline",
    "whats_changed",
    "topic_trend",
    "polarity_evolution",
    "entity_activity",
    # E1 — events
    "new_events",
    "entity_new_connections",
    # P1 — quality
    "contradictions",
    "orphans",
    "incomplete_entities",
    "merge_candidates",
}


def test_wave0_catalog_is_complete() -> None:
    """All 28 Wave-0 primitives must be registered in CATALOG."""
    missing = _EXPECTED - set(CATALOG)
    assert not missing, f"Missing from CATALOG: {sorted(missing)}"


def test_planner_prompt_lists_every_primitive() -> None:
    """render_catalog_for_planner() must mention every Wave-0 primitive name."""
    rendered = render_catalog_for_planner()
    missing = [name for name in _EXPECTED if name not in rendered]
    assert not missing, f"Missing from planner prompt: {sorted(missing)}"

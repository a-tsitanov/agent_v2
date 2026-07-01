"""Wave 0 + Wave 1 + Wave 2 catalog completeness test.

Importing src.analytics.primitives triggers all family-module registrations via
side-effect imports in that package's __init__.py.  Once all family modules
are imported we expect CATALOG to contain the full set of 42 primitives
(28 Wave-0 + 8 Wave-1 + 5 Wave-2 + 1 Wave-3) and render_catalog_for_planner()
to mention each one.
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
    # Wave 1 — v1b (centrality)
    "top_central_entities",
    "link_prediction",
    # Wave 1 — P2 (signals)
    "risk_score",
    "investigate_next",
    "recommended_merges",
    "review_queue",
    "circular_ownership",
    # Wave 1 — Arc 1 (rollups)
    "numeric_rollup",
    # Wave 2 — E2 event reads (events_llm)
    "event_dossier",
    "event_timeline",
    # Wave 2 — Arc 2 alerts
    "alerts",
    # Wave 2 — P3 domain rollups
    "issue_resolution_stats",
    "communication_stats",
    # Wave 3 — E3 trending
    "trending_events",
}


def test_wave0_catalog_is_complete() -> None:
    """All 42 primitives (Wave-0 + Wave-1 + Wave-2 + Wave-3) must be registered in CATALOG."""
    missing = _EXPECTED - set(CATALOG)
    assert not missing, f"Missing from CATALOG: {sorted(missing)}"


def test_planner_prompt_lists_every_primitive() -> None:
    """render_catalog_for_planner() must mention every primitive name."""
    rendered = render_catalog_for_planner()
    missing = [name for name in _EXPECTED if name not in rendered]
    assert not missing, f"Missing from planner prompt: {sorted(missing)}"

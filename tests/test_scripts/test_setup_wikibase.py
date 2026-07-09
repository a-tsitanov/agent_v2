"""Unit tests for the Wikibase bootstrap script.

Focus on pure helpers and the Neo4j writeback logic.  The actual
Wikibase API interaction is deferred to a live smoke (operator
runs ``uv run python -m scripts.setup_wikibase`` against a real
Wikibase container in the Stage 2 gate).

setup_wikibase no longer needs the host docker CLI (container-runnable).
"""

from __future__ import annotations

from typing import get_args
from unittest.mock import MagicMock, patch

import scripts.setup_wikibase as sw


def test_no_host_docker_bot_path():
    text = open(sw.__file__, encoding="utf-8").read()
    # the host-docker bot provisioning path is gone
    assert "_ensure_bot_user" not in text
    assert "_wikibase_container_name" not in text
    assert "createAndPromote --bot" in text  # operator exec command documented


def test_identifier_properties_match_IdentifierType_literal() -> None:
    """The bootstrap must mint one ``external-id`` Property per type in
    :data:`src.ingestion.identifiers.IdentifierType`.  Drift between
    the two lists silently breaks identifier folding at ingest.
    """
    from scripts.setup_wikibase import _identifier_properties
    from src.ingestion.identifiers import IdentifierType

    expected = sorted(get_args(IdentifierType))
    got = sorted(label for label, _ in _identifier_properties())
    assert got == expected
    # All must be ``external-id`` datatype.
    for _, datatype in _identifier_properties():
        assert datatype == "external-id"


def test_persist_cache_writes_two_node_types() -> None:
    """``_persist_cache`` issues a MERGE for every base class and every
    property under the two distinct node labels."""
    from scripts.setup_wikibase import _persist_cache

    gs = MagicMock()
    base_qids = {"Person": "Q1", "Organization": "Q2"}
    property_pids = {
        "er_canonical_name": ("P1", "string"),
        "PhoneNumber":       ("P3", "external-id"),
    }

    with patch(
        "scripts.setup_wikibase.build_graph_store", return_value=gs
    ):
        _persist_cache(base_qids, property_pids)

    # 2 base + 2 properties = 4 queries.
    assert gs.structured_query.call_count == 4
    queries = [c.args[0] for c in gs.structured_query.call_args_list]
    assert any(":WikibaseBaseClass" in q for q in queries)
    assert any(":WikibaseProperty" in q for q in queries)
    # MERGE — not CREATE — so re-runs are idempotent.
    assert all(q.lstrip().startswith("MERGE") for q in queries)


def test_base_class_list_matches_spec() -> None:
    """Sanity: the 10 classes from the spec are present, no typos
    sneaking in."""
    from scripts.setup_wikibase import _BASE_CLASSES

    labels = {label for label, _ in _BASE_CLASSES}
    assert labels == {
        "Person", "Organization", "Concept", "Metric", "Topic",
        "Issue", "Resolution", "EventOrAction", "Product", "Document",
    }
    # All entries carry a description.
    for label, description in _BASE_CLASSES:
        assert description and label not in description.split()[:0]

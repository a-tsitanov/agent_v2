"""Unit tests for `src.storage.wikibase.push_entities`.

The :class:`AsyncWikibase` SDK wrapper is fully mocked — these tests
exercise the orchestrator's partition + lookup + claim-assembly
logic without round-tripping any real Wikibase instance.

Neo4j is also mocked via ``structured_query.return_value`` to
control the existing-QID branch.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from llama_index.core.graph_stores.types import EntityNode, Relation

from src.storage.wikibase import push_entities

# ── fixtures / factories ──────────────────────────────────────────


def _wb_client() -> Any:
    """Build an AsyncMock-backed stand-in for ``AsyncWikibase``."""
    wb = MagicMock()
    wb.create_item = AsyncMock(return_value="Q123")
    wb.update_item = AsyncMock(return_value=None)
    wb.add_statement = AsyncMock(return_value=None)
    wb.create_property = AsyncMock(return_value="P999")
    return wb


def _neo4j_store(existing_qid: str | None = None) -> Any:
    gs = MagicMock()
    if existing_qid is not None:
        gs.structured_query.return_value = [{"qid": existing_qid}]
    else:
        gs.structured_query.return_value = []
    return gs


def _base_class_qids() -> dict[str, str]:
    return {
        "Person": "Q1",
        "Organization": "Q2",
    }


def _property_pids() -> dict[str, str]:
    return {
        "instance_of": "P1",
        "er_canonical_name": "P2",
        "mention_count": "P3",
        "PhoneNumber": "P10",
        "Email": "P11",
    }


# ── tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_new_entity_gets_qid_and_writes_back_to_neo4j() -> None:
    """Absent QID in Neo4j → ``create_item`` fires → QID stamped back
    onto the ``:__Entity__`` node via ``SET n.wikibase_qid``.
    """
    person = EntityNode(name="Анна Морозова", label="Person")
    wb = _wb_client()
    gs = _neo4j_store(existing_qid=None)

    counts = await push_entities(
        entities=[person],
        relations=[],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=_base_class_qids(),
        property_pids=_property_pids(),
    )

    wb.create_item.assert_awaited_once()
    wb.update_item.assert_not_awaited()
    # One MATCH-lookup query + one SET-qid persist query.
    persist_calls = [
        c for c in gs.structured_query.call_args_list
        if "SET n.wikibase_qid" in c.args[0]
    ]
    assert len(persist_calls) == 1
    assert persist_calls[0].kwargs["param_map"] == {
        "name": "Анна Морозова", "qid": "Q123",
    }
    assert counts["created_items"] == 1
    assert counts["updated_items"] == 0


@pytest.mark.asyncio
async def test_existing_qid_takes_update_path() -> None:
    """Neo4j returns an existing QID → ``update_item`` fires, no create."""
    person = EntityNode(name="Анна Морозова", label="Person")
    wb = _wb_client()
    gs = _neo4j_store(existing_qid="Q999")

    counts = await push_entities(
        entities=[person],
        relations=[],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=_base_class_qids(),
        property_pids=_property_pids(),
    )

    wb.update_item.assert_awaited_once()
    wb.create_item.assert_not_awaited()
    update_kwargs = wb.update_item.await_args.kwargs
    assert update_kwargs["qid"] == "Q999"
    assert counts["updated_items"] == 1
    assert counts["created_items"] == 0


@pytest.mark.asyncio
async def test_identifier_entity_becomes_external_id_statement_on_owner() -> None:
    """PhoneNumber entity related to a Person → Person Item gets an
    external-id claim ``(P10, "+74951234567")``; the PhoneNumber
    entity itself is NOT pushed as a standalone Item.
    """
    person = EntityNode(name="Анна Морозова", label="Person")
    phone = EntityNode(name="+74951234567", label="PhoneNumber")
    rel = Relation(label="has_phone", source_id=person.id, target_id=phone.id)

    wb = _wb_client()
    gs = _neo4j_store(existing_qid=None)

    counts = await push_entities(
        entities=[person, phone],
        relations=[rel],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=_base_class_qids(),
        property_pids=_property_pids(),
    )

    # Only one Item create — the Person.  PhoneNumber is folded.
    assert wb.create_item.await_count == 1
    create_kwargs = wb.create_item.await_args.kwargs
    assert create_kwargs["label"] == "Анна Морозова"
    # The phone claim is in the claims list as an external-id with P10.
    phone_claims = [
        c for c in create_kwargs["claims"]
        if c[0] == "P10" and c[2] == "external-id"
    ]
    assert phone_claims == [("P10", "+74951234567", "external-id")]
    assert counts["external_id_statements"] == 1
    assert counts["created_items"] == 1


@pytest.mark.asyncio
async def test_lazy_relation_property_creation() -> None:
    """Owner-owner relation with unseen label → ``create_property``
    fires once; the PID is cached both in ``property_pids`` and in
    Neo4j via a MERGE on ``:WikibaseProperty``.
    """
    person = EntityNode(name="Анна Морозова", label="Person")
    org = EntityNode(name="ACME", label="Organization")
    rel = Relation(
        label="EMPLOYMENT", source_id=person.id, target_id=org.id,
    )

    wb = _wb_client()
    # Neo4j returns no existing QID for either entity → both get created.
    gs = _neo4j_store(existing_qid=None)

    property_pids = _property_pids()  # EMPLOYMENT is NOT in here
    assert "EMPLOYMENT" not in property_pids

    counts = await push_entities(
        entities=[person, org],
        relations=[rel],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=_base_class_qids(),
        property_pids=property_pids,
    )

    wb.create_property.assert_awaited_once()
    create_prop_kwargs = wb.create_property.await_args.kwargs
    assert create_prop_kwargs["label"] == "EMPLOYMENT"
    assert create_prop_kwargs["datatype"] == "wikibase-item"
    # PID propagated to in-memory cache.
    assert property_pids["EMPLOYMENT"] == "P999"
    # MERGE :WikibaseProperty query fired.
    wp_persists = [
        c for c in gs.structured_query.call_args_list
        if "MERGE (p:WikibaseProperty" in c.args[0]
    ]
    assert len(wp_persists) == 1
    assert wp_persists[0].kwargs["param_map"] == {
        "label": "EMPLOYMENT", "pid": "P999", "dt": "wikibase-item",
    }
    # Statement linking the two owners was added.
    wb.add_statement.assert_awaited_once()
    assert counts["new_properties_created"] == 1
    assert counts["relation_statements"] == 1


@pytest.mark.asyncio
async def test_orphan_identifier_with_no_owner_relation_is_skipped() -> None:
    """PhoneNumber entity with no relation → no Item created for it,
    and ``external_id_statements`` stays at zero for that ingest.
    """
    phone = EntityNode(name="+74951234567", label="PhoneNumber")

    wb = _wb_client()
    gs = _neo4j_store(existing_qid=None)

    counts = await push_entities(
        entities=[phone],
        relations=[],
        neo4j_store=gs,
        wb_client=wb,
        base_class_qids=_base_class_qids(),
        property_pids=_property_pids(),
    )

    wb.create_item.assert_not_awaited()
    wb.update_item.assert_not_awaited()
    assert counts == {
        "created_items": 0,
        "updated_items": 0,
        "external_id_statements": 0,
        "relation_statements": 0,
        "new_properties_created": 0,
    }

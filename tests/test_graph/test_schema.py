"""Stage-6 schema tests — Pydantic Literal types render to non-empty
sets and the validation schema covers the most common business
relationships."""

from __future__ import annotations

from typing import get_args

from src.graph.schema import (
    DEFAULT_VALIDATION_SCHEMA,
    EntityType,
    RelationType,
)


def test_entity_types_cover_business_identifiers() -> None:
    types = set(get_args(EntityType))
    for required in {
        "Person", "Organization", "PhoneNumber", "INN", "OGRN",
        "BIC", "ContractNumber", "PostalAddress", "DocumentDate",
        "Amount", "Email",
    }:
        assert required in types, f"missing entity type {required!r}"


def test_relation_types_non_empty() -> None:
    types = set(get_args(RelationType))
    assert len(types) >= 5
    for required in {"WORKS_AT", "TAX_ID_OF", "PARTY_OF", "DATED"}:
        assert required in types


def test_validation_schema_uses_defined_types() -> None:
    entity_set = set(get_args(EntityType))
    relation_set = set(get_args(RelationType))
    for head, rel, tail in DEFAULT_VALIDATION_SCHEMA:
        assert head in entity_set, head
        assert tail in entity_set, tail
        assert rel in relation_set, rel

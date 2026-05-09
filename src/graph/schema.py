"""Pydantic-typed schema for ``SchemaLLMPathExtractor``.

Bound to LlamaIndex 0.13's ``SchemaLLMPathExtractor`` which expects
``possible_entities``, ``possible_relations`` and an optional
``kg_validation_schema`` (allowed (head, relation, tail) triples).

Entity types mirror the production set in
``enterprise-kb/src/retrieval/lightrag_setup.py``: business-domain
identifiers (PhoneNumber, INN, OGRN, BIC, ContractNumber,
PostalAddress, DocumentDate, Amount) plus generic ones (Person,
Organization, Location, Concept, Method, Event).  Relation types
encode the most common business links between identifiers and their
holders.

Stage 7 attaches the deterministic identifier transform BEFORE the
extractor runs — by then the canonical text of each identifier
already lives in node metadata, so the LLM doesn't have to invent
the format.
"""

from __future__ import annotations

from typing import Literal

# Entities the extractor is allowed to produce.  Matches the
# `BUSINESS_ENTITY_TYPES` list in enterprise-kb.
EntityType = Literal[
    "Person",
    "Organization",
    "Location",
    "PhoneNumber",
    "Email",
    "ContractNumber",
    "OrderNumber",
    "InvoiceNumber",
    "PostalAddress",
    "INN",
    "OGRN",
    "BIC",
    "BankAccount",
    "DocumentDate",
    "Amount",
    "Concept",
    "Method",
    "Event",
]


# Relation labels.  Keep small — too many labels ⇒ LLM picks
# inconsistent ones.  Add more only if a recurring real-doc pattern
# motivates them.
RelationType = Literal[
    "WORKS_AT",
    "OWNS",
    "CONTACT",
    "PARTY_OF",
    "DATED",
    "AMOUNT_OF",
    "ADDRESS_OF",
    "TAX_ID_OF",
    "REGISTRATION_OF",
    "BANK_OF",
    "REFERENCES",
    "RELATED_TO",
]


# Optional schema: which (head, relation, tail) triples are valid.
# An empty dict means "anything goes".  Constrain heavily for noisy
# corpora; relax for exploratory bring-up.
DEFAULT_VALIDATION_SCHEMA: list[tuple[EntityType, RelationType, EntityType]] = [
    ("Person", "WORKS_AT", "Organization"),
    ("Person", "CONTACT", "PhoneNumber"),
    ("Person", "CONTACT", "Email"),
    ("Organization", "TAX_ID_OF", "INN"),
    ("Organization", "REGISTRATION_OF", "OGRN"),
    ("Organization", "BANK_OF", "BIC"),
    ("Organization", "ADDRESS_OF", "PostalAddress"),
    ("Organization", "PARTY_OF", "ContractNumber"),
    ("Person", "PARTY_OF", "ContractNumber"),
    ("ContractNumber", "DATED", "DocumentDate"),
    ("ContractNumber", "AMOUNT_OF", "Amount"),
    ("Organization", "RELATED_TO", "Organization"),
    ("Person", "RELATED_TO", "Person"),
]


__all__ = [
    "DEFAULT_VALIDATION_SCHEMA",
    "EntityType",
    "RelationType",
]

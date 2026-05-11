"""Pydantic-typed schema for `SchemaLLMPathExtractor`.

Universal entity / relation types covering the project's
heterogeneous corpus: analytical reports, email correspondence,
support call transcripts.

Two layers:

1. **Core types** — people, organizations, generic concepts,
   topics, issues — universal across domains.
2. **Identifier types** — phones, INNs, contract numbers, dates,
   amounts — handled deterministically by the canonicalization
   transform (`src/ingestion/identifier_transform.py`) BEFORE the
   LLM extractor runs.  When LLM extracts them anyway, they
   collapse onto the same canonical node via name-equality.
"""

from __future__ import annotations

from typing import Literal


# ── core entity types (cover reports / emails / transcripts) ────────

EntityType = Literal[
    # who
    "Person",
    "Organization",
    "Location",
    # what
    "Concept",
    "Topic",
    "Metric",
    "Product",
    "Document",
    "Issue",
    "Resolution",
    "EventOrAction",
    # identifiers (handled deterministically; LLM may also extract them)
    "Email",
    "PhoneNumber",
    "PostalAddress",
    "DocumentDate",
    "Amount",
    "ContractNumber",
    "OrderNumber",
    "InvoiceNumber",
    "INN",
    "OGRN",
    "BIC",
    "BankAccount",
]


# ── relation types ───────────────────────────────────────────────────
#
# Kept compact: ~20 labels.  Adding many more makes the LLM pick
# inconsistent labels for the same real-world relation.  Each label
# is intentionally generic enough to cover all three doc types.

RelationType = Literal[
    # affiliation / ownership
    "WORKS_AT",
    "MEMBER_OF",
    "OWNS",
    "AUTHORED",
    # interaction
    "CONTACT",
    "MENTIONS",
    "DISCUSSES",
    "PARTICIPATED_IN",
    "RESPONDED_TO",
    # business / contract
    "PARTY_OF",
    "DATED",
    "AMOUNT_OF",
    "ADDRESS_OF",
    "TAX_ID_OF",
    "REGISTRATION_OF",
    "BANK_OF",
    # support / issue resolution
    "REPORTED",
    "RESOLVED_BY",
    "AFFECTS",
    # generic
    "REFERENCES",
    "RELATED_TO",
]


# ── validation schema ────────────────────────────────────────────────
#
# (head, relation, tail) triples accepted by `SchemaLLMPathExtractor`
# in strict mode.  In non-strict mode it's a hint only.  ~25 templates
# covering analytical-report, email, and support-transcript patterns.

DEFAULT_VALIDATION_SCHEMA: list[tuple[EntityType, RelationType, EntityType]] = [
    # — people in organizations / authoring
    ("Person", "WORKS_AT", "Organization"),
    ("Person", "MEMBER_OF", "Organization"),
    ("Person", "AUTHORED", "Document"),
    ("Person", "AUTHORED", "Concept"),
    # — interaction / discussion (emails + transcripts)
    ("Person", "CONTACT", "PhoneNumber"),
    ("Person", "CONTACT", "Email"),
    ("Person", "MENTIONS", "Topic"),
    ("Person", "MENTIONS", "Concept"),
    ("Person", "DISCUSSES", "Topic"),
    ("Person", "PARTICIPATED_IN", "EventOrAction"),
    ("Person", "RESPONDED_TO", "Person"),
    # — analytical reports
    ("Document", "MENTIONS", "Metric"),
    ("Concept", "RELATED_TO", "Concept"),
    ("Metric", "AFFECTS", "Concept"),
    # — business / contracts
    ("Organization", "TAX_ID_OF", "INN"),
    ("Organization", "REGISTRATION_OF", "OGRN"),
    ("Organization", "BANK_OF", "BIC"),
    ("Organization", "ADDRESS_OF", "PostalAddress"),
    ("Organization", "PARTY_OF", "ContractNumber"),
    ("Person", "PARTY_OF", "ContractNumber"),
    ("ContractNumber", "DATED", "DocumentDate"),
    ("ContractNumber", "AMOUNT_OF", "Amount"),
    ("Organization", "RELATED_TO", "Organization"),
    # — support transcripts
    ("Person", "REPORTED", "Issue"),
    ("Issue", "RESOLVED_BY", "Resolution"),
    ("Issue", "AFFECTS", "Product"),
    ("Resolution", "RELATED_TO", "EventOrAction"),
    # — generic fallback
    ("Person", "RELATED_TO", "Person"),
]


__all__ = [
    "DEFAULT_VALIDATION_SCHEMA",
    "EntityType",
    "RelationType",
]

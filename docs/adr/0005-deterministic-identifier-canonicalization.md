# ADR-0005: Deterministic identifier canonicalization before LLM extraction

- Status: Accepted
- Date: 2026-06-07

## Context

Structured identifiers (phones, emails, INN/OGRN/BIC/SNILS, IBAN, VIN, IMEI,
URLs, social handles, etc.) appear in many surface forms — the same phone as
`+7 (495) 123-45-67` and `8 495 1234567`. If the LLM extracts each verbatim,
Neo4j gets two separate nodes and graph dedup breaks. The graph keys entities
by name, so identical real-world identifiers must collapse to one canonical
name regardless of source formatting.

## Decision

Run a **deterministic, pre-LLM canonicalization stage**: regex/lib-based
detectors (`phonenumbers`, `dateparser`, optional libpostal, plus checksum
validators for INN/OGRN/SNILS/IBAN/IMEI/VIN/credit-card) extract ~24 identifier
types, each normalized to a canonical form (E.164, lowercased email, etc.).
Overlapping matches are resolved by a priority table (specialised > generic).
The transform then (1) upserts one canonical `EntityNode` per
`(entity_type, canonical)` into Neo4j **before** extraction
(`inject_canonical` activity), and (2) appends a
`Канонические идентификаторы:` block to the chunk text so the LLM is taught to
use the canonical form in `entity_name`. These types are excluded from Entity
Resolution (ADR-0007).

## Consequences

- Identical identifiers collapse to one node deterministically, cheaply, and
  without an LLM call; checksums reject false positives (random digit runs).
- The canonical node is guaranteed to exist before LLM relationship extraction.
- Commits us to maintaining per-type detectors/validators and the priority
  table; libpostal is optional with a rule-based fallback.

## Alternatives considered

- **Let the LLM extract identifiers verbatim** — produces duplicate nodes,
  non-deterministic forms, and costs tokens for work a regex+checksum does
  exactly.
- **Post-hoc dedup of identifiers in ER** — risks merging two genuinely
  different identifiers that embed close; deterministic canon is safer.

## References

- `src/ingestion/identifiers.py`, `src/ingestion/identifier_transform.py`,
  `src/workflow/activities/inject_canonical.py`; `_DETERMINISTIC_LABELS` in
  `src/graph/entity_resolution.py`
- `docs/INGEST.md`; CONCEPTS.md → "Identifier canonicalization"

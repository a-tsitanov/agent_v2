"""Constants + pure helpers for the analytical layer (no I/O, no LLM)."""

from __future__ import annotations

import re
from datetime import date, timedelta

from src.graph.schema import EntityType

# The entity label is a literal string everywhere in the graph (no constant
# exists in src/graph/schema.py — it is established by the extractor/store).
ENTITY_LABEL = "__Entity__"

# Canonical entity-type names (lowercased for case-insensitive lookup →
# canonical TitleCase). Sourced from the single EntityType Literal so this
# never drifts from the schema.
_CANON_BY_LOWER: dict[str, str] = {t.lower(): t for t in EntityType.__args__}
VALID_ENTITY_TYPES: frozenset[str] = frozenset(_CANON_BY_LOWER)


def coerce_entity_type(v: str | None) -> str | None:
    """Map a user/LLM-supplied entity type to its canonical casing, or None
    if it is not a real EntityType. Guards against the planner filling the
    `type` param with a bogus value (e.g. "entity"/"сущность") that would
    otherwise match zero rows."""
    if not v:
        return None
    return _CANON_BY_LOWER.get(v.strip().lower())


# The 12 identifier entity types (the identifier block of EntityType in
# src/graph/schema.py:25-52). Many aggregates exclude these by default.
ID_TYPES: list[str] = [
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

_ID_TYPES_SET = frozenset(ID_TYPES)
# A name is "number-ish" if, stripped of digits/%/spaces/currency/punctuation,
# nothing meaningful remains (e.g. "60%", "7,2 трлн" → drop; "822" → drop).
_NUMERISH_RE = re.compile(
    r"^[\d\s.,%€$₽+–—-]*(?:трлн|млрд|млн|тыс|%)?[\d\s.,%€$₽+–—-]*$", re.IGNORECASE
)

# A name that is really a URL, not an entity (e.g. "https://kod.ru/niva").
# Conservative on purpose: only the unambiguous `scheme://` / leading `www.`
# shapes trip it, so dotted real names ("U.S.A.", "BAE Systems") never match.
_URL_RE = re.compile(r"^(?:https?://|www\.)|://", re.IGNORECASE)


def is_meaningful_entity(
    name: str | None, type: str | None, *, exclude_identifiers: bool = True
) -> bool:
    """Whether an entity row is worth showing in a themes/events answer.

    Always drops: empty/single-char names and names equal to their own type
    label (e.g. "Concept"/"Concept"). When ``exclude_identifiers`` (default
    True), also drops identifier-typed rows (``ID_TYPES``), pure
    number/percent/amount names (e.g. "60%", "7,2 трлн", "822"), and
    URL-shaped names (e.g. "https://kod.ru/niva") — pass
    ``exclude_identifiers=False`` to opt back into raw identifier-ish rows.
    Keeps genuine named entities/events.
    """
    n = (name or "").strip()
    if len(n) < 2:
        return False
    if type and n.lower() == type.lower():
        return False
    if exclude_identifiers:
        if (type or "") in _ID_TYPES_SET:
            return False
        if _NUMERISH_RE.match(n):
            return False
        if _URL_RE.search(n):
            return False
    return True


_EPOCH = date(1970, 1, 1)


def clamp_top_n(n: int | None, *, default: int = 20, hard_max: int = 200) -> int:
    """Clamp a requested row cap into ``[1, hard_max]``; ``None``/<=0 → default."""
    if not n or n <= 0:
        return default
    return min(int(n), hard_max)


def epoch_days_to_period(epoch: int, granularity: str = "month") -> str:
    """Bucket an epoch-day integer into a period label.

    granularity: ``year`` → ``"2024"`` · ``quarter`` → ``"2024-Q1"`` ·
    ``month`` (default) → ``"2024-03"``.
    """
    d = _EPOCH + timedelta(days=int(epoch))
    if granularity == "year":
        return f"{d.year:04d}"
    if granularity == "quarter":
        return f"{d.year:04d}-Q{(d.month - 1) // 3 + 1}"
    return f"{d.year:04d}-{d.month:02d}"

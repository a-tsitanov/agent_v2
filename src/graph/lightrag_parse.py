"""Parser for LightRAG-style extraction output.

Consumes the `<|#|>`-delimited text the LLM emits and returns
(entities, relations) tuples ready to be stashed on a chunk's
`KG_NODES_KEY` / `KG_RELATIONS_KEY` metadata or fed into the
merge step.

The parser is intentionally lenient — qwen3 / gpt-4o-mini both
occasionally emit malformed lines (extra whitespace, missing
fields, truncation, `<think>` blocks).  Anything we can't parse
is dropped, not raised — the cost of dropping a line is one
fewer entity; the cost of raising would be losing the whole
chunk's output.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM
from src.retrieval._common import strip_thinking

# ── Names: normalisation helpers ─────────────────────────────────────


# Anything outside this set goes to `_` in a Cypher-safe relation label.
_CYPHER_LABEL_RE = re.compile(r"[^A-Z0-9_]+")

# Match a leading "ent"/"rel"/"event" prefix optionally followed by quotes — qwen3
# sometimes wraps the line keyword in quotes (LightRAG's own examples too).
_LEADING_KIND_RE = re.compile(r'^[\s"]*"?(?P<kind>entity|relation|event)"?[\s"]*$', re.IGNORECASE)


def _clean_raw_name(raw: str) -> str:
    """Strip surrounding whitespace and quote/guillemet wrappers."""
    return (raw or "").strip().strip('"').strip("«»").strip()


def _normalize_entity_name(raw: str) -> str:
    """Match-key normalisation: title-case ASCII-only names; preserve names
    containing Cyrillic / CJK / other non-ASCII verbatim.

    Title-case is a *match key* concern — it makes "BCC" / "Bcc" /
    "bcc" collapse to one key so they merge into one entity across
    chunks.  But applying it to "Иванов И.П." would mangle Russian
    proper nouns, so we skip casing whenever any non-ASCII character
    is present.

    This is the DEDUP/LINKING key, not the shown name — every call site
    (parser dedup, relation resolution, entity-resolution blocking)
    re-applies it to ``EntityNode.name``.  The human-facing name is
    ``_display_entity_name`` (acronym-preserving); storing that as the
    name never fragments merge because this key is recomputed on top.
    """
    name = _clean_raw_name(raw)
    if not name:
        return ""
    if name.isascii():
        # Capitalise each whitespace-separated chunk; preserve internal
        # punctuation like dashes and slashes.
        return " ".join(part.capitalize() for part in name.split())
    return name


def _display_entity_name(raw: str) -> str:
    """Human-facing entity name stored on ``EntityNode.name``.

    Title-case *all-lowercase* ASCII words for readability but preserve
    acronyms / CamelCase / mixed-case tokens verbatim — "RSR" stays
    "RSR" (not "Rsr"), "SpaceX" stays "SpaceX" (not "Spacex"), "iPhone"
    stays "iPhone".  Non-ASCII (Cyrillic/CJK) is preserved verbatim, as
    in ``_normalize_entity_name``.

    Only the shown name changes; the dedup/linking key stays
    ``_normalize_entity_name`` (casefold-ish), so cross-chunk merge is
    unaffected — plain ``str.capitalize`` used to smear acronyms and
    Latin brand names, which is a display defect, not a merge concern.
    """
    name = _clean_raw_name(raw)
    if not name or not name.isascii():
        return name
    return " ".join(p.capitalize() if p.islower() else p for p in name.split())


# A real email address: local@domain.tld, no scheme, not a leading-@ handle.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Countries (nominative RU + key EN/abbrev) are routinely mislabeled
# Organization/Other by the extractor — a country is a Location (GPE). Match is
# exact-casefold on the nominative form (the extractor tends to emit it), so no
# over-correction of non-country orgs; declined mentions just aren't corrected.
_COUNTRIES = frozenset(s.casefold() for s in {
    "Россия", "РФ", "Российская Федерация", "Украина", "США", "Соединённые Штаты",
    "Иран", "Ирак", "Китай", "КНР", "Германия", "ФРГ", "Франция", "Великобритания",
    "Англия", "Индия", "Япония", "Турция", "Польша", "Румыния", "Казахстан",
    "Белоруссия", "Беларусь", "Грузия", "Армения", "Азербайджан", "Сирия",
    "Израиль", "Египет", "Бразилия", "Канада", "Италия", "Испания", "Греция",
    "Финляндия", "Швеция", "Норвегия", "Молдова", "Литва", "Латвия", "Эстония",
    "Венгрия", "Чехия", "Словакия", "Болгария", "Сербия", "КНДР", "Южная Корея",
    "Вьетнам", "Афганистан", "Саудовская Аравия", "ОАЭ", "Катар", "Йемен", "Мали",
    "Монако", "Ливия", "Судан", "Нигерия", "Пакистан", "Индонезия", "Мексика",
    "Аргентина", "Куба", "Венесуэла", "Узбекистан", "Киргизия", "Таджикистан",
    "Монголия", "Австралия", "Нидерланды", "Бельгия", "Австрия", "Швейцария",
    "Португалия", "Ирландия", "Дания", "Хорватия", "Словения", "Черногория",
    "Russia", "Ukraine", "Iran", "China", "Germany", "France", "USA", "UK",
    "India", "Japan", "Turkey", "Poland", "Romania", "Greece", "Kazakhstan",
})


def _correct_entity_label(name: str, label: str) -> str:
    """Fix common entity-label mistakes from the extractor.

    * Countries mislabeled ``Organization``/``Other``/``Concept`` → ``Location``
      (a country is a place, not an org — powers geo-filters/analytics).
    * URLs / @-handles tagged ``Email`` → ``Document`` / ``Other`` (only a real
      ``local@domain.tld`` is an Email).
    Other labels are left untouched."""
    n = (name or "").strip()
    if label in ("Organization", "Other", "Concept") and n.casefold() in _COUNTRIES:
        return "Location"
    if label != "Email":
        return label
    if n.startswith(("http://", "https://", "www.")) or "://" in n:
        return "Document"
    if _EMAIL_RE.match(n) and not n.startswith("@"):
        return "Email"
    return "Other"


def _cypher_safe_label(raw: str) -> str:
    """Convert a free-text predicate/keyword to a Cypher-safe upper-case
    relation label."""
    cleaned = (raw or "").strip().upper()
    cleaned = _CYPHER_LABEL_RE.sub("_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "RELATED"


def _first_keyword(keywords_field: str) -> str:
    """Extract the first comma-separated keyword from the relation
    `relationship_keywords` field."""
    for kw in (keywords_field or "").split(","):
        kw = kw.strip()
        if kw:
            return kw
    return ""


def _normalize_polarity(raw: str) -> str:
    """Map the LLM's free-text polarity to one of
    `affirmed` / `negated` / `uncertain`.

    Logical polarity (NOT sentiment): does the text *assert* the
    relation, *deny* it, or hedge?  Anything unrecognised defaults to
    `affirmed` so legacy / malformed extractions read as plain facts.
    """
    val = (raw or "").strip().lower()
    if val.startswith("neg"):
        return "negated"
    if val.startswith(("uncert", "unsure", "unknown", "doubt", "maybe")):
        return "uncertain"
    return "affirmed"


# Validity bound: ISO YYYY / YYYY-MM / YYYY-MM-DD only. Anything else the
# LLM improvises ("2024-XX", "март 2024", "Q1 2024") is dropped to None —
# non-ISO strings poison the lexicographic date-window comparisons in
# whats_changed/relationship_timeline (40 such rels reached prod, 2026-07-05).
_ISO_BOUND_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")


def _valid_bound(raw: str) -> str | None:
    val = (raw or "").strip()
    m = _ISO_BOUND_RE.match(val)
    if not m:
        return None
    _, mm, dd = m.groups()
    if mm is not None and not 1 <= int(mm) <= 12:
        return None
    if dd is not None and not 1 <= int(dd) <= 31:
        return None
    return val


def _parse_temporal(raw: str) -> tuple[str | None, str | None]:
    """Parse a `from..to` validity window into `(valid_from, valid_to)`.

    * `"2015..2020"` → `("2015", "2020")`
    * `"..2020"` → `(None, "2020")` (open start)
    * `"2015.."` → `("2015", None)` (open end)
    * bare `"2024-03-15"` (no `..`) → `("2024-03-15", None)` (point/start)
    * empty / `none` / `null` → `(None, None)`

    Each bound must be ISO `YYYY[-MM[-DD]]` (see ``_valid_bound``) so dates
    sort lexicographically for the merge window-widening; a non-ISO bound
    is dropped independently (the other side of the window survives).
    """
    val = (raw or "").strip()
    if not val or val.lower() in {"none", "null", "n/a", "-"}:
        return None, None
    if ".." in val:
        left, _, right = val.partition("..")
        return (_valid_bound(left), _valid_bound(right))
    return (_valid_bound(val), None)


def drop_unsupported_dates(relations: list[ParsedRelation], chunk_text: str) -> int:
    """Null out ``valid_from``/``valid_to`` bounds whose YEAR does not
    literally appear in ``chunk_text``; returns how many bounds were dropped.

    Anti-fabrication guard: the extraction model never sees the document
    date, so a date whose year is absent from the chunk cannot have been
    read from the text — it was copied from the prompt's instructions or
    few-shot examples (342 prod rels carried a few-shot date, 2026-07-05).
    Year-substring is a deliberate heuristic: it keeps «15.03.2024» /
    «в 2024 году» dates and only lets a copied date through when the same
    year genuinely occurs in the chunk for other reasons."""
    dropped = 0
    for rel in relations:
        for attr in ("valid_from", "valid_to"):
            bound = getattr(rel, attr)
            if bound and bound[:4] not in chunk_text:
                setattr(rel, attr, None)
                dropped += 1
    return dropped


# ── Event ts sanity gate ────────────────────────────────────────────

_TS_POLARITY_LITERALS = {"affirmed", "negated", "uncertain"}
_TS_PLACEHOLDERS = {
    "empty", "unknown", "none", "null", "n/a", "-", "not specified",
    "не указано", "не указана", "неизвестно", "дата неизвестна", "дата не указана",
    "нет времени", "нет даты", "время не указано",
}
_TS_COORD_RE = re.compile(r"^-?\d{1,3}\.\d+\s*[,;]\s*-?\d{1,3}\.\d+$")
_TS_MAX_LEN = 64


def _sanitize_event_ts(value: str | None) -> str | None:
    """Verbatim time phrase or None — reject polarity/location/participant
    debris that slides into the ts position on malformed tuples."""
    v = (value or "").strip()
    if not v or len(v) > _TS_MAX_LEN:
        return None
    if v.lower().strip("().") in _TS_POLARITY_LITERALS | _TS_PLACEHOLDERS:
        return None
    if ";" in v or _TS_COORD_RE.match(v):
        return None
    return v


# ── Parsing ─────────────────────────────────────────────────────────


@dataclass
class ParsedEvent:
    """Intermediate parsed event tuple."""

    event_type: str
    trigger: str
    participants: list[str]
    event_ts: str | None
    location: str | None
    polarity: str
    source_chunk_id: str
    file_path: str


@dataclass
class ParsedRelation:
    """Intermediate parsed relation before resolving names → node ids."""

    source_name: str
    target_name: str
    keywords: str  # raw "kw1, kw2"
    description: str
    weight: float = 1.0
    polarity: str = "affirmed"  # affirmed | negated | uncertain
    valid_from: str | None = None  # window start (opaque ISO string)
    valid_to: str | None = None  # window end


@dataclass
class ParseResult:
    entities: list[EntityNode] = field(default_factory=list)
    relations: list[ParsedRelation] = field(default_factory=list)
    events: list[ParsedEvent] = field(default_factory=list)


def parse_lightrag_output(
    raw: str,
    *,
    source_chunk_id: str | None = None,
    file_path: str | None = None,
    tuple_delimiter: str = TUPLE_DELIM,
    completion_delimiter: str = COMPLETE_DELIM,
) -> ParseResult:
    """Parse one LightRAG extract response.

    Strategy:

    1. Strip `<think>...</think>` blocks (qwen3 leaks them through).
    2. Iterate lines; stop at the completion sentinel.
    3. For each line that contains the tuple delimiter, look at the
       first field — `entity` or `relation` — and dispatch.
    4. Drop any line that doesn't fit the contract.

    Returns a `ParseResult` whose `relations` carry name references;
    name → `EntityNode.id` resolution happens in the merger so the
    parser stays storage-agnostic.
    """
    text = strip_thinking(raw or "")
    out = ParseResult()
    seen_entity_names: set[str] = set()

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if completion_delimiter in line:
            break
        if tuple_delimiter not in line:
            continue

        fields = [f.strip() for f in line.split(tuple_delimiter)]
        kind_match = _LEADING_KIND_RE.match(fields[0])
        if not kind_match:
            continue
        kind = kind_match.group("kind").lower()

        if kind == "entity":
            ent = _parse_entity(
                fields=fields[1:],
                source_chunk_id=source_chunk_id,
                file_path=file_path,
            )
            if ent is None:
                continue
            key = _normalize_entity_name(ent.name)
            if key in seen_entity_names:
                continue
            seen_entity_names.add(key)
            out.entities.append(ent)

        elif kind == "relation":
            rel = _parse_relation(fields[1:])
            if rel is None:
                continue
            out.relations.append(rel)

        elif kind == "event":
            ev = _parse_event(
                fields=fields,
                source_chunk_id=source_chunk_id,
                file_path=file_path,
            )
            if ev is None:
                continue
            out.events.append(ev)

    return out


def _parse_event(
    fields: list[str],
    *,
    source_chunk_id: str | None,
    file_path: str | None,
) -> ParsedEvent | None:
    """Parse an event line.

    Full format: event<|#|>event_type<|#|>trigger<|#|>participants(;)<|#|>time<|#|>location<|#|>polarity
    Fields[0] is the `event` kind keyword; data starts at fields[1].
    """
    if len(fields) < 3:
        return None
    event_type = (fields[1] if len(fields) > 1 else "").strip() or "event"
    trigger = (fields[2] if len(fields) > 2 else "").strip()
    raw_parts = fields[3] if len(fields) > 3 else ""
    participants = [p.strip() for p in raw_parts.split(";") if p.strip()]
    # ts position is only trustworthy on a full 7-field tuple; on shorter
    # tuples neighboring fields slide into it (audited 2026-07-05).
    event_ts = _sanitize_event_ts(fields[4]) if len(fields) >= 7 else None
    location = (fields[5].strip() or None) if len(fields) > 5 else None
    polarity = _normalize_polarity(fields[6]) if len(fields) > 6 else "affirmed"
    return ParsedEvent(
        event_type=event_type,
        trigger=trigger,
        participants=participants,
        event_ts=event_ts,
        location=location,
        polarity=polarity,
        source_chunk_id=source_chunk_id or "",
        file_path=file_path or "",
    )


def _parse_entity(
    *,
    fields: list[str],
    source_chunk_id: str | None,
    file_path: str | None,
) -> EntityNode | None:
    """Return an `EntityNode` for a valid (name, type, description)
    triple; `None` for anything malformed."""
    if len(fields) < 3:
        return None
    name, etype, description = fields[0], fields[1], fields[2]
    # Store the acronym-preserving display name; dedup/linking below and in
    # entity-resolution recompute the match key via _normalize_entity_name.
    name = _display_entity_name(name)
    if not name or not description.strip():
        # Skip entities without a body — LightRAG-style merge needs
        # at least one non-empty description to start with.
        return None
    label = (etype or "Other").strip() or "Other"
    label = _correct_entity_label(name, label)
    properties: dict = {"description": description.strip()}
    if source_chunk_id:
        properties["source_chunk_id"] = source_chunk_id
    if file_path:
        properties["file_path"] = file_path
    return EntityNode(
        name=name,
        label=label,
        properties=properties,
    )


def _parse_relation(fields: list[str]) -> ParsedRelation | None:
    """Return a `ParsedRelation` for a valid (src, tgt, keywords,
    description[, polarity, temporal]) tuple; `None` for anything
    malformed.

    Fields 5 (polarity) and 6 (temporal validity `from..to`) are
    optional — legacy 5-field relations parse with defaults
    (affirmed, no window)."""
    if len(fields) < 4:
        return None
    src, tgt, keywords, description = fields[0], fields[1], fields[2], fields[3]
    src = _normalize_entity_name(src)
    tgt = _normalize_entity_name(tgt)
    if not src or not tgt or src == tgt:
        return None
    if not description.strip():
        return None
    polarity = _normalize_polarity(fields[4]) if len(fields) > 4 else "affirmed"
    valid_from, valid_to = _parse_temporal(fields[5]) if len(fields) > 5 else (None, None)
    return ParsedRelation(
        source_name=src,
        target_name=tgt,
        keywords=keywords.strip(),
        description=description.strip(),
        polarity=polarity,
        valid_from=valid_from,
        valid_to=valid_to,
    )


def parsed_relations_to_relations(
    parsed: list[ParsedRelation],
    entity_id_by_name: dict[str, str],
    *,
    source_chunk_id: str | None = None,
) -> list[Relation]:
    """Resolve `ParsedRelation` name refs → `EntityNode.id` and produce
    LlamaIndex `Relation` instances.  Drops relations whose source or
    target name doesn't appear in the id map (means the LLM referenced
    an entity it didn't extract — drop or create ad-hoc; we choose to
    create ad-hoc orphan entries upstream)."""
    out: list[Relation] = []
    for rel in parsed:
        sid = entity_id_by_name.get(_normalize_entity_name(rel.source_name))
        tid = entity_id_by_name.get(_normalize_entity_name(rel.target_name))
        if sid is None or tid is None:
            continue
        label = _cypher_safe_label(_first_keyword(rel.keywords))
        if not label or label == "_":
            label = "RELATED"
        properties: dict = {
            "description": rel.description,
            "keywords": rel.keywords,
            "weight": rel.weight,
            "polarity": rel.polarity,
            "valid_from": rel.valid_from,
            "valid_to": rel.valid_to,
        }
        if source_chunk_id:
            properties["source_chunk_id"] = source_chunk_id
        out.append(
            Relation(
                label=label,
                source_id=sid,
                target_id=tid,
                properties=properties,
            )
        )
    return out


def ensure_orphan_entities(
    parsed: list[ParsedRelation],
    entity_id_by_name: dict[str, str],
    *,
    source_chunk_id: str | None = None,
) -> list[EntityNode]:
    """For every relation whose endpoint isn't in `entity_id_by_name`,
    synthesise a minimal `EntityNode(label='Other')` so the relation
    can still be stored.  LightRAG does the same — it preserves edges
    even when the LLM forgot to list one of the endpoints as a
    standalone entity."""
    out: list[EntityNode] = []
    seen: set[str] = set()
    for rel in parsed:
        for name in (rel.source_name, rel.target_name):
            normalized = _normalize_entity_name(name)
            if not normalized:
                continue
            if normalized in entity_id_by_name or normalized in seen:
                continue
            seen.add(normalized)
            ent = EntityNode(
                name=normalized,
                label="Other",
                properties={
                    "description": "",
                    "source_chunk_id": source_chunk_id or "",
                    "orphan": True,
                },
            )
            out.append(ent)
            entity_id_by_name[normalized] = ent.id
    return out


__all__ = [
    "ParseResult",
    "ParsedEvent",
    "ParsedRelation",
    "ensure_orphan_entities",
    "parse_lightrag_output",
    "parsed_relations_to_relations",
]

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
import uuid
from dataclasses import dataclass, field

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_prompts import COMPLETE_DELIM, TUPLE_DELIM
from src.retrieval._common import strip_thinking


# ── Names: normalisation helpers ─────────────────────────────────────


# Anything outside this set goes to `_` in a Cypher-safe relation label.
_CYPHER_LABEL_RE = re.compile(r"[^A-Z0-9_]+")

# Match a leading "ent"/"rel" prefix optionally followed by quotes — qwen3
# sometimes wraps the line keyword in quotes (LightRAG's own examples too).
_LEADING_KIND_RE = re.compile(r'^[\s"]*"?(?P<kind>entity|relation)"?[\s"]*$', re.IGNORECASE)


def _normalize_entity_name(raw: str) -> str:
    """Title-case ASCII-only names; preserve names containing
    Cyrillic / CJK / other non-ASCII verbatim.

    Title-case is a *match key* concern — it makes "BCC" / "Bcc" /
    "bcc" merge into one entity across chunks.  But applying it to
    "Иванов И.П." would mangle Russian proper nouns, so we skip
    casing whenever any non-ASCII character is present.
    """
    name = (raw or "").strip().strip('"').strip("«»").strip()
    if not name:
        return ""
    if name.isascii():
        # Capitalise each whitespace-separated chunk; preserve internal
        # punctuation like dashes and slashes.
        return " ".join(part.capitalize() for part in name.split())
    return name


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


# ── Parsing ─────────────────────────────────────────────────────────


@dataclass
class ParsedRelation:
    """Intermediate parsed relation before resolving names → node ids."""

    source_name: str
    target_name: str
    keywords: str           # raw "kw1, kw2"
    description: str
    weight: float = 1.0


@dataclass
class ParseResult:
    entities: list[EntityNode] = field(default_factory=list)
    relations: list[ParsedRelation] = field(default_factory=list)


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

    return out


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
    name = _normalize_entity_name(name)
    if not name or not description.strip():
        # Skip entities without a body — LightRAG-style merge needs
        # at least one non-empty description to start with.
        return None
    label = (etype or "Other").strip() or "Other"
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
    description) tuple; `None` for anything malformed."""
    if len(fields) < 4:
        return None
    src, tgt, keywords, description = fields[0], fields[1], fields[2], fields[3]
    src = _normalize_entity_name(src)
    tgt = _normalize_entity_name(tgt)
    if not src or not tgt or src == tgt:
        return None
    if not description.strip():
        return None
    return ParsedRelation(
        source_name=src,
        target_name=tgt,
        keywords=keywords.strip(),
        description=description.strip(),
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
        }
        if source_chunk_id:
            properties["source_chunk_id"] = source_chunk_id
        out.append(Relation(
            label=label,
            source_id=sid,
            target_id=tid,
            properties=properties,
        ))
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
    "ParsedRelation",
    "ensure_orphan_entities",
    "parse_lightrag_output",
    "parsed_relations_to_relations",
]

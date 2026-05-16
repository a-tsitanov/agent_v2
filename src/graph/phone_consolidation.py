"""Collapse LightRAG-extracted PhoneNumber duplicates onto their
canonical E.164 form.

ER excludes PhoneNumber from semantic merging on purpose: two
different numbers can embed close (same country/area code).  But
two non-semantic paths legitimately produce duplicates of the SAME
phone — the canonical injector and the LightRAG extractor.  This
helper parses digits via libphonenumber, builds an E.164 form, and
merges any PhoneNumber entities whose digits resolve to the same
canonical into one.

Used by the `merge_and_resolve` workflow activity.
"""

from __future__ import annotations

from typing import Any

from loguru import logger


def consolidate_phone_entities(
    entities: list[Any],
    relations: list[Any],
    nodes: list[Any] | None = None,
) -> tuple[list[Any], list[Any], dict[str, str]]:
    """Collapse LLM-extracted PhoneNumber duplicates onto their
    canonical E.164 form.

    Why a separate pass: ER excludes PhoneNumber from semantic merge
    on purpose (close cosine between two different numbers is
    expected — same country / area code).  But two non-semantic
    paths legitimately produce duplicates of the SAME phone:

      * `inject_canonical_entities` writes deterministic canonical
        nodes ("+74951234567") with label=PhoneNumber.
      * `LightRAGExtractor` reads the augment block ("Канонические
        идентификаторы: +74951234567 ...") + the original text and
        emits its own PhoneNumber entities ("Телефон +7 (495)...",
        "Горячая линия 8-800-...").

    Both end up as separate Neo4j nodes because they have different
    names.  This helper parses digits via libphonenumber, builds a
    canonical E.164, and merges every PhoneNumber entity whose
    digits resolve to the same canonical into one.
    """
    import phonenumbers
    from llama_index.core.graph_stores.types import EntityNode, Relation

    # name → canonical_phone_or_None.  We only consolidate when
    # libphonenumber can parse a valid number.
    name_to_canonical: dict[str, str] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        if (ent.label or "") != "PhoneNumber":
            continue
        # libphonenumber tolerates noisy prefixes ("Телефон ...") —
        # try the whole name first, then digits-only as fallback.
        canon: str | None = None
        for region in ("RU", "GB", None):
            try:
                matches = list(phonenumbers.PhoneNumberMatcher(ent.name, region))
            except Exception:  # noqa: BLE001
                continue
            if matches:
                canon = phonenumbers.format_number(
                    matches[0].number, phonenumbers.PhoneNumberFormat.E164,
                )
                break
        if canon is None:
            continue
        name_to_canonical[ent.name] = canon

    if not name_to_canonical:
        return entities, relations, {}

    # Group entities by canonical.  Pick the entity ALREADY in
    # canonical form as the survivor; otherwise create one.
    by_canonical: dict[str, list[EntityNode]] = {}
    for ent in entities:
        if not isinstance(ent, EntityNode):
            continue
        canon = name_to_canonical.get(ent.name)
        if canon is None:
            continue
        by_canonical.setdefault(canon, []).append(ent)

    # Build the merge map: old_entity_id → survivor_entity_id.
    id_remap: dict[str, str] = {}
    survivors_by_canonical: dict[str, EntityNode] = {}
    consolidated_ids: set[str] = set()
    for canon, group in by_canonical.items():
        if len(group) < 2:
            # Unique entry — only rename to canonical for consistency.
            ent = group[0]
            if ent.name != canon:
                aliases = list((ent.properties or {}).get("aliases", []))
                if ent.name not in aliases:
                    aliases.append(ent.name)
                ent.name = canon
                (ent.properties or {})["aliases"] = aliases
            survivors_by_canonical[canon] = ent
            continue
        # Prefer the entity whose name IS the canonical form.
        survivor = next(
            (e for e in group if e.name == canon), group[0],
        )
        aliases = list((survivor.properties or {}).get("aliases", []))
        mention_count = int((survivor.properties or {}).get("mention_count", 1) or 1)
        descs = [str((survivor.properties or {}).get("description", "") or "")]
        source_chunks: list[str] = list(
            (survivor.properties or {}).get("source_chunks", []) or [],
        )
        file_paths: list[str] = list(
            (survivor.properties or {}).get("file_paths", []) or [],
        )
        for other in group:
            if other is survivor:
                continue
            if other.name not in aliases and other.name != canon:
                aliases.append(other.name)
            mention_count += int(
                (other.properties or {}).get("mention_count", 1) or 1
            )
            d = str((other.properties or {}).get("description", "") or "")
            if d and d not in descs:
                descs.append(d)
            for cid in (other.properties or {}).get("source_chunks", []) or []:
                if cid not in source_chunks:
                    source_chunks.append(cid)
            for fp in (other.properties or {}).get("file_paths", []) or []:
                if fp not in file_paths:
                    file_paths.append(fp)
            id_remap[other.id] = survivor.id
            consolidated_ids.add(other.id)
        survivor.name = canon
        if survivor.properties is None:
            survivor.properties = {}
        survivor.properties["aliases"] = aliases
        survivor.properties["mention_count"] = mention_count
        survivor.properties["description"] = "\n---\n".join(d for d in descs if d)
        survivor.properties["source_chunks"] = source_chunks
        survivor.properties["file_paths"] = file_paths
        survivors_by_canonical[canon] = survivor

    # Drop consolidated entities from the list.
    new_entities = [
        e for e in entities
        if not (isinstance(e, EntityNode) and e.id in consolidated_ids)
    ]

    # Rewrite relations: any source_id / target_id that was merged
    # away points at the survivor now.  Drop self-loops.
    new_relations = []
    seen_pairs: set[tuple[str, str, str]] = set()
    for rel in relations:
        if not isinstance(rel, Relation):
            new_relations.append(rel)
            continue
        src = id_remap.get(rel.source_id, rel.source_id)
        tgt = id_remap.get(rel.target_id, rel.target_id)
        if src == tgt:
            continue
        key = (src, tgt, rel.label or "")
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        rel.source_id = src
        rel.target_id = tgt
        new_relations.append(rel)

    # Build name_map (old → canonical) so the caller can rewrite
    # chunk-level KG_NODES_KEY metadata.  Without this rewrite,
    # PropertyGraphIndex creates Neo4j nodes from the chunks'
    # original names (e.g. "Телефон +7 (495)...") parallel to the
    # canonical ones we just upserted — producing duplicates again.
    phone_name_map: dict[str, str] = {
        name: canon for name, canon in name_to_canonical.items()
        if name != canon
    }
    if nodes and phone_name_map:
        from llama_index.core.graph_stores.types import (
            KG_NODES_KEY as _KG_NODES_KEY,
        )
        for node in nodes:
            md = getattr(node, "metadata", None)
            ents = (md or {}).get(_KG_NODES_KEY) or []
            for ent in ents:
                if not isinstance(ent, EntityNode):
                    continue
                canonical = phone_name_map.get(ent.name)
                if canonical:
                    ent.name = canonical

    logger.info(
        "phone consolidation  total_phones={t}  consolidated={c}  "
        "surviving={s}  renamed_chunks={r}",
        t=len(name_to_canonical), c=len(consolidated_ids),
        s=len(survivors_by_canonical), r=len(phone_name_map),
    )
    return new_entities, new_relations, phone_name_map

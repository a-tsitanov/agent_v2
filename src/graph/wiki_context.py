"""Read an entity's 1-hop subgraph from Neo4j and hash it for change
detection. The hash covers only graph FACTS (name/label/description +
relations) — NOT the QID, page title, or citations — so the article is
regenerated exactly when the facts change."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

# (rel_label, direction "out"|"in", neighbor_name, neighbor_label, rel_description)
Relation = tuple[str, str, str, str, str]


@dataclass
class EntityContext:
    name: str
    label: str
    description: str
    wikibase_qid: str
    page_title: str
    relations: list[Relation] = field(default_factory=list)


def subgraph_hash(ctx: EntityContext) -> str:
    """Stable sha256 over the entity's facts. Order-independent on
    relations (sorted), independent of qid/page_title/citations."""
    rels = sorted(
        "|".join((rl, d, nn, rd)) for (rl, d, nn, _nl, rd) in ctx.relations
    )
    payload = "\x1e".join([ctx.name, ctx.label, ctx.description, *rels])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

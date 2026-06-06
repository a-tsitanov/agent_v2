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
        "\x1f".join((rl, d, nn, rd)) for (rl, d, nn, _nl, rd) in ctx.relations
    )
    payload = "\x1e".join([ctx.name, ctx.label, ctx.description, *rels])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_SUBGRAPH_CYPHER = """
MATCH (e:__Entity__ {name: $name})
OPTIONAL MATCH (e)-[r]-(m:__Entity__)
WITH e,
  collect(CASE WHEN m IS NULL THEN NULL ELSE {
    rl: type(r),
    dir: CASE WHEN startNode(r) = e THEN 'out' ELSE 'in' END,
    nn: m.name,
    nl: head([l IN labels(m) WHERE l <> '__Entity__' AND l <> '__Node__']),
    rd: coalesce(r.description, '')
  } END) AS rels
RETURN e.name AS name,
  head([l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__']) AS label,
  coalesce(e.description, '') AS description,
  coalesce(e.wikibase_qid, '') AS qid,
  coalesce(e.wiki_page_title, '') AS page_title,
  [x IN rels WHERE x IS NOT NULL] AS relations
"""

_CITATIONS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN coalesce(c.text, '') AS text, coalesce(c.doc_id, '') AS doc_id
ORDER BY c.doc_id, text
LIMIT $k
"""


def read_entity_subgraph(store, name: str) -> EntityContext:
    rows = store.structured_query(_SUBGRAPH_CYPHER, param_map={"name": name})
    if not rows:
        raise ValueError(f"entity not found: {name!r}")
    r = rows[0]
    relations = [
        (x["rl"], x["dir"], x["nn"], x.get("nl") or "", x.get("rd") or "")
        for x in (r.get("relations") or [])
    ]
    return EntityContext(
        name=r["name"], label=r.get("label") or "",
        description=r.get("description") or "", wikibase_qid=r.get("qid") or "",
        page_title=(r.get("page_title") or r["name"]), relations=relations,
    )


def read_citations(store, name: str, k: int) -> list[tuple[str, str]]:
    rows = store.structured_query(
        _CITATIONS_CYPHER, param_map={"name": name, "k": k})
    return [(row.get("text") or "", row.get("doc_id") or "") for row in rows]

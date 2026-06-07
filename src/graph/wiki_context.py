"""Read an entity's 1-hop subgraph from Neo4j and hash it for change
detection. The hash covers graph FACTS (name/label/description +
relations) AND the entity's source-document id set — so a NEW source
document also regenerates the article — but NOT the QID, page title, or
citation text, so the article is regenerated exactly when those change."""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
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


def subgraph_hash(ctx: EntityContext, source_doc_ids: Iterable[str] = ()) -> str:
    """Stable sha256 over the entity's facts AND its source-document set.
    Order-independent on relations and doc ids; independent of qid/page_title.
    Folding doc ids in means a NEW source document (which adds a download
    link) regenerates the article even when no 1-hop relation changed."""
    rels = sorted(
        "\x1f".join((rl, d, nn, rd)) for (rl, d, nn, _nl, rd) in ctx.relations
    )
    docs = "\x1d".join(sorted(source_doc_ids or ()))
    payload = "\x1e".join([ctx.name, ctx.label, ctx.description, *rels, docs])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_SUBGRAPH_CYPHER = """
MATCH (e:__Entity__ {name: $name})
OPTIONAL MATCH (e)-[r]-(m:__Entity__)
WITH e, r, m, coalesce(m.mention_count, 0) AS mc
ORDER BY mc DESC, m.name
WITH e, collect(CASE WHEN m IS NULL THEN NULL ELSE {
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
  [x IN rels WHERE x IS NOT NULL][0..$max_rel] AS relations
"""

_CITATIONS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
WITH c.doc_id AS doc_id, c ORDER BY c.text
WITH doc_id, collect(c)[0] AS c
RETURN coalesce(c.text, '') AS text, doc_id
ORDER BY doc_id LIMIT $k
"""


def read_entity_subgraph(store, name: str, max_relations: int = 30) -> EntityContext:
    rows = store.structured_query(
        _SUBGRAPH_CYPHER, param_map={"name": name, "max_rel": max_relations})
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


_SOURCE_DOCS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN DISTINCT c.doc_id AS doc_id ORDER BY doc_id
"""


def read_source_docs(store, name: str) -> list[str]:
    """Distinct source-document ids that mention this entity, sorted.
    Used both for the article's download links and folded into the hash."""
    rows = store.structured_query(_SOURCE_DOCS_CYPHER, param_map={"name": name})
    return [row["doc_id"] for row in rows if row.get("doc_id")]

"""Read an entity's 1-hop subgraph from the graph store and hash it for
change detection. The hash covers graph FACTS (name/label/description +
relations) AND the entity's source-document id set — so a NEW source
document also regenerates the article — but NOT the QID, page title, or
citation text, so the article is regenerated exactly when those change.

Reads route through the backend-dispatched ``WikiGraphOps`` seam
(``src/graph/wiki_graph_ops.py``) so the same call works under both neo4j
(byte-for-byte unchanged Cypher) and nebula (nGQL; citations/source-docs
are chunk-dependent and return ``[]`` under nebula)."""
from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field

from src.graph.wiki_graph_ops import build_wiki_graph_ops

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


def read_entity_subgraph(store, name: str, max_relations: int = 30) -> EntityContext:
    rows = build_wiki_graph_ops(store).read_subgraph(name, max_relations)
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
    rows = build_wiki_graph_ops(store).read_citations(name, k)
    return [(row.get("text") or "", row.get("doc_id") or "") for row in rows]


def read_source_docs(store, name: str) -> list[str]:
    """Distinct source-document ids that mention this entity, sorted.
    Used both for the article's download links and folded into the hash."""
    return build_wiki_graph_ops(store).read_source_docs(name)

"""Graph-search wrapper for the agent loop.

Returns a ``RoundGraphData`` with structured ``entities`` and
``relations`` lists — same shape used by enterprise-kb's
``query_graph_data`` so dedup / accumulation logic ports
directly.

Implementation thin over LlamaIndex's PG retriever:
  * Uses ``LLMSynonymRetriever`` from PropertyGraphIndex by default —
    it normalises query terms via the LLM before traversing.
  * Returns three things the agent cares about:
      - entity dicts (``entity_name``, ``entity_type``,
        ``description``);
      - relationship dicts (``src_id``, ``tgt_id``, ``label``);
      - chunk nodes related to the matched graph elements.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import date

from llama_index.core import PropertyGraphIndex
from llama_index.core.schema import NodeWithScore
from loguru import logger

from src.config import settings
from src.graph.nebula_store import _q as _nebula_q
from src.graph.nebula_store import entity_vid

# ── bounded multi-hop walk caps (R3) ─────────────────────────────────
# Hard ceilings so a deep/dense traversal can never blow up the agent's
# context. Enforced both in the Cypher LIMIT (server side) and again
# when mapping rows (defensive truncation).
GRAPH_WALK_MAX_HOPS = 3
GRAPH_WALK_NODE_CAP = 50
GRAPH_WALK_EDGE_CAP = 100

# Hard ceiling for the similarity-path retriever's ``path_depth`` (how
# many triplet-hops of neighbours ``aretrieve`` pulls around each matched
# entity). Bounds rel-map blow-up the same way the walk caps bound awalk.
GRAPH_PATH_DEPTH_MAX = 3

# ── relation polarity / temporal validity filtering (#8) ─────────────
# merge.py stores a logical ``polarity`` (majority vote) and an opaque ISO
# validity window (``valid_from`` / ``valid_to``) on each relationship.
# lightrag_parse normalises polarity to exactly one of these three values;
# anything missing/unrecognised reads as "affirmed". Edges in
# EXCLUDED_POLARITIES are dropped at retrieval; 'uncertain' is KEPT by
# default (it is a hedge, not a denial). Override the set here to change.
EXCLUDED_POLARITIES = frozenset({"negated"})


def _relation_is_live(rel: dict, *, now_iso: str) -> bool:
    """True if a walk-relation dict should be surfaced to the agent.

    Drops edges the source text NEGATES (``polarity`` in
    ``EXCLUDED_POLARITIES``) and edges whose ``valid_to`` is strictly
    before ``now_iso`` (expired). NULL/missing polarity ⇒ affirmed; NULL
    ``valid_to`` ⇒ never-expiring; legacy edges lacking both props pass.

    Temporal compare is lexicographic on ISO strings, which is correct for
    same-shaped ISO dates ("2020" < "2026-06-16", "2020-01-01" <
    "2026-06-16"). Mixed precision compares by common prefix, which keeps
    a year-only ``valid_to`` of a past year correctly expired.
    """
    polarity = rel.get("polarity")
    if polarity is not None and str(polarity).strip().lower() in EXCLUDED_POLARITIES:
        return False
    valid_to = rel.get("valid_to")
    if valid_to:
        # Compare on the overlapping prefix length so "2020" vs
        # "2026-06-16" doesn't mis-rank on length.
        vt = str(valid_to)
        cmp = now_iso[: len(vt)]
        if vt < cmp:
            return False
    return True


@dataclass
class RoundGraphData:
    entities: list[dict] = field(default_factory=list)
    relations: list[dict] = field(default_factory=list)
    chunks: list[NodeWithScore] = field(default_factory=list)
    # Why this came back empty, when it did so because the lookup could
    # not run. Empty string means it ran. Without this an infrastructure
    # refusal is indistinguishable from "the graph has no such entity" —
    # which is exactly how a `GraphMemoryExceeded` spent a day being
    # reported to callers as "Украина is not in the knowledge base".
    error: str = ""


# Bounded N-hop traversal. ``$hops`` is interpolated into the
# variable-length pattern (Neo4j can't parametrise the upper bound), so
# it MUST be a clamped int — never user text. Everything else is a
# proper param. ``size($rel_filter)=0`` means "no relation filter".
_WALK_CYPHER = """
MATCH (e:__Entity__ {{name: $name}})
CALL {{
    WITH e
    MATCH path = (e)-[r*1..{hops}]-(m:__Entity__)
    WHERE all(rel IN r WHERE size($rel_filter) = 0
              OR type(rel) IN $rel_filter)
    RETURN m, r
    LIMIT $node_cap
}}
WITH collect(DISTINCT m) AS ms,
     [rel IN apoc.coll.flatten(collect(r)) | rel][0..$edge_cap] AS rels,
     e
RETURN
    [n IN ([e] + ms) | {{
        name: n.name,
        label: head([l IN labels(n) WHERE l <> '__Entity__'
                     AND l <> '__Node__']),
        description: coalesce(n.description, '')
    }}][0..$node_cap] AS entities,
    [rel IN rels | {{
        src: startNode(rel).name,
        tgt: endNode(rel).name,
        label: type(rel),
        polarity: rel.polarity,
        valid_from: rel.valid_from,
        valid_to: rel.valid_to
    }}] AS relations
"""

# Fallback without APOC (apoc.coll.flatten) — relation list is built
# per-path and flattened in Python instead.
_WALK_CYPHER_NO_APOC = """
MATCH (e:__Entity__ {{name: $name}})
MATCH path = (e)-[r*1..{hops}]-(m:__Entity__)
WHERE all(rel IN r WHERE size($rel_filter) = 0
          OR type(rel) IN $rel_filter)
WITH e, m, r
LIMIT $node_cap
RETURN
    e.name AS start_name,
    [l IN labels(e) WHERE l <> '__Entity__' AND l <> '__Node__'] AS start_labels,
    coalesce(e.description, '') AS start_description,
    m.name AS m_name,
    [l IN labels(m) WHERE l <> '__Entity__' AND l <> '__Node__'] AS m_labels,
    coalesce(m.description, '') AS m_description,
    [rel IN r | {{src: startNode(rel).name, tgt: endNode(rel).name,
                  label: type(rel), polarity: rel.polarity,
                  valid_from: rel.valid_from, valid_to: rel.valid_to}}] AS rels
"""


_FIND_BY_NAME_CYPHER = """
CALL db.index.fulltext.queryNodes('entity_name_fulltext', $lucene)
YIELD node, score
WHERE node:`__Entity__`
RETURN node.name AS name,
       [l IN labels(node) WHERE l <> '__Entity__' AND l <> '__Node__'] AS labels,
       coalesce(node.description, '') AS description
ORDER BY score DESC
LIMIT $limit
"""


def _find_by_name_ngql(query: str, *, limit: int) -> str:
    """PREFIX name lookup under nebula, over the ``entity_name_idx`` index.

    Whitespace-split the input and OR a ``STARTS WITH`` per token. Exact
    and prefix both work: "Украина" finds itself, "Иванов" finds "Иванов
    Иван Иванович".

    KNOWN LIMIT: a token matching mid-name does NOT match. "Ромаш" will
    not find "ООО Ромашка", which the previous ``CONTAINS`` version
    promised. That promise was not kept in practice — ``CONTAINS`` cannot
    use an index, so the query was a full scan of every Entity vertex,
    and on a 161k-entity graph it failed outright with
    ``GraphMemoryExceeded (-2600)``. Measured 2026-08-18: this form
    answers in under two seconds where ``CONTAINS`` failed in three.
    Prefix matching that works beats substring matching that does not.

    True substring search needs an index built for it — a trigram table
    of entity names in Postgres, the way the statistics registry is
    searched. That is a separate piece of work; do not reintroduce
    ``CONTAINS`` here instead.

    Returns ``""`` for blank input so the caller short-circuits.
    Case-sensitive, like the index.
    """
    tokens = [t for t in (query or "").split() if t.strip()]
    if not tokens:
        return ""
    clauses = " OR ".join(
        f"`Entity`.name STARTS WITH {_nebula_q(t)}" for t in tokens
    )
    return (
        f"LOOKUP ON `Entity` WHERE {clauses} "
        "YIELD id(vertex) AS vid, `Entity`.name AS name, "
        "`Entity`.label AS label, `Entity`.description AS description "
        f"| LIMIT {int(limit)};"
    )


# Lucene special chars that must be backslash-escaped inside a query term.
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}\[\]^"~*?:\\/&|])')


def build_fulltext_query(query: str) -> str:
    """Build a Lucene OR-of-tokens query for the ``entity_name_fulltext``
    index: whitespace-split the input, escape Lucene special chars in each
    token, join with ``OR``.  Returns ``""`` for blank input so the caller
    short-circuits to empty results (never issues a bare/invalid query)."""
    tokens: list[str] = []
    for raw in (query or "").split():
        esc = _LUCENE_SPECIAL.sub(r"\\\1", raw).strip()
        if esc:
            tokens.append(esc)
    return " OR ".join(tokens)


# LlamaIndex's PG retriever prepends this header when include_text=True;
# the triplet lines sit between it and the next blank line.
_FACTS_HEADER = "Here are some facts extracted from the provided text:"


def _parse_triplet_chains(text: str) -> list[tuple[str, str, str]]:
    """Extract ``(src, label, tgt)`` triplets from the PG retriever's
    text-serialised facts (``A -> REL -> B [-> REL2 -> C …]`` lines).

    With ``include_text=True`` only the facts section (header → blank
    line) is scanned so arrows inside the source chunk body can't be
    misparsed; without the header the whole text is treated as facts."""
    if _FACTS_HEADER in text:
        after = text.split(_FACTS_HEADER, 1)[1].lstrip("\n")
        text = after.split("\n\n", 1)[0]
    triplets: list[tuple[str, str, str]] = []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(" -> ")]
        # A valid chain alternates entity/relation: odd length >= 3.
        if len(parts) < 3 or len(parts) % 2 == 0 or not all(parts):
            continue
        for i in range(0, len(parts) - 2, 2):
            triplets.append((parts[i], parts[i + 1], parts[i + 2]))
    return triplets


class GraphRetriever:
    """Async wrapper over ``PropertyGraphIndex.as_retriever``."""

    def __init__(
        self,
        pg_index: PropertyGraphIndex,
        *,
        similarity_top_k: int = 10,
        path_depth: int = 1,
        include_text: bool = True,
        filter_polarity_temporal: bool = True,
    ) -> None:
        self._pg_index = pg_index
        self._similarity_top_k = similarity_top_k
        self._include_text = include_text
        self._default_path_depth = path_depth
        # #8: drop negated / expired relations from awalk results. Resolved
        # at construction (activity-runtime code) so it's snapshot-stable
        # for the life of the retriever; opt-out via AgentSettings.
        self._filter_polarity_temporal = filter_polarity_temporal
        self._retriever = pg_index.as_retriever(
            similarity_top_k=similarity_top_k,
            path_depth=path_depth,
            include_text=include_text,
        )
        # Lazily-built retrievers keyed by clamped path_depth so a
        # per-call depth doesn't rebuild on every request. The default
        # is pre-seeded. Construction is cheap (no LLM/vector I/O — that
        # happens in ``aretrieve``).
        self._retrievers: dict[int, object] = {path_depth: self._retriever}
        # Underlying store handle for the bounded N-hop ``awalk`` (R3).
        # ``structured_query`` is the generic Cypher entry on
        # Neo4jPropertyGraphStore (same one ER uses). None for stores
        # that don't expose it — ``awalk`` degrades to empty.
        self._graph_store = getattr(
            pg_index, "property_graph_store", None,
        )

    @classmethod
    def for_store(
        cls,
        store,
        *,
        similarity_top_k: int = 10,
        filter_polarity_temporal: bool = True,
    ) -> GraphRetriever:
        """Build a retriever backed only by a KbGraphStore (structured_query),
        without a LlamaIndex PropertyGraphIndex. Used for the nebula backend:
        awalk/afind_entities_by_name work over nGQL; the vector/synonym
        `aretrieve` path is unavailable (Phase 3) and returns empty."""
        r = cls.__new__(cls)
        r._pg_index = None
        r._retriever = None
        r._retrievers = {}
        r._similarity_top_k = similarity_top_k
        r._include_text = True
        r._default_path_depth = 1
        r._filter_polarity_temporal = filter_polarity_temporal
        r._graph_store = store
        return r

    def _retriever_for(self, path_depth: int):
        """Return a retriever configured for ``path_depth`` (clamped to
        ``[1, GRAPH_PATH_DEPTH_MAX]``), building + caching on first use."""
        pd = max(1, min(int(path_depth), GRAPH_PATH_DEPTH_MAX))
        r = self._retrievers.get(pd)
        if r is None:
            r = self._pg_index.as_retriever(
                similarity_top_k=self._similarity_top_k,
                path_depth=pd,
                include_text=self._include_text,
            )
            self._retrievers[pd] = r
        return r

    async def aretrieve(
        self, query: str, *, path_depth: int | None = None,
    ) -> RoundGraphData:
        """Similarity retrieval over the KG. ``path_depth`` overrides how
        many triplet-hops of neighbours are pulled around each matched
        entity (None ⇒ the retriever's default, clamped ≤
        ``GRAPH_PATH_DEPTH_MAX``)."""
        if settings.graph.backend == "nebula":
            return await self._aretrieve_nebula(query, path_depth)
        if self._retriever is None:
            return RoundGraphData()
        retriever = (
            self._retriever if path_depth is None
            else self._retriever_for(path_depth)
        )
        nodes = await retriever.aretrieve(query)
        out = RoundGraphData()
        seen_rels: set[tuple[str, str, str]] = set()
        seen_ents: set[str] = set()
        for n in nodes:
            text = n.node.get_content() or ""
            md = n.node.metadata or {}
            # PG retriever interleaves three node kinds; classify by
            # node-class name so we don't depend on private fields.
            cls = type(n.node).__name__
            if cls in {"EntityNode", "ChunkNode"} and md.get("triplet_source_id"):
                # text-as-triplet snippet
                out.relations.append({
                    "src_id": md.get("subj") or md.get("src") or "",
                    "tgt_id": md.get("obj") or md.get("tgt") or "",
                    "label": md.get("rel_type") or md.get("label") or "",
                    "description": text,
                })
            elif cls == "EntityNode":
                out.entities.append({
                    "entity_name": md.get("name") or text,
                    "entity_type": md.get("label") or md.get("type") or "",
                    "description": text,
                })
            else:
                # In practice the PG retriever returns plain TextNodes whose
                # CONTENT serialises the matched triplets — there is no
                # EntityNode/ChunkNode to classify (that mismatch left
                # entities/relations always empty; found 2026-07-03).
                # Recover them from the text, keep the node as a chunk.
                for src, label, tgt in _parse_triplet_chains(text):
                    if (src, label, tgt) not in seen_rels:
                        seen_rels.add((src, label, tgt))
                        out.relations.append({
                            "src_id": src,
                            "tgt_id": tgt,
                            "label": label,
                            "description": "",
                        })
                    for name in (src, tgt):
                        if name not in seen_ents:
                            seen_ents.add(name)
                            out.entities.append({
                                "entity_name": name,
                                "entity_type": "",
                                "description": "",
                            })
                # plain content chunk attached for context
                out.chunks.append(n)
        return out

    async def awalk(
        self,
        start_entity: str,
        *,
        hops: int = 2,
        rel_filter: list[str] | None = None,
    ) -> RoundGraphData:
        """Bounded N-hop traversal from ``start_entity``.

        Distinct from ``aretrieve`` (which is similarity-based,
        path_depth=1 and UNCHANGED). This issues one bounded Cypher
        query against the underlying store:

          * ``hops`` is clamped to ``GRAPH_WALK_MAX_HOPS`` and
            interpolated into the variable-length pattern (Neo4j can't
            parametrise the bound — so it must be a clamped int).
          * ``rel_filter`` restricts to the given relationship types at
            the query level (empty ⇒ all types).
          * server-side ``LIMIT $node_cap`` plus Python-side truncation
            guarantee at most ``GRAPH_WALK_NODE_CAP`` nodes /
            ``GRAPH_WALK_EDGE_CAP`` edges.

        Returns the same ``RoundGraphData`` shape as ``aretrieve`` so
        callers serialise identically. Empty on any failure / missing
        store (best-effort — never raises through the tool boundary).
        """
        if self._graph_store is None:
            return RoundGraphData()

        if settings.graph.backend == "nebula":
            safe_hops = max(1, min(int(hops), GRAPH_WALK_MAX_HOPS))
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.subgraph, entity_vid(start_entity), safe_hops,
                )
            except Exception as exc:
                logger.warning("graph_walk (nebula) failed: {e}", e=repr(exc))
                return RoundGraphData()
            out = self._map_walk_rows(rows)
            if rel_filter:
                allow = set(rel_filter)
                out.relations = [r for r in out.relations if r.get("label") in allow]
            return out

        safe_hops = max(1, min(int(hops), GRAPH_WALK_MAX_HOPS))
        params = {
            "name": start_entity,
            "hops": safe_hops,
            "rel_filter": list(rel_filter) if rel_filter else [],
            "node_cap": GRAPH_WALK_NODE_CAP,
            "edge_cap": GRAPH_WALK_EDGE_CAP,
        }
        try:
            rows = await asyncio.to_thread(
                self._graph_store.structured_query,
                _WALK_CYPHER.format(hops=safe_hops),
                params,
            )
        except Exception as exc:
            logger.warning(
                "graph_walk: APOC path failed, retrying without APOC: {e}",
                e=repr(exc),
            )
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.structured_query,
                    _WALK_CYPHER_NO_APOC.format(hops=safe_hops),
                    params,
                )
                return self._map_no_apoc_rows(rows)
            except Exception as exc2:
                logger.warning("graph_walk failed: {e}", e=repr(exc2))
                return RoundGraphData()

        return self._map_walk_rows(rows)

    async def _aretrieve_nebula(
        self, query: str, path_depth: int | None,
    ) -> RoundGraphData:
        """graph_search under nebula: embed the query, kNN over ``er_vec``
        (Milvus entity vectors), then subgraph-expand each matched entity via
        ``awalk``. There is no LlamaIndex PropertyGraphIndex retriever here.
        Fail-soft — any error yields empty."""
        if self._graph_store is None or not (query or "").strip():
            return RoundGraphData()
        try:
            from src.graph.entity_vector_store import build_entity_vector_store
            from src.ingestion.embeddings import build_embedding_model

            vec = await build_embedding_model().aget_text_embedding(query)
            evs = build_entity_vector_store(self._graph_store)
            cands = await asyncio.to_thread(evs.knn, vec, self._similarity_top_k)
            # EntityCandidate is a TypedDict (a dict), not an attr object.
            names = [c.get("name") for c in cands if isinstance(c, dict) and c.get("name")]
        except Exception as exc:  # embed / vector-store / kNN failure
            logger.warning("aretrieve (nebula) entity kNN failed: {e}", e=repr(exc))
            return RoundGraphData()

        hops = path_depth if path_depth is not None else 1
        out = RoundGraphData()
        for name in names:
            sub = await self.awalk(name, hops=hops)
            out.entities.extend(sub.entities)
            out.relations.extend(sub.relations)
        out.entities = _dedupe_entities(out.entities)
        return out

    async def afind_entities_by_name(
        self, query: str, *, limit: int | None = None,
    ) -> RoundGraphData:
        """Full-text lookup of entities by (partial) name.

        Complements ``aretrieve`` (exact-synonym + vector): catches
        "Иванов" → "Иванов Иван Иванович" on large graphs via the
        ``entity_name_fulltext`` index.  Best-effort — empty on a missing
        store / missing index / any error / blank query (never raises)."""
        if self._graph_store is None:
            return RoundGraphData()
        if settings.graph.backend == "nebula":
            cap = limit if limit is not None else self._similarity_top_k
            ngql = _find_by_name_ngql(query, limit=int(cap))
            if not ngql:  # blank query -> nothing to look up
                return RoundGraphData()
            try:
                rows = await asyncio.to_thread(
                    self._graph_store.structured_query, ngql,
                )
            except Exception as exc:
                logger.warning("find_entities_by_name (nebula) failed: {e}", e=repr(exc))
                # Report it. Returning a bare empty result told the caller
                # "no such entity" when the truth was "the lookup could
                # not run".
                return RoundGraphData(error=str(exc)[:200])
            out = RoundGraphData()
            for row in (rows or [])[: int(cap)]:
                # LOOKUP yields flat columns; accept the older `p` map too
                # so a caller or fake feeding either shape still works.
                p = (row or {}).get("p") or row or {}
                name = p.get("name")
                if not name:
                    continue
                out.entities.append({
                    "entity_name": name,
                    "entity_type": p.get("label") or "",
                    "description": p.get("description") or "",
                })
            out.entities = _dedupe_entities(out.entities)
            return out
        lucene = build_fulltext_query(query)
        if not lucene:
            return RoundGraphData()
        cap = limit if limit is not None else self._similarity_top_k
        try:
            rows = await asyncio.to_thread(
                self._graph_store.structured_query,
                _FIND_BY_NAME_CYPHER,
                {"lucene": lucene, "limit": int(cap)},
            )
        except Exception as exc:  # broad by design — index/store missing, fail-open
            logger.warning("find_entities_by_name failed: {e}", e=repr(exc))
            return RoundGraphData()
        out = RoundGraphData()
        for row in rows or []:
            name = (row or {}).get("name")
            if not name:
                continue
            labels = list(row.get("labels") or [])
            out.entities.append({
                "entity_name": name,
                "entity_type": labels[0] if labels else "",
                "description": row.get("description") or "",
            })
        out.entities = _dedupe_entities(out.entities)
        return out

    def _map_rel(self, rel: dict) -> dict | None:
        """Map one walk-relation dict → mapped row, or ``None`` if it is
        filtered out (negated / expired) and filtering is enabled.

        Exposes ``polarity`` / ``valid_from`` / ``valid_to`` on the kept
        rows so downstream can still reason over a hedged ('uncertain')
        edge even though negated/expired ones never reach it.
        """
        if self._filter_polarity_temporal and not _relation_is_live(
            rel, now_iso=date.today().isoformat(),
        ):
            return None
        return {
            "src_id": rel.get("src") or "",
            "tgt_id": rel.get("tgt") or "",
            "label": rel.get("label") or "",
            "polarity": rel.get("polarity"),
            "valid_from": rel.get("valid_from"),
            "valid_to": rel.get("valid_to"),
        }

    def _map_walk_rows(self, rows: list[dict] | None) -> RoundGraphData:
        """Map the APOC-path Cypher result into ``RoundGraphData``.

        Defensive caps re-applied here in case the store ignored the
        LIMIT (e.g. a non-Neo4j store).
        """
        out = RoundGraphData()
        for row in rows or []:
            for ent in (row.get("entities") or []):
                name = ent.get("name")
                if not name:
                    continue
                out.entities.append({
                    "entity_name": name,
                    "entity_type": ent.get("label") or "",
                    "description": ent.get("description") or "",
                })
            for rel in (row.get("relations") or []):
                mapped = self._map_rel(rel)
                if mapped is not None:
                    out.relations.append(mapped)
        out.entities = _dedupe_entities(out.entities)[:GRAPH_WALK_NODE_CAP]
        out.relations = _dedupe_relations(out.relations)[:GRAPH_WALK_EDGE_CAP]
        return out

    def _map_no_apoc_rows(self, rows: list[dict] | None) -> RoundGraphData:
        """Map the per-path (no-APOC) Cypher result into RoundGraphData."""
        out = RoundGraphData()
        for row in rows or []:
            start = row.get("start_name")
            if start:
                labels = list(row.get("start_labels") or [])
                out.entities.append({
                    "entity_name": start,
                    "entity_type": labels[0] if labels else "",
                    "description": row.get("start_description") or "",
                })
            m_name = row.get("m_name")
            if m_name:
                m_labels = list(row.get("m_labels") or [])
                out.entities.append({
                    "entity_name": m_name,
                    "entity_type": m_labels[0] if m_labels else "",
                    "description": row.get("m_description") or "",
                })
            for rel in (row.get("rels") or []):
                mapped = self._map_rel(rel)
                if mapped is not None:
                    out.relations.append(mapped)
        out.entities = _dedupe_entities(out.entities)[:GRAPH_WALK_NODE_CAP]
        out.relations = _dedupe_relations(out.relations)[:GRAPH_WALK_EDGE_CAP]
        return out


def _dedupe_entities(entities: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for e in entities:
        key = e["entity_name"]
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _dedupe_relations(relations: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    out: list[dict] = []
    for r in relations:
        key = (r["src_id"], r["tgt_id"], r["label"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

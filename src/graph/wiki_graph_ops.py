"""Backend-dispatched wiki-editor graph ops (dirty-flag bookkeeping +
article-context reads/writes).

``Neo4jWikiGraphOps`` wraps the existing Cypher constants verbatim
(default path, byte-for-byte unchanged; the constants and query calls
were MOVED here from ``wiki_dirty.py``'s ``mark_dirty``/``select_dirty``/
``clear_dirty``, ``wiki_context.py``'s ``read_entity_subgraph``/
``read_citations``/``read_source_docs``, ``wiki_sweep.py``'s inline
hash-check/title-write, and ``admin.py``'s mark-all-dirty). Those call
sites still hold their own (transitionally duplicated) copies of the
constants — Task 4 rewires them through this seam.

``NebulaWikiGraphOps`` is a stub for Task 3.
"""
from __future__ import annotations

from typing import Any, Protocol

from src.config import settings

# ── dirty-flag bookkeeping Cypher (moved verbatim from wiki_dirty.py) ──

_MARK = """
UNWIND $names AS n
MATCH (e:__Entity__ {name: n})
SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()
"""

_SELECT = """
MATCH (e:__Entity__) WHERE e.wiki_dirty = true
RETURN e.name AS name ORDER BY e.wiki_dirty_at LIMIT $limit
"""

_CLEAR = """
MATCH (e:__Entity__ {name: $name})
SET e.wiki_dirty = false, e.wiki_hash = $hash, e.wiki_synced_at = datetime()
"""

# ── article-context Cypher (moved verbatim from wiki_context.py) ──────

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

_SOURCE_DOCS_CYPHER = """
MATCH (c:Chunk)-[:MENTIONS]->(e:__Entity__ {name: $name})
RETURN DISTINCT c.doc_id AS doc_id ORDER BY doc_id
"""

# ── sweep inline Cypher (moved verbatim from wiki_sweep.py) ───────────

_READ_HASH_CYPHER = (
    "MATCH (e:__Entity__ {name:$n}) RETURN coalesce(e.wiki_hash,'') AS h"
)

_WRITE_TITLE_CYPHER = "MATCH (e:__Entity__ {name:$n}) SET e.wiki_page_title=$t"

# ── admin mark-all Cypher (moved verbatim from admin.py) ──────────────

_MARK_ALL_CYPHER = (
    "MATCH (e:__Entity__) SET e.wiki_dirty = true, e.wiki_dirty_at = datetime()"
)


class WikiGraphOps(Protocol):
    def mark_dirty(self, names: list[str]) -> None: ...

    def select_dirty(self, limit: int) -> list[str]: ...

    def clear_dirty(self, name: str, digest: str) -> None: ...

    def mark_all_dirty(self) -> None: ...

    def read_subgraph(self, name: str, max_relations: int) -> list[dict]: ...

    def read_citations(self, name: str, k: int) -> list[dict]: ...

    def read_source_docs(self, name: str) -> list[str]: ...

    def read_wiki_hash(self, name: str) -> str: ...

    def write_page_title(self, name: str, title: str) -> None: ...


class Neo4jWikiGraphOps:
    """Runs the historical wiki-editor Cypher verbatim — zero behaviour
    change from the pre-seam ``wiki_dirty.py``/``wiki_context.py``/
    ``wiki_sweep.py``/``admin.py`` implementations."""

    def __init__(self, store: Any):
        self._store = store

    def mark_dirty(self, names: list[str]) -> None:
        if not names:
            return
        self._store.structured_query(_MARK, param_map={"names": list(names)})

    def select_dirty(self, limit: int) -> list[str]:
        rows = self._store.structured_query(_SELECT, param_map={"limit": limit})
        return [row["name"] for row in rows]

    def clear_dirty(self, name: str, digest: str) -> None:
        self._store.structured_query(
            _CLEAR, param_map={"name": name, "hash": digest})

    def mark_all_dirty(self) -> None:
        self._store.structured_query(_MARK_ALL_CYPHER)

    def read_subgraph(self, name: str, max_relations: int) -> list[dict]:
        rows = self._store.structured_query(
            _SUBGRAPH_CYPHER, param_map={"name": name, "max_rel": max_relations})
        return list(rows or [])

    def read_citations(self, name: str, k: int) -> list[dict]:
        rows = self._store.structured_query(
            _CITATIONS_CYPHER, param_map={"name": name, "k": k})
        return list(rows or [])

    def read_source_docs(self, name: str) -> list[str]:
        rows = self._store.structured_query(
            _SOURCE_DOCS_CYPHER, param_map={"name": name})
        return [row["doc_id"] for row in rows if row.get("doc_id")]

    def read_wiki_hash(self, name: str) -> str:
        rows = self._store.structured_query(
            _READ_HASH_CYPHER, param_map={"n": name})
        return rows[0]["h"] if rows else ""

    def write_page_title(self, name: str, title: str) -> None:
        self._store.structured_query(
            _WRITE_TITLE_CYPHER, param_map={"n": name, "t": title})


class NebulaWikiGraphOps:
    """nGQL wiki-editor graph ops — STUB (Task 3)."""

    def __init__(self, store: Any):
        self._store = store

    def mark_dirty(self, names: list[str]) -> None:
        raise NotImplementedError("NebulaWikiGraphOps.mark_dirty (Task 3)")

    def select_dirty(self, limit: int) -> list[str]:
        raise NotImplementedError("NebulaWikiGraphOps.select_dirty (Task 3)")

    def clear_dirty(self, name: str, digest: str) -> None:
        raise NotImplementedError("NebulaWikiGraphOps.clear_dirty (Task 3)")

    def mark_all_dirty(self) -> None:
        raise NotImplementedError("NebulaWikiGraphOps.mark_all_dirty (Task 3)")

    def read_subgraph(self, name: str, max_relations: int) -> list[dict]:
        raise NotImplementedError("NebulaWikiGraphOps.read_subgraph (Task 3)")

    def read_citations(self, name: str, k: int) -> list[dict]:
        raise NotImplementedError("NebulaWikiGraphOps.read_citations (Task 3)")

    def read_source_docs(self, name: str) -> list[str]:
        raise NotImplementedError("NebulaWikiGraphOps.read_source_docs (Task 3)")

    def read_wiki_hash(self, name: str) -> str:
        raise NotImplementedError("NebulaWikiGraphOps.read_wiki_hash (Task 3)")

    def write_page_title(self, name: str, title: str) -> None:
        raise NotImplementedError("NebulaWikiGraphOps.write_page_title (Task 3)")


def build_wiki_graph_ops(store: Any) -> WikiGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaWikiGraphOps(store)
    return Neo4jWikiGraphOps(store)

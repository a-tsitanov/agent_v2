"""Backend-dispatched community SUMMARIZE I/O (context reads + report write).

`Neo4jCommunitySummarize` wraps the existing Cypher constants verbatim
(default path, byte-for-byte). `NebulaCommunitySummarize` translates the same
ops to nGQL. `report_vec` is NOT written to the nebula vertex (Milvus owns it,
via the already-dispatched community report-vector store).
"""
from __future__ import annotations

from typing import Any, Protocol

from src.config import settings

# Read the members' names + descriptions for the summary prompt (and any
# inter-member relations to give the LLM relational context).  Members are
# resolved from Neo4j by ``community_id`` via the ``IN_COMMUNITY`` links
# that ``detect`` already persisted (level 0) — so the summarize params
# carry only a count, never the full name list (Temporal payload stays
# tiny).  ``o`` is constrained to the SAME community so relation context
# matches the legacy ``o.name IN $members`` semantics.
_MEMBER_CONTEXT_CYPHER = """
MATCH (c:Community {id: $community_id, level: $level})<-[:IN_COMMUNITY]-(e:__Entity__)
OPTIONAL MATCH (e)-[r]-(o:__Entity__)-[:IN_COMMUNITY]->(c)
RETURN e.name AS name,
       coalesce(e.description, '') AS description,
       collect(DISTINCT type(r))[..10] AS rel_types
ORDER BY name
"""

# Child-report context for level>0 communities — a parent report is
# composed from its children's reports (cheaper than re-reading every
# leaf member).  Direction is intentional: PARENT_OF runs coarser→finer
# (``(parent {level:k})-[:PARENT_OF]->(child {level:k+1})``), so a level-k
# community here reads its finer level-(k+1) constituents — i.e. a coarse
# report is built bottom-up from its finer children.  Only children that
# ALREADY have a report participate, so the summarise fan-out MUST run
# finest-level-first for parents to see them — that level ordering is wired
# in Phase 3 (CommunityBuildWorkflow); until then the level>0 path is
# latent (the build workflow only detects the coarsest level).
_CHILD_REPORTS_CYPHER = """
MATCH (c:Community {id: $community_id, level: $level})-[:PARENT_OF]->(child:Community)
WHERE child.report IS NOT NULL
RETURN child.title AS title, child.summary AS summary
ORDER BY child.member_count DESC
"""

# Idempotent: re-running updates the report on the SAME :Community node
# (keyed on id+level) rather than creating a new one.  ``summary`` is kept
# as a plain column (embedding source + lexical-fallback text); ``report``
# is the JSON-serialised structured report; ``report_vec`` is the native
# embedding (may be unset on embed failure — fail-open).
_WRITE_REPORT_CYPHER = """
MERGE (c:Community {id: $community_id, level: $level})
SET c.report = $report, c.title = $title, c.summary = $summary,
    c.report_vec = $report_vec, c.summarized_at = timestamp()
"""


class CommunitySummarize(Protocol):
    def read_member_context(self, *, community_id: str, level: int) -> list[dict]: ...
    def read_child_reports(self, *, community_id: str, level: int) -> list[dict]: ...
    def write_report(self, *, community_id: str, level: int, report: str,
                     title: str, summary: str, report_vec: list[float] | None) -> None: ...


class Neo4jCommunitySummarize:
    """Runs the historical Cypher constants verbatim — zero behaviour change."""

    def __init__(self, store: Any):
        self._store = store

    def _run(self, cypher: str, params: dict) -> list[dict]:
        return list(self._store.structured_query(cypher, param_map=params) or [])

    def read_member_context(self, *, community_id, level) -> list[dict]:
        return self._run(_MEMBER_CONTEXT_CYPHER, {"community_id": community_id, "level": level})

    def read_child_reports(self, *, community_id, level) -> list[dict]:
        return self._run(_CHILD_REPORTS_CYPHER, {"community_id": community_id, "level": level})

    def write_report(self, *, community_id, level, report, title, summary, report_vec) -> None:
        self._run(_WRITE_REPORT_CYPHER, {
            "community_id": community_id, "level": level, "report": report,
            "title": title, "summary": summary, "report_vec": report_vec,
        })


class NebulaCommunitySummarize:
    """nGQL community SUMMARIZE. UPDATE VERTEX is a partial update (preserves
    BUILD's member_count/members_hash); report_vec is never written (Milvus).
    The intra-community edge filter for member context is done in Python."""

    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def read_child_reports(self, *, community_id, level) -> list[dict]:
        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        child_rows = self._exec(f'GO FROM {_q(cvid)} OVER `PARENT_OF` YIELD dst(edge) AS child;')
        cvids = [r["child"] for r in child_rows if r.get("child")]
        if not cvids:
            return []
        listed = ", ".join(_q(v) for v in cvids)
        props = self._exec(
            f"FETCH PROP ON `Community` {listed} YIELD "
            "`Community`.title AS title, `Community`.summary AS summary, "
            "`Community`.report AS report, `Community`.member_count AS mc;"
        )
        kept = [r for r in props if (r.get("report") or "").strip()]
        kept.sort(key=lambda r: r.get("mc") or 0, reverse=True)
        return [{"title": r.get("title") or "", "summary": r.get("summary") or ""} for r in kept]

    def read_member_context(self, *, community_id, level) -> list[dict]:
        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        mrows = self._exec(f'GO FROM {_q(cvid)} OVER `IN_COMMUNITY` REVERSELY YIELD src(edge) AS m;')
        members = [r["m"] for r in mrows if r.get("m")]
        if not members:
            return []
        mset = set(members)
        listed = ", ".join(_q(v) for v in members)
        # YIELD id(vertex) so props can be keyed back to each member's VID.
        prop_rows = self._exec(
            f"FETCH PROP ON `Entity` {listed} YIELD id(vertex) AS vid, "
            "`Entity`.name AS name, `Entity`.description AS description;"
        )
        # Key by the row's own vid when present (real nGQL rows always carry
        # it); fall back to positional pairing with `members` for fakes/tests
        # whose canned rows omit "vid" but preserve request order.
        props = {
            r.get("vid", v): (r.get("name") or "", r.get("description") or "")
            for v, r in zip(members, prop_rows, strict=False)
        }
        edges = self._exec(
            f"GO FROM {listed} OVER `RELATED` BIDIRECT YIELD "
            "src(edge) AS s, dst(edge) AS d, `RELATED`.rel_type AS rt;"
        )
        rel: dict[str, list[str]] = {v: [] for v in members}
        for e in edges:
            s_, d_, rt = e.get("s"), e.get("d"), e.get("rt")
            if s_ in mset and d_ in mset and rt:      # intra-community only
                for endpoint in (s_, d_):
                    if rt not in rel[endpoint]:
                        rel[endpoint].append(rt)
        out = []
        for v in members:
            name, desc = props.get(v, ("", ""))
            out.append({"name": name, "description": desc, "rel_types": rel[v][:10]})
        out.sort(key=lambda r: r["name"])
        return out

    def write_report(self, *, community_id, level, report, title, summary, report_vec) -> None:
        import time

        from src.graph.community_writeback import community_vid
        from src.graph.nebula_store import _q
        cvid = community_vid(community_id, level)
        now = int(time.time() * 1000)
        # report_vec intentionally NOT written (Milvus owns it). UPDATE VERTEX
        # is a partial update, preserving member_count/members_hash from BUILD.
        self._exec(
            f'UPDATE VERTEX ON `Community` {_q(cvid)} SET '
            f"report = {_q(report)}, title = {_q(title)}, "
            f"summary = {_q(summary)}, summarized_at = {now};"
        )


def build_community_summarize(store: Any) -> CommunitySummarize:
    if settings.graph.backend == "nebula":
        return NebulaCommunitySummarize(store)
    return Neo4jCommunitySummarize(store)

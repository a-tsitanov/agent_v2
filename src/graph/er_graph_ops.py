"""Backend-dispatched entity-resolution GRAPH ops (verdict cache +
edge-redirect merge).

``Neo4jERGraphOps`` wraps the existing Cypher/APOC constants verbatim
(default path, byte-for-byte unchanged; the constants and query calls
were MOVED here from ``entity_resolution.py``'s ``_load_verdict_cache``,
``_store_verdicts`` and ``_cleanup_stored_losers``). ``NebulaERGraphOps``
is a stub for Task 3.
"""
from __future__ import annotations

import hashlib
from typing import Any, Protocol

from src.config import settings

# ── verdict cache Cypher (moved verbatim from entity_resolution.py) ──

_LOAD_VERDICTS_CYPHER = (
    "MATCH (v:ERVerdict) WHERE v.key IN $keys "
    "RETURN v.key AS key, v.same AS same"
)

_ENSURE_VERDICT_CONSTRAINT_CYPHER = (
    "CREATE CONSTRAINT er_verdict_key IF NOT EXISTS "
    "FOR (v:ERVerdict) REQUIRE v.key IS UNIQUE"
)

_STORE_VERDICTS_CYPHER = (
    "UNWIND $rows AS row MERGE (v:ERVerdict {key: row.key}) "
    "SET v.same = row.same, v.updated = datetime()"
)

# ── loser->canonical edge-redirect merge (moved verbatim from
# entity_resolution.py's _cleanup_stored_losers) ─────────────────────

_MERGE_LOSER_CYPHER = """
                MATCH (loser:__Entity__ {name: $loser})
                MATCH (canon:__Entity__ {name: $canon})
                WHERE elementId(loser) <> elementId(canon)
                // Copy outgoing edges loser→X to canon→X
                CALL {
                    WITH loser, canon
                    MATCH (loser)-[r]->(t)
                    WHERE elementId(t) <> elementId(canon)
                    WITH canon, t, type(r) AS rt, properties(r) AS rp
                    CALL apoc.merge.relationship(canon, rt, {}, rp, t, {})
                        YIELD rel
                    RETURN count(*) AS _o
                }
                // Copy incoming edges X→loser to X→canon
                CALL {
                    WITH loser, canon
                    MATCH (s)-[r]->(loser)
                    WHERE elementId(s) <> elementId(canon)
                    WITH canon, s, type(r) AS rt, properties(r) AS rp
                    CALL apoc.merge.relationship(s, rt, {}, rp, canon, {})
                        YIELD rel
                    RETURN count(*) AS _i
                }
                DETACH DELETE loser
                """


def verdict_vid(key: str) -> str:
    """Deterministic Nebula VID for an ``ERVerdict`` vertex.

    Mirrors ``nebula_store.entity_vid``: 128-bit blake2b digest of the
    verdict cache key, hex-encoded. Used by ``NebulaERGraphOps`` (Task 3)
    to address ``ERVerdict`` vertices by VID instead of a Cypher `key`
    property lookup.
    """
    return hashlib.blake2b((key or "").encode("utf-8"), digest_size=16).hexdigest()


class ERGraphOps(Protocol):
    def ensure_verdict_schema(self) -> None: ...

    def load_verdicts(self, keys: list[str]) -> dict[str, bool]: ...

    def store_verdicts(self, entries: dict[str, bool]) -> None: ...

    def merge_loser_into_canonical(self, *, loser: str, canon: str) -> None: ...


class Neo4jERGraphOps:
    """Runs the historical ER graph Cypher/APOC verbatim — zero behaviour
    change from the pre-seam ``entity_resolution.py`` implementation."""

    def __init__(self, store: Any):
        self._store = store

    def ensure_verdict_schema(self) -> None:
        self._store.structured_query(_ENSURE_VERDICT_CONSTRAINT_CYPHER)

    def load_verdicts(self, keys: list[str]) -> dict[str, bool]:
        rows = self._store.structured_query(
            _LOAD_VERDICTS_CYPHER,
            param_map={"keys": list(keys)},
        )
        return {r["key"]: bool(r["same"]) for r in (rows or []) if isinstance(r, dict)}

    def store_verdicts(self, entries: dict[str, bool]) -> None:
        # Idempotent: backs the MERGE and prevents duplicate :ERVerdict
        # nodes under concurrent writes; also indexes the IN-list load.
        # Replicates current behaviour: the constraint is (re-)issued on
        # every call, same as the pre-seam ``_store_verdicts``.
        self._store.structured_query(_ENSURE_VERDICT_CONSTRAINT_CYPHER)
        self._store.structured_query(
            _STORE_VERDICTS_CYPHER,
            param_map={"rows": [{"key": k, "same": s} for k, s in entries.items()]},
        )

    def merge_loser_into_canonical(self, *, loser: str, canon: str) -> None:
        self._store.structured_query(
            _MERGE_LOSER_CYPHER,
            param_map={"loser": loser, "canon": canon},
        )


class NebulaERGraphOps:
    """nGQL ER graph ops: verdict cache (VID-addressed ``ERVerdict``
    vertices) + the loser->canonical edge-redirect merge.

    ``ensure_verdict_schema`` is a no-op: the ``ERVerdict`` TAG and its
    index are created by ``nebula_schema.ensure_schema``, not per-call.

    Values are inline-quoted (nebula binds no params); every write here
    goes through ``store.structured_query`` which RAISES on nGQL failure
    (unlike the fail-open ``store._exec``/``upsert_*`` helpers elsewhere).
    That is deliberate: ``merge_loser_into_canonical``'s safety guarantee
    (never delete the loser without first repointing all of its edges)
    depends on a failed re-insert raising BEFORE the delete is reached.
    """

    def __init__(self, store: Any):
        self._store = store

    def ensure_verdict_schema(self) -> None:
        return None

    def load_verdicts(self, keys: list[str]) -> dict[str, bool]:
        from src.graph.nebula_store import _q

        vids = [verdict_vid(k) for k in keys]
        if not vids:
            return {}
        listed = ", ".join(_q(v) for v in vids)
        rows = self._store.structured_query(
            f"FETCH PROP ON `ERVerdict` {listed} YIELD "
            "`ERVerdict`.er_key AS key, `ERVerdict`.same AS same;"
        )
        return {r["key"]: bool(r["same"]) for r in (rows or []) if isinstance(r, dict)}

    def store_verdicts(self, entries: dict[str, bool]) -> None:
        import time

        from src.graph.nebula_store import _chunks, _q

        if not entries:
            return
        now_ms = int(time.time() * 1000)
        rows = [
            f"{_q(verdict_vid(k))}:({_q(k)}, {'true' if same else 'false'}, {now_ms})"
            for k, same in entries.items()
        ]
        for chunk in _chunks(rows, settings.nebula.write_batch_size):
            stmt = (
                "INSERT VERTEX `ERVerdict` (er_key, same, updated) VALUES "
                + ", ".join(chunk)
                + ";"
            )
            self._store.structured_query(stmt)

    def merge_loser_into_canonical(self, *, loser: str, canon: str) -> None:
        # NOTE: do NOT catch exceptions here — `structured_query` raises on
        # any nGQL failure, and the caller's try/except relies on that to
        # leave the loser intact (edges preserved) on ANY error. Re-inserts
        # are idempotent upserts-by-endpoint, so a retry re-copies harmlessly.
        from src.graph.nebula_store import _chunks, _q, entity_vid

        lv, cv = entity_vid(loser), entity_vid(canon)
        if lv == cv:
            return

        edge_cols = "rel_type, polarity, valid_from, valid_to, weight"

        def edge_props(row: dict) -> str:
            rt = row.get("rt")
            pol = row.get("pol")
            vf = row.get("vf")
            vt = row.get("vt")
            w = row.get("w")
            # valid_from/valid_to are opaque ISO date strings (or '') — carry
            # them through as strings (they were read from RELATED as strings).
            return (
                f"({_q(rt)}, {_q(pol)}, {_q(vf or '')}, {_q(vt or '')}, "
                f"{float(w if w is not None else 1.0)})"
            )

        def insert_edges(rows: list[str]) -> None:
            for chunk in _chunks(rows, settings.nebula.write_batch_size):
                stmt = (
                    f"INSERT EDGE `RELATED` ({edge_cols}) VALUES "
                    + ", ".join(chunk)
                    + ";"
                )
                self._store.structured_query(stmt)

        # Out-edges: loser -> t  ==>  canon -> t (skip t == canon).
        out_rows = self._store.structured_query(
            f"GO FROM {_q(lv)} OVER `RELATED` YIELD "
            "dst(edge) AS t, `RELATED`.rel_type AS rt, "
            "`RELATED`.polarity AS pol, `RELATED`.valid_from AS vf, "
            "`RELATED`.valid_to AS vt, `RELATED`.weight AS w;"
        )
        out_values = [
            f'"{cv}" -> "{r.get("t")}":{edge_props(r)}'
            for r in (out_rows or [])
            if r.get("t") != cv
        ]
        insert_edges(out_values)

        # In-edges: s -> loser  ==>  s -> canon (skip s == canon).
        in_rows = self._store.structured_query(
            f"GO FROM {_q(lv)} OVER `RELATED` REVERSELY YIELD "
            "src(edge) AS s, `RELATED`.rel_type AS rt, "
            "`RELATED`.polarity AS pol, `RELATED`.valid_from AS vf, "
            "`RELATED`.valid_to AS vt, `RELATED`.weight AS w;"
        )
        in_values = [
            f'"{r.get("s")}" -> "{cv}":{edge_props(r)}'
            for r in (in_rows or [])
            if r.get("s") != cv
        ]
        insert_edges(in_values)

        # Only after both edge-redirect passes fully succeed do we drop the
        # loser vertex — never delete-without-repointing.
        self._store.structured_query(f"DELETE VERTEX {_q(lv)} WITH EDGE;")


def build_er_graph_ops(store: Any) -> ERGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaERGraphOps(store)
    return Neo4jERGraphOps(store)

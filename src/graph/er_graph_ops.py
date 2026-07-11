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
    """nGQL ER graph ops. STUB — implemented in Task 3.

    ``ensure_verdict_schema`` is a no-op: the ``ERVerdict`` TAG and its
    index are created by ``nebula_schema.ensure_schema``, not per-call.
    """

    def __init__(self, store: Any):
        self._store = store

    def ensure_verdict_schema(self) -> None:
        return None

    def load_verdicts(self, keys: list[str]) -> dict[str, bool]:
        raise NotImplementedError("NebulaERGraphOps.load_verdicts (Task 3)")

    def store_verdicts(self, entries: dict[str, bool]) -> None:
        raise NotImplementedError("NebulaERGraphOps.store_verdicts (Task 3)")

    def merge_loser_into_canonical(self, *, loser: str, canon: str) -> None:
        raise NotImplementedError("NebulaERGraphOps.merge_loser_into_canonical (Task 3)")


def build_er_graph_ops(store: Any) -> ERGraphOps:
    if settings.graph.backend == "nebula":
        return NebulaERGraphOps(store)
    return Neo4jERGraphOps(store)

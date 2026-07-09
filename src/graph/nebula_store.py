"""NebulaGraph implementation of the KbGraphStore seam (write path).

Phase 1 scope: schema-aware writes (upsert_nodes/upsert_relations) + a raw
nGQL passthrough (structured_query).  Cypher READ queries are translated in
Phase 2; until then reads still run against Neo4j.  Process-global cache
mirrors src/graph/store.py's Neo4j process-global store builder — one
pooled client per process, thread-safe session use.
"""

from __future__ import annotations

import contextlib
import hashlib
import re
import threading
from typing import Any

from loguru import logger

from src.config import settings
from src.graph.nebula_schema import ensure_schema

_store: NebulaGraphStore | None = None
_lock = threading.Lock()

_SAFE_EDGE_LABEL = re.compile(r"[A-Za-z0-9_]+")


def _safe_edge_label(label: str) -> str:
    """nGQL edge types are bare identifiers spliced into the query, so a
    label must be a plain identifier. Fall back to RELATED for anything
    unsafe — defense-in-depth so this module never trusts caller input.
    (Whether a regex-safe label is a *declared* edge type is a separate
    Phase-2 schema-mapping concern: Neo4j allows dynamic rel types, Nebula
    requires pre-declared edge types — not handled here.)"""
    return label if label and _SAFE_EDGE_LABEL.fullmatch(label) else "RELATED"


def entity_vid(name: str) -> str:
    """Stable 128-bit VID as a 32-char hex string from an entity name.

    read/write must agree on this. 128-bit (blake2b digest_size=16) instead
    of 64-bit so birthday collisions stay negligible at the billions-of-
    entities target — a VID collision silently merges two distinct entities.
    The space is created with vid_type=FIXED_STRING(32) to match (see
    nebula_schema.SPACE_DDL)."""
    return hashlib.blake2b((name or "").encode("utf-8"), digest_size=16).hexdigest()


def _q(value: Any) -> str:
    """Quote a scalar for inline nGQL (strings only; ints pass through)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
    s = s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return f'"{s}"'


class NebulaGraphStore:
    def __init__(self, session: Any):
        self._session = session

    # --- writes ---------------------------------------------------------
    def upsert_nodes(self, nodes: list[Any]) -> None:
        for n in nodes:
            props = getattr(n, "properties", {}) or {}
            vid = entity_vid(getattr(n, "name", ""))
            stmt = (
                "INSERT VERTEX `Entity` "
                "(name, description, mention_count, created_at, label) VALUES "
                f"{_q(vid)}:({_q(getattr(n, 'name', ''))}, "
                f"{_q(props.get('description', ''))}, "
                f"{int(props.get('mention_count', 0) or 0)}, "
                f"{int(props.get('created_at', 0) or 0)}, "
                f"{_q(getattr(n, 'label', '') or '')});"
            )
            self._exec(stmt)

    def upsert_relations(self, relations: list[Any]) -> None:
        for r in relations:
            label = _safe_edge_label(getattr(r, "label", "RELATED") or "RELATED")
            props = getattr(r, "properties", {}) or {}
            src = entity_vid(getattr(r, "source_id", ""))
            tgt = entity_vid(getattr(r, "target_id", ""))
            stmt = (
                f"INSERT EDGE `{label}` (polarity, valid_from, valid_to) VALUES "
                f"{_q(src)} -> {_q(tgt)}:("
                f"{_q(props.get('polarity', ''))}, "
                f"{int(props.get('valid_from', 0) or 0)}, "
                f"{int(props.get('valid_to', 0) or 0)});"
            )
            self._exec(stmt)

    # --- raw nGQL (Phase 2 read path builds on this) --------------------
    def structured_query(self, query: str, param_map: dict[str, Any] | None = None) -> list[dict]:
        # param_map is not bound into nGQL yet (Phase 2) — fail loud rather
        # than silently dropping caller-supplied params. `None`/`{}` (the
        # common no-params call) still pass through and execute normally.
        if param_map:
            raise NotImplementedError(
                "NebulaGraphStore.structured_query does not bind nGQL params yet "
                f"(Phase 2); got param_map keys: {sorted(param_map)}"
            )
        resp = self._session.execute(query)
        if not resp.is_succeeded():
            raise RuntimeError(f"nGQL failed: {resp.error_msg()}")
        return _rows_to_dicts(resp)

    def _exec(self, stmt: str) -> None:
        resp = self._session.execute(stmt)
        if not resp.is_succeeded():
            logger.warning("nebula write failed: {s} -> {e}", s=stmt[:80], e=resp.error_msg())

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._session.release()


def _rows_to_dicts(resp: Any) -> list[dict]:
    """Map a nebula3 ResultSet to a list of column->value dicts."""
    cols = resp.keys()
    out: list[dict] = []
    for i in range(resp.row_size()):
        row = resp.row_values(i)
        out.append({c: row[j].cast() for j, c in enumerate(cols)})
    return out


def build_nebula_graph_store() -> NebulaGraphStore:
    """Process-global NebulaGraph store (mirrors build_neo4j_graph_store)."""
    global _store
    if _store is not None:
        return _store
    with _lock:
        if _store is None:
            from nebula3.Config import Config
            from nebula3.gclient.net import ConnectionPool

            cfg = settings.nebula
            pool = ConnectionPool()
            pool.init([(cfg.host, cfg.port)], Config())
            sess = pool.get_session(cfg.user, cfg.password.get_secret_value())
            ensure_schema(sess)
            _store = NebulaGraphStore(sess)
    return _store

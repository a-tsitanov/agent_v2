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
import threading
from typing import Any

from loguru import logger

from src.config import settings
from src.graph.nebula_schema import ensure_schema

_store: "NebulaGraphStore | None" = None
_lock = threading.Lock()


def entity_vid(name: str) -> int:
    """Stable signed int64 VID from an entity name (read/write must agree)."""
    h = hashlib.blake2b((name or "").encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "big", signed=True)


def _q(value: Any) -> str:
    """Quote a scalar for inline nGQL (strings only; ints pass through)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value or "").replace("\\", "\\\\").replace('"', '\\"')
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
                f"{vid}:({_q(getattr(n, 'name', ''))}, "
                f"{_q(props.get('description', ''))}, "
                f"{int(props.get('mention_count', 0) or 0)}, "
                f"{int(props.get('created_at', 0) or 0)}, "
                f"{_q(getattr(n, 'label', '') or '')});"
            )
            self._exec(stmt)

    def upsert_relations(self, relations: list[Any]) -> None:
        for r in relations:
            label = getattr(r, "label", "RELATED") or "RELATED"
            props = getattr(r, "properties", {}) or {}
            src = entity_vid(getattr(r, "source_id", ""))
            tgt = entity_vid(getattr(r, "target_id", ""))
            stmt = (
                f"INSERT EDGE `{label}` (polarity, valid_from, valid_to) VALUES "
                f"{src} -> {tgt}:("
                f"{_q(props.get('polarity', ''))}, "
                f"{int(props.get('valid_from', 0) or 0)}, "
                f"{int(props.get('valid_to', 0) or 0)});"
            )
            self._exec(stmt)

    # --- raw nGQL (Phase 2 read path builds on this) --------------------
    def structured_query(self, query: str, param_map: dict[str, Any] | None = None) -> list[dict]:
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


def build_nebula_graph_store() -> "NebulaGraphStore":
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

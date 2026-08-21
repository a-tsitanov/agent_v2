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
from collections.abc import Iterator
from typing import Any

from loguru import logger

from src.config import settings
from src.graph.nebula_schema import ensure_schema

_store: NebulaGraphStore | None = None
# Held at module scope only to keep the ConnectionPool alive for the life of
# the process (a GC'd pool would close the reconnect path's sockets).
_pool: Any = None
_lock = threading.Lock()

# graphd kills idle sessions server-side (session_idle_timeout_secs); the
# cached session object then keeps failing every execute() with one of these
# markers until re-acquired. Detect that so _run can self-heal (see the
# reconnect closure in build_nebula_graph_store).
_SESSION_DEAD_MARKERS = ("session not existed", "session not found")


def _is_session_dead(resp: Any) -> bool:
    """True when a failed ResultSet blames an expired/unknown session."""
    try:
        if resp is None or resp.is_succeeded():
            return False
        msg = (resp.error_msg() or "").lower()
    except Exception:
        return False
    return any(m in msg for m in _SESSION_DEAD_MARKERS)


def _acquire_session(pool: Any, user: str, password: str) -> Any:
    """Get a session from the pool, re-probing a written-off address first.

    nebula3 marks an address S_BAD as soon as graphd is unreachable and only
    clears it from the interval_check health thread. That thread is enabled
    below, but it fires on a timer — a session asked for inside the gap would
    still hit 'No available server'. Force the re-probe on failure so a
    restarted graphd is picked up immediately instead of poisoning the pool
    for the life of the process."""
    try:
        return pool.get_session(user, password)
    except Exception:
        pool.update_servers_status()
        return pool.get_session(user, password)


def entity_vid(name: str) -> str:
    """Stable 128-bit VID as a 32-char hex string from an entity name.

    read/write must agree on this. 128-bit (blake2b digest_size=16) instead
    of 64-bit so birthday collisions stay negligible at the billions-of-
    entities target — a VID collision silently merges two distinct entities.
    The space is created with vid_type=FIXED_STRING(32) to match (see
    nebula_schema.SPACE_DDL)."""
    return hashlib.blake2b((name or "").encode("utf-8"), digest_size=16).hexdigest()


def _chunks(seq: list[Any], n: int) -> Iterator[list[Any]]:
    """Yield successive n-sized slices of seq (n >= 1)."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


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
    def __init__(self, session: Any, reconnect: Any = None):
        self._session = session
        # Optional zero-arg callable returning a fresh, space-selected
        # session. Set by build_nebula_graph_store so _run can transparently
        # re-acquire when graphd has expired the cached session (None in
        # tests / direct construction → no self-heal, prior behaviour).
        self._reconnect = reconnect
        # The nebula3 session/connection is NOT thread-safe: concurrent
        # execute() on one thrift socket desyncs the protocol → the graphd
        # returns 'Unknown client type'. This store is a shared singleton used
        # from many worker threads (asyncio.to_thread writes + read tools), so
        # serialize every execute() through one lock.
        self._exec_lock = threading.Lock()

    def _run(self, stmt: str) -> Any:
        with self._exec_lock:
            try:
                resp = self._session.execute(stmt)
            except Exception:
                # A hard socket/session failure — try one reconnect+retry
                # before surfacing it (rare; graphd dropped the connection).
                if self._reconnect is None:
                    raise
                self._session = self._reconnect()
                return self._session.execute(stmt)
            # A soft "Session not existed" ResultSet: re-acquire once and
            # replay the statement on the fresh session.
            if self._reconnect is not None and _is_session_dead(resp):
                logger.warning("nebula: session expired — reconnecting and retrying")
                self._session = self._reconnect()
                resp = self._session.execute(stmt)
            return resp

    # --- writes ---------------------------------------------------------
    def upsert_nodes(self, nodes: list[Any]) -> None:
        for chunk in _chunks(nodes, settings.nebula.write_batch_size):
            by_vid = {entity_vid(getattr(n, "name", "")): n for n in chunk}

            # Read-back (best-effort): preserve created_at/first_doc_id for
            # entities that already exist. INSERT VERTEX resets omitted
            # columns on nebula, so without this every re-upsert would wipe
            # first-seen provenance. Fail-open — a FETCH failure just means
            # no preservation this round (all nodes treated as new); the
            # INSERT below still runs so ingest is never blocked.
            preserved: dict[str, tuple[Any, Any]] = {}
            if by_vid:
                try:
                    vids = ", ".join(_q(v) for v in by_vid)
                    fetch_stmt = (
                        f"FETCH PROP ON `Entity` {vids} "
                        "YIELD id(vertex) AS vid, `Entity`.created_at AS ca, "
                        "`Entity`.first_doc_id AS fdi;"
                    )
                    rows = self.structured_query(fetch_stmt)
                    preserved = {r["vid"]: (r.get("ca"), r.get("fdi")) for r in rows}
                except Exception as exc:
                    # Fail-open: this chunk's existing entities won't have their
                    # created_at/first_doc_id preserved this round (they may
                    # drift), but ingest continues. Logged so a persistent
                    # read-back failure is not invisible (matches the
                    # fail-open-but-logged convention of _exec/_execute_with_retry).
                    logger.warning(
                        "upsert_nodes read-back failed (no first-seen preservation "
                        "this round): {e}", e=repr(exc),
                    )
                    preserved = {}

            def row(n: Any, _preserved: dict[str, tuple[Any, Any]] = preserved) -> str:
                props = getattr(n, "properties", {}) or {}
                vid = entity_vid(getattr(n, "name", ""))
                if vid in _preserved:
                    ca_raw, fdi_raw = _preserved[vid]
                    ca = int(ca_raw or 0)
                    fdi = fdi_raw or ""
                else:
                    ca = int(props.get("created_at", 0) or 0)
                    fdi = props.get("first_doc_id", "") or ""
                return (
                    f"{_q(vid)}:({_q(getattr(n, 'name', ''))}, "
                    f"{_q(props.get('description', ''))}, "
                    f"{int(props.get('mention_count', 0) or 0)}, "
                    f"{ca}, "
                    f"{_q(getattr(n, 'label', '') or '')}, "
                    f"{_q(props.get('er_canonical_name', '') or '')}, "
                    f"{_q(fdi)}, "
                    # E2 event-timeframe fields (empty/0 for non-event entities).
                    f"{_q(props.get('event_type', '') or '')}, "
                    f"{_q(props.get('event_ts_raw', '') or '')}, "
                    f"{int(props.get('event_start_epoch') or 0)}, "
                    f"{int(props.get('event_end_epoch') or 0)}, "
                    f"{_q(props.get('event_ts_precision', '') or '')})"
                )

            stmt = (
                "INSERT VERTEX `Entity` (name, description, mention_count, created_at, "
                "label, er_canonical_name, first_doc_id, "
                "event_type, event_ts_raw, event_start_epoch, event_end_epoch, "
                "event_ts_precision) VALUES "
                + ", ".join(row(n) for n in chunk)
                + ";"
            )
            self._exec(stmt)

            # Search mirror (fail-soft). Same nodes, same entity_vid key.
            from src.graph.entity_table import mirror_entities

            mirror_entities(
                [
                    {
                        "vid": entity_vid(getattr(n, "name", "")),
                        "name": getattr(n, "name", ""),
                        "label": getattr(n, "label", "") or "",
                        "description": (getattr(n, "properties", {}) or {}).get(
                            "description", "",
                        ),
                        "mention_count": (getattr(n, "properties", {}) or {}).get(
                            "mention_count", 1,
                        ),
                    }
                    for n in chunk
                ],
            )

    def upsert_relations(self, relations: list[Any]) -> None:
        # Neo4j allows dynamic relationship types; Nebula needs declared
        # edge types. Entity-entity relations all become `RELATED`, with
        # the original type stored in the `rel_type` PROPERTY (a value,
        # so no edge-identifier injection). See ADR / Phase-2 spec.
        for chunk in _chunks(relations, settings.nebula.write_batch_size):
            # Read-back (best-effort): preserve created_at/first_doc_id for edges
            # that already exist. INSERT EDGE resets omitted columns on nebula, so
            # without this every re-upsert would wipe first-seen provenance (same
            # rework as upsert_nodes). Keyed by (src_vid, tgt_vid) — upsert writes
            # rank 0. Fail-open: a FETCH failure just skips preservation this round.
            keys = {
                (entity_vid(getattr(r, "source_id", "")), entity_vid(getattr(r, "target_id", "")))
                for r in chunk
            }
            preserved: dict[tuple[str, str], tuple[Any, Any]] = {}
            if keys:
                try:
                    edge_list = ", ".join(f"{_q(s)}->{_q(t)}" for s, t in keys)
                    rows = self.structured_query(
                        f"FETCH PROP ON `RELATED` {edge_list} YIELD "
                        "src(edge) AS s, dst(edge) AS d, "
                        "`RELATED`.created_at AS ca, `RELATED`.first_doc_id AS fdi;"
                    )
                    preserved = {
                        (r["s"], r["d"]): (r.get("ca"), r.get("fdi")) for r in rows
                    }
                except Exception as exc:
                    logger.warning(
                        "upsert_relations read-back failed (no first-seen preservation "
                        "this round): {e}", e=repr(exc),
                    )
                    preserved = {}

            def row(r: Any, _preserved: dict[tuple[str, str], tuple[Any, Any]] = preserved) -> str:
                rel_type = getattr(r, "label", "") or ""
                props = getattr(r, "properties", {}) or {}
                src = entity_vid(getattr(r, "source_id", ""))
                tgt = entity_vid(getattr(r, "target_id", ""))
                # first-write-wins: keep the existing created_at/first_doc_id if the
                # edge already carries one (created_at != 0), else take this pass's.
                prev = _preserved.get((src, tgt))
                if prev and int(prev[0] or 0) != 0:
                    ca, fdi = int(prev[0] or 0), (prev[1] or "")
                else:
                    ca = int(props.get("created_at", 0) or 0)
                    fdi = props.get("first_doc_id", "") or ""
                return (
                    f"{_q(src)} -> {_q(tgt)}:("
                    f"{_q(rel_type)}, "
                    f"{_q(props.get('polarity', ''))}, "
                    # valid_from/valid_to are opaque ISO date strings (or None) —
                    # store as strings (int() here crashed on a real ISO window).
                    f"{_q(props.get('valid_from') or '')}, "
                    f"{_q(props.get('valid_to') or '')}, "
                    f"{float(props.get('weight', 1.0) or 1.0)}, "
                    f"{ca}, {_q(fdi)})"
                )

            stmt = (
                "INSERT EDGE `RELATED` "
                "(rel_type, polarity, valid_from, valid_to, weight, created_at, first_doc_id) "
                "VALUES "
                + ", ".join(row(r) for r in chunk)
                + ";"
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
        resp = self._run(query)
        if not resp.is_succeeded():
            raise RuntimeError(f"nGQL failed: {resp.error_msg()}")
        return _rows_to_dicts(resp)

    def subgraph(self, vid: str, hops: int, *, edge: str = "RELATED") -> list[dict]:
        """Bounded GET SUBGRAPH from `vid`, mapped to the shape
        GraphRetriever._map_walk_rows consumes: a single-element list
        [{entities:[{name,label,description}], relations:[{src,tgt,label,
        polarity,valid_from,valid_to}]}] with src/tgt as entity NAMES and
        `label` taken from the edge's rel_type property."""
        q = (
            f"GET SUBGRAPH WITH PROP {int(hops)} STEPS FROM {_q(vid)} "
            f"BOTH `{edge}` YIELD VERTICES AS nodes, EDGES AS rels;"
        )
        rs = self._run(q)
        if not rs.is_succeeded():
            logger.warning("nebula subgraph failed: {e}", e=rs.error_msg())
            return [{"entities": [], "relations": []}]
        vid_name: dict[str, str] = {}
        entities: list[dict] = []
        edges: list[dict] = []
        keys = rs.keys()
        ni, ei = keys.index("nodes"), keys.index("rels")
        for i in range(rs.row_size()):
            row = rs.row_values(i)
            for nv in row[ni].as_list():
                node = nv.as_node()
                tags = node.tags()
                if not tags:
                    # GET SUBGRAPH returns frontier vertices (endpoints one
                    # step past the last hop) with no tags/props loaded →
                    # node.tags() == [] → the old node.tags()[0] raised
                    # IndexError, failing the whole walk ("graph_walk (nebula)
                    # failed: list index out of range"). No tag ⇒ no props ⇒
                    # no name to map; skip. Edges to it keep whatever endpoint
                    # names the tagged vertices already contributed.
                    continue
                nid = node.get_id().cast()
                props = {k: v.cast() for k, v in node.properties(tags[0]).items()}
                name = props.get("name") or ""
                vid_name[nid] = name
                entities.append({
                    "name": name,
                    "label": props.get("label") or "",
                    "description": props.get("description") or "",
                })
            for ev in row[ei].as_list():
                e = ev.as_relationship()
                ep = {k: v.cast() for k, v in e.properties().items()}
                edges.append({
                    "_src_id": e.start_vertex_id().cast(),
                    "_tgt_id": e.end_vertex_id().cast(),
                    "rel_type": ep.get("rel_type") or "",
                    "polarity": ep.get("polarity"),
                    "valid_from": ep.get("valid_from"),
                    "valid_to": ep.get("valid_to"),
                })
        relations = [{
            "src": vid_name.get(e["_src_id"], ""),
            "tgt": vid_name.get(e["_tgt_id"], ""),
            "label": e["rel_type"],
            "polarity": e["polarity"],
            "valid_from": e["valid_from"],
            "valid_to": e["valid_to"],
        } for e in edges]
        return [{"entities": entities, "relations": relations}]

    def _exec(self, stmt: str) -> None:
        resp = self._run(stmt)
        if not resp.is_succeeded():
            logger.warning("nebula write failed: {s} -> {e}", s=stmt[:80], e=resp.error_msg())

    def close(self) -> None:
        with self._exec_lock, contextlib.suppress(Exception):
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
    """Process-global NebulaGraph store (mirrors build_neo4j_graph_store).

    The store is handed a ``reconnect`` closure so it can transparently
    re-acquire a session when graphd expires the cached one (idle timeout) —
    otherwise the singleton would fail every query with 'Session not existed'
    for the rest of the process's life (seen on the rarely-used global-search
    path). A fresh session must re-run ``ensure_schema`` because ``USE
    <space>`` is per-session state.

    Session re-acquisition goes through ``_acquire_session`` so a graphd
    *restart* (as opposed to an idle-timeout) also heals: the pool writes the
    address off and, on the library defaults, would never take it back."""
    global _store, _pool
    if _store is not None:
        return _store
    with _lock:
        if _store is None:
            from nebula3.Config import Config
            from nebula3.gclient.net import ConnectionPool

            cfg = settings.nebula
            pool_config = Config()
            # Without this the pool never re-checks a graphd it wrote off.
            pool_config.interval_check = cfg.pool_health_check_s
            pool = ConnectionPool()
            pool.init([(cfg.host, cfg.port)], pool_config)
            user = cfg.user
            password = cfg.password.get_secret_value()

            def _new_session() -> Any:
                sess = _acquire_session(pool, user, password)
                ensure_schema(sess)  # selects the space (USE) + idempotent DDL
                return sess

            _pool = pool  # keep the pool alive for the reconnect path
            _store = NebulaGraphStore(_new_session(), reconnect=_new_session)
    return _store

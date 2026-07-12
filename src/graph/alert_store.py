"""Backend-dispatched Arc-2 :Alert store (upsert / read / mark_watched).

``Neo4jAlertStore`` runs the historical Cypher verbatim (the constants live in
src/graph/alerts.py). ``NebulaAlertStore`` uses nGQL against the ``Alert`` TAG +
``Entity.watched`` column (see nebula_schema): MERGE-on-key semantics are
emulated with a FETCH-then-INSERT/UPDATE (unscored = create-only/first-write-wins;
scored = create-or-refresh-score keeping created_at). All ops are fail-soft — the
monitoring sweep must stay alive on a transient graph error.
"""
from __future__ import annotations

from hashlib import blake2b
from typing import Any, Protocol

from loguru import logger

from src.config import settings


def alert_vid(key: str) -> str:
    """32-hex VID for an :Alert, mirroring entity_vid/verdict_vid."""
    return blake2b(key.encode("utf-8"), digest_size=16).hexdigest()


class AlertStore(Protocol):
    def upsert_alert(
        self, *, key: str, kind: str, entity: str, detail: str,
        created_at: int, score: float | None,
    ) -> None: ...

    def read_alerts(
        self, kind: str | None, entity: str | None, since: int | None, top_n: int,
    ) -> list[dict]: ...

    def mark_watched(self, names: list[str], watched: bool) -> None: ...


class Neo4jAlertStore:
    def __init__(self, store: Any):
        self._store = store

    def upsert_alert(self, *, key, kind, entity, detail, created_at, score):
        from src.graph.alerts import _UPSERT_ALERT, _UPSERT_ALERT_SCORED

        params: dict[str, Any] = {
            "key": key, "kind": kind, "entity": entity,
            "detail": detail, "created_at": created_at,
        }
        cypher = _UPSERT_ALERT
        if score is not None:
            params["score"] = score
            cypher = _UPSERT_ALERT_SCORED
        self._store.structured_query(cypher, param_map=params)

    def read_alerts(self, kind, entity, since, top_n):
        from src.graph.alerts import read_alerts_cypher

        rows = self._store.structured_query(
            read_alerts_cypher,
            param_map={"kind": kind, "entity": entity, "since": since, "top_n": top_n},
        )
        return list(rows or [])

    def mark_watched(self, names, watched):
        from src.graph.alerts import _MARK_WATCHED

        self._store.structured_query(
            _MARK_WATCHED, param_map={"names": names, "watched": watched}
        )


class NebulaAlertStore:
    def __init__(self, store: Any):
        self._store = store

    def _exec(self, stmt: str) -> list[dict]:
        return list(self._store.structured_query(stmt) or [])

    def upsert_alert(self, *, key, kind, entity, detail, created_at, score):
        from src.graph.nebula_store import _q

        vid = alert_vid(key)
        existing = self._exec(
            f'FETCH PROP ON `Alert` {_q(vid)} YIELD `Alert`.created_at AS ca;'
        )
        if score is None:
            # unscored MERGE ON CREATE only — first-write-wins, no-op if present.
            if existing:
                return
            self._exec(
                "INSERT VERTEX `Alert` "
                "(alert_key, kind, entity, detail, created_at, score, updated_at) VALUES "
                f'{_q(vid)}:({_q(key)}, {_q(kind)}, {_q(entity)}, {_q(detail)}, '
                f"{int(created_at)}, 0.0, 0);"
            )
            return
        # scored MERGE: create with score, or refresh score+updated_at keeping created_at.
        if existing:
            self._exec(
                f'UPDATE VERTEX ON `Alert` {_q(vid)} '
                f"SET score = {float(score)}, updated_at = {int(created_at)};"
            )
        else:
            self._exec(
                "INSERT VERTEX `Alert` "
                "(alert_key, kind, entity, detail, created_at, score, updated_at) VALUES "
                f'{_q(vid)}:({_q(key)}, {_q(kind)}, {_q(entity)}, {_q(detail)}, '
                f"{int(created_at)}, {float(score)}, {int(created_at)});"
            )

    def read_alerts(self, kind, entity, since, top_n):
        rows = self._exec(
            "LOOKUP ON `Alert` YIELD `Alert`.alert_key AS key, `Alert`.kind AS kind, "
            "`Alert`.entity AS entity, `Alert`.detail AS detail, "
            "`Alert`.created_at AS created_at, `Alert`.score AS score, "
            "`Alert`.updated_at AS updated_at;"
        )
        out = []
        for r in rows:
            if kind is not None and r.get("kind") != kind:
                continue
            if entity is not None and r.get("entity") != entity:
                continue
            if since is not None and int(r.get("created_at") or 0) < since:
                continue
            out.append(r)
        out.sort(key=lambda r: int(r.get("created_at") or 0), reverse=True)
        return out[:top_n]

    def mark_watched(self, names, watched):
        from src.graph.nebula_store import entity_vid

        flag = "true" if watched else "false"
        for name in names:
            stmt = (
                f'UPDATE VERTEX ON `Entity` "{entity_vid(name)}" SET watched = {flag};'
            )
            try:
                self._store.structured_query(stmt)
            except Exception as exc:  # one missing vertex must not stop the rest
                logger.debug("mark_watched skipped for {n}: {e}", n=name, e=exc)


def build_alert_store(store: Any) -> AlertStore:
    if settings.graph.backend == "nebula":
        return NebulaAlertStore(store)
    return Neo4jAlertStore(store)

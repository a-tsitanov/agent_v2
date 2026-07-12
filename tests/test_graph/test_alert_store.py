from __future__ import annotations

from src.graph import alert_store as als
from src.graph.alert_store import alert_vid


class _RecStore:
    def __init__(self, rows=None):
        self.calls: list[tuple[str, dict | None]] = []
        self._rows = list(rows or [])

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map))
        return self._rows.pop(0) if self._rows else []


class _NebulaRecStore:
    def __init__(self, canned: list[tuple[str, list[dict]]] | None = None):
        self.calls: list[tuple[str, dict | None]] = []
        self._canned = list(canned or [])

    def structured_query(self, stmt, param_map=None):
        self.calls.append((stmt, param_map))
        assert param_map is None, "nebula alert store must not use param_map"
        for substr, rows in self._canned:
            if substr in stmt:
                return rows
        return []


# --- Neo4j: byte-for-byte -----------------------------------------------


def test_neo4j_upsert_unscored_uses_create_only_cypher():
    from src.graph.alerts import _UPSERT_ALERT

    store = _RecStore()
    als.Neo4jAlertStore(store).upsert_alert(
        key="new_connection:A:OWNS:B", kind="new_connection", entity="A",
        detail="OWNS:B", created_at=100, score=None,
    )
    assert store.calls[0][0] == _UPSERT_ALERT
    assert store.calls[0][1]["key"] == "new_connection:A:OWNS:B"
    assert "score" not in store.calls[0][1]


def test_neo4j_upsert_scored_uses_scored_cypher():
    from src.graph.alerts import _UPSERT_ALERT_SCORED

    store = _RecStore()
    als.Neo4jAlertStore(store).upsert_alert(
        key="risk_rise:Shell:", kind="risk_rise", entity="Shell",
        detail="", created_at=100, score=0.8,
    )
    assert store.calls[0][0] == _UPSERT_ALERT_SCORED
    assert store.calls[0][1]["score"] == 0.8


def test_neo4j_read_alerts_uses_read_cypher():
    from src.graph.alerts import read_alerts_cypher

    store = _RecStore(rows=[[{"key": "k", "kind": "risk_rise", "entity": "Shell"}]])
    result = als.Neo4jAlertStore(store).read_alerts("risk_rise", None, 19000, 50)
    assert result[0]["entity"] == "Shell"
    assert store.calls[0] == (
        read_alerts_cypher,
        {"kind": "risk_rise", "entity": None, "since": 19000, "top_n": 50},
    )


# --- Nebula: MERGE semantics --------------------------------------------


def test_nebula_upsert_unscored_inserts_when_absent():
    store = _NebulaRecStore(canned=[("FETCH PROP ON `Alert`", [])])  # absent
    als.NebulaAlertStore(store).upsert_alert(
        key="k", kind="new_connection", entity="A", detail="OWNS:B",
        created_at=100, score=None,
    )
    insert = [c[0] for c in store.calls if "INSERT VERTEX `Alert`" in c[0]]
    assert len(insert) == 1
    assert alert_vid("k") in insert[0]
    assert "new_connection" in insert[0]


def test_nebula_upsert_unscored_noop_when_present():
    store = _NebulaRecStore(canned=[("FETCH PROP ON `Alert`", [{"ca": 50}])])  # present
    als.NebulaAlertStore(store).upsert_alert(
        key="k", kind="new_connection", entity="A", detail="d", created_at=100, score=None,
    )
    # first-write-wins: only the FETCH, no INSERT/UPDATE
    assert all("INSERT VERTEX `Alert`" not in c[0] for c in store.calls)
    assert all("UPDATE VERTEX ON `Alert`" not in c[0] for c in store.calls)


def test_nebula_upsert_scored_refreshes_when_present():
    store = _NebulaRecStore(canned=[("FETCH PROP ON `Alert`", [{"ca": 50}])])
    als.NebulaAlertStore(store).upsert_alert(
        key="k", kind="risk_rise", entity="Shell", detail="", created_at=100, score=0.9,
    )
    upd = [c[0] for c in store.calls if "UPDATE VERTEX ON `Alert`" in c[0]]
    assert len(upd) == 1
    assert "score = 0.9" in upd[0] and "updated_at = 100" in upd[0]
    # created_at NOT touched on refresh
    assert "created_at" not in upd[0]


def test_nebula_upsert_scored_inserts_when_absent():
    store = _NebulaRecStore(canned=[("FETCH PROP ON `Alert`", [])])
    als.NebulaAlertStore(store).upsert_alert(
        key="k", kind="risk_rise", entity="Shell", detail="", created_at=100, score=0.9,
    )
    ins = [c[0] for c in store.calls if "INSERT VERTEX `Alert`" in c[0]]
    assert len(ins) == 1 and "0.9" in ins[0]


def test_nebula_read_alerts_filters_and_sorts_in_python():
    rows = [
        {"key": "k1", "kind": "risk_rise", "entity": "A", "detail": "", "created_at": 100,
         "score": 0.5, "updated_at": 100},
        {"key": "k2", "kind": "new_connection", "entity": "B", "detail": "x", "created_at": 200,
         "score": 0.0, "updated_at": 0},
        {"key": "k3", "kind": "risk_rise", "entity": "A", "detail": "", "created_at": 50,
         "score": 0.7, "updated_at": 50},  # below since
    ]
    store = _NebulaRecStore(canned=[("LOOKUP ON `Alert`", rows)])
    result = als.NebulaAlertStore(store).read_alerts("risk_rise", "A", 80, 50)
    # kind=risk_rise, entity=A, created_at>=80 → k1 only
    assert [r["key"] for r in result] == ["k1"]


def test_nebula_mark_watched_updates_each_entity():
    from src.graph.nebula_store import entity_vid

    store = _NebulaRecStore()
    als.NebulaAlertStore(store).mark_watched(["Alice", "Bob"], True)
    stmts = [c[0] for c in store.calls]
    assert any(entity_vid("Alice") in s and "SET watched = true" in s for s in stmts)
    assert any(entity_vid("Bob") in s for s in stmts)


# --- Dispatch ------------------------------------------------------------


def test_dispatch_neo4j_default(monkeypatch):
    monkeypatch.setattr(als.settings.graph, "backend", "neo4j")
    assert isinstance(als.build_alert_store(_RecStore()), als.Neo4jAlertStore)


def test_dispatch_nebula(monkeypatch):
    monkeypatch.setattr(als.settings.graph, "backend", "nebula")
    assert isinstance(als.build_alert_store(_NebulaRecStore()), als.NebulaAlertStore)

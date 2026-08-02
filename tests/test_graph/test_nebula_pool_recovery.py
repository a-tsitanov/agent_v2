"""The nebula3 ConnectionPool recovers after graphd restarts.

Regression (2026-08-02): graphd restarted at 06:00:55. nebula3 flags an
address S_BAD the moment it is unreachable and clears it only from the
``interval_check`` health thread — which the library default (-1) never
starts. So the single graphd address stayed written off for the life of the
process: every later ``get_session`` returned 'No available server', the
merge/search worker pools lost NebulaGraph entirely, graph activities retried
to attempt 20+, the K=5 in-flight ingest slots stalled and ingest.pending
backed up to 254 messages. Only a worker restart cleared it."""
from __future__ import annotations

import pytest

import src.graph.nebula_store as nebula_store


class _FakePool:
    """get_session fails while the address is written off (S_BAD)."""

    def __init__(self, *, heals: bool = True):
        self.written_off = True
        self.status_updates = 0
        self.sessions = 0
        self._heals = heals

    def update_servers_status(self) -> None:
        self.status_updates += 1
        if self._heals:
            self.written_off = False

    def get_session(self, user: str, password: str) -> str:
        if self.written_off:
            raise RuntimeError("No available server")
        self.sessions += 1
        return f"session-{self.sessions}"


def test_acquire_session_reprobes_a_written_off_address():
    pool = _FakePool()
    assert nebula_store._acquire_session(pool, "root", "nebula") == "session-1"
    assert pool.status_updates == 1


def test_acquire_session_does_not_reprobe_a_healthy_pool():
    pool = _FakePool()
    pool.written_off = False
    assert nebula_store._acquire_session(pool, "root", "nebula") == "session-1"
    assert pool.status_updates == 0


def test_acquire_session_reraises_when_the_reprobe_does_not_help():
    # graphd genuinely down — surface it instead of looping.
    pool = _FakePool(heals=False)
    with pytest.raises(RuntimeError, match="No available server"):
        nebula_store._acquire_session(pool, "root", "nebula")
    assert pool.status_updates == 1


def test_pool_is_built_with_a_background_health_check(monkeypatch):
    """interval_check > 0 is what starts nebula3's periodic re-probe."""
    import nebula3.Config as nebula_config
    import nebula3.gclient.net as nebula_net

    captured: dict = {}

    class _Cfg:
        def __init__(self):
            self.interval_check = -1  # library default

    class _Pool:
        def init(self, addresses, configs):
            captured["interval_check"] = configs.interval_check
            return True

        def get_session(self, user, password):
            return object()

    monkeypatch.setattr(nebula_config, "Config", _Cfg)
    monkeypatch.setattr(nebula_net, "ConnectionPool", _Pool)
    monkeypatch.setattr(nebula_store, "ensure_schema", lambda sess: None)
    monkeypatch.setattr(nebula_store, "_store", None)
    monkeypatch.setattr(nebula_store, "_pool", None)

    nebula_store.build_nebula_graph_store()

    assert captured["interval_check"] > 0

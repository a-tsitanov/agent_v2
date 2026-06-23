"""Process-global Neo4j graph-store cache + driver-pool kwargs (Track A2).

No live Neo4j: the real store construction is patched so we exercise the
caching contract (one driver/pool per process, mirroring get_pg_pool)
and the settings→driver-kwargs mapping in isolation.
"""
from __future__ import annotations

import src.graph.store as store_mod


def teardown_function() -> None:
    store_mod.reset_neo4j_graph_store()


def test_driver_kwargs_reflect_settings(monkeypatch) -> None:
    cfg = store_mod.settings.neo4j
    monkeypatch.setattr(cfg, "max_connection_pool_size", 24, raising=False)
    monkeypatch.setattr(cfg, "connection_acquisition_timeout_s", 45.0, raising=False)
    monkeypatch.setattr(cfg, "connection_timeout_s", 12.0, raising=False)

    kw = store_mod._neo4j_driver_kwargs()
    assert kw["max_connection_pool_size"] == 24
    assert kw["connection_acquisition_timeout"] == 45.0
    assert kw["connection_timeout"] == 12.0


def test_build_caches_one_store_per_process(monkeypatch) -> None:
    calls = {"n": 0}

    class _FakeStore:
        def close(self) -> None:
            pass

    def _fake_construct():
        calls["n"] += 1
        return _FakeStore()

    monkeypatch.setattr(store_mod, "_construct_neo4j_graph_store", _fake_construct)

    s1 = store_mod.build_neo4j_graph_store()
    s2 = store_mod.build_neo4j_graph_store()
    assert s1 is s2  # same instance → one driver/pool, no per-call construction
    assert calls["n"] == 1


def test_reset_rebuilds_and_closes(monkeypatch) -> None:
    closed = {"n": 0}

    class _FakeStore:
        def close(self) -> None:
            closed["n"] += 1

    monkeypatch.setattr(
        store_mod, "_construct_neo4j_graph_store", lambda: _FakeStore()
    )

    s1 = store_mod.build_neo4j_graph_store()
    store_mod.reset_neo4j_graph_store()
    assert closed["n"] == 1  # reset closed the driver
    s2 = store_mod.build_neo4j_graph_store()
    assert s1 is not s2  # a fresh store after reset

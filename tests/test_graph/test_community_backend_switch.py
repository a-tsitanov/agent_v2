import asyncio

import src.graph.communities as comm
import src.graph.community_graphscope as cg


def test_leidenalg_backend_produces_communityrefs(monkeypatch):
    # Backend = leidenalg; stub edge extraction + a no-op store write path.
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "leidenalg")

    def fake_extract(store, *, batch_size=50_000):
        edges = [("a","b",5.0),("b","c",5.0),("a","c",5.0),
                 ("x","y",5.0),("y","z",5.0),("x","z",5.0),("c","x",0.1)]
        return edges, list("abcxyz")
    monkeypatch.setattr(comm, "extract_entity_edges", fake_extract)

    class _Store:
        def structured_query(self, cypher, param_map=None):
            return []          # swallow all writes/reads
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert len(refs) == 2                       # two cliques
    assert all(r.level == 0 for r in refs)


def test_graphscope_backend_produces_communityrefs(monkeypatch):
    # Backend = graphscope; stub edge extraction + the graphscope rows fn +
    # a no-op store write path (mirrors the leidenalg test above).
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "graphscope")

    def fake_extract(store, *, batch_size=50_000):
        edges = [("a", "b", 5.0), ("b", "c", 5.0), ("a", "c", 5.0),
                 ("x", "y", 5.0), ("y", "z", 5.0), ("x", "z", 5.0), ("c", "x", 0.1)]
        return edges, list("abcxyz")
    monkeypatch.setattr(comm, "extract_entity_edges", fake_extract)

    def fake_rows(edges, names, *, gamma, seed=19):
        return [
            {"name": "a", "communityId": "0", "ids": ["0"]},
            {"name": "b", "communityId": "0", "ids": ["0"]},
            {"name": "c", "communityId": "0", "ids": ["0"]},
            {"name": "x", "communityId": "1", "ids": ["1"]},
            {"name": "y", "communityId": "1", "ids": ["1"]},
            {"name": "z", "communityId": "1", "ids": ["1"]},
        ]
    monkeypatch.setattr(comm, "single_level_rows_graphscope", fake_rows)

    class _Store:
        def structured_query(self, cypher, param_map=None):
            return []          # swallow all writes/reads
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert len(refs) == 2                       # two cliques
    assert all(r.level == 0 for r in refs)


def test_graphscope_backend_is_fail_safe(monkeypatch):
    # Outer try/except belt-and-suspenders: even if something inside the
    # graphscope branch RAISES outright (not the adapter's real fail-open
    # path — see test below for that), detect_communities must still yield
    # [] (logged), never propagate, exactly like the leidenalg/GDS branches.
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "graphscope")
    monkeypatch.setattr(comm, "extract_entity_edges", lambda store, **kw: ([], []))

    def boom(edges, names, *, gamma, seed=19):
        raise RuntimeError("graphscope cluster unreachable")
    monkeypatch.setattr(comm, "single_level_rows_graphscope", boom)

    class _Store:
        def structured_query(self, cypher, param_map=None):
            return []
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert refs == []


def test_graphscope_adapter_fail_open_does_not_write_mega_community(monkeypatch):
    # This is the REAL fail-open path (the bug this whole test file exists
    # to guard against): the GraphScope adapter never raises — it fail-opens
    # to {} (import/API error inside _run_graphscope_community, or the stub
    # pre-manual-gate). Leave single_level_rows_graphscope REAL; only mock the
    # adapter it wraps. Before the fix, membership={} would map EVERY name to
    # communityId "0" via `.get(name, "0")`, producing a non-empty `rows` that
    # sailed past the graphscope branch's try/except untouched, so
    # detect_communities would PRUNE the prior :Community nodes and MERGE one
    # garbage mega-community. The single_level_rows_graphscope guard turns the
    # empty membership into empty rows, and the graphscope branch's
    # `if not rows: return []` then makes it a TRUE no-op — returning before
    # the write-back so the existing :Community layer is never touched (no
    # MERGE, and crucially no level PRUNE that would clear it).
    monkeypatch.setattr(comm.settings.temporal, "community_backend", "graphscope")

    def fake_extract(store, *, batch_size=50_000):
        edges = [("a", "b", 5.0), ("b", "c", 5.0), ("a", "c", 5.0),
                 ("x", "y", 5.0), ("y", "z", 5.0), ("x", "z", 5.0), ("c", "x", 0.1)]
        return edges, list("abcxyz")
    monkeypatch.setattr(comm, "extract_entity_edges", fake_extract)
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {})

    write_calls: list[str] = []

    class _Store:
        def structured_query(self, cypher, param_map=None):
            write_calls.append(cypher)
            return []
    refs = asyncio.run(comm.detect_communities(_Store(), min_size=2, level=0))
    assert refs == []
    # True no-op: the branch returns BEFORE the write-back, so NOTHING is
    # written to the store — not the degenerate mega-community MERGE, and
    # crucially not the level PRUNE that would otherwise clear the existing
    # :Community layer under a mere backend misconfiguration.
    assert write_calls == [], f"fail-open must not touch the store, got: {write_calls}"

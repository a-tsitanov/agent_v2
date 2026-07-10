import asyncio

import src.graph.communities as comm


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
    # A graphscope-side error must yield [] (logged), never raise, exactly
    # like the leidenalg/GDS branches.
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

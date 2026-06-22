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

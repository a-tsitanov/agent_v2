"""Analytics layer — algorithmic analysis over the knowledge graph.

A unified analytical core (this package) with thin facades (search /
admin / batch).  v1 ships **temporal analytics**: bitemporal
point-in-time ``snapshot`` and ``diff`` over the graph.  Other algorithm
modules (centrality, anomaly, embeddings) plug into the same frame.

Like ``graph/analysis.py`` and ``graph/communities.py``, every public
entry point is **fail-soft**: a ``None`` store or any Cypher error is
logged and yields a safe empty result, never raised through the caller.
"""

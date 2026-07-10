"""single_level_rows_graphscope maps a mocked GraphScope partition to rows."""
from __future__ import annotations

import src.graph.community_graphscope as cg


def test_single_level_rows_maps_membership(monkeypatch):
    # Mock the only GraphScope-touching function with a canned partition.
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {"A": "0", "B": "0", "C": "1"})
    edges = [("A", "B", 1.0), ("B", "C", 1.0)]
    rows = cg.single_level_rows_graphscope(edges, ["A", "B", "C"], gamma=1.0)
    by = {r["name"]: r for r in rows}
    assert by["A"] == {"name": "A", "communityId": "0", "ids": ["0"]}
    assert by["C"]["communityId"] == "1" and by["C"]["ids"] == ["1"]


def test_rows_cover_edge_endpoint_names_and_default(monkeypatch):
    # A name only present in edges (not node_names) must still get a row;
    # a name absent from the membership map defaults to "0".
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {"A": "5"})
    rows = cg.single_level_rows_graphscope([("A", "Z", 1.0)], ["A"], gamma=1.0)
    names = {r["name"] for r in rows}
    assert names == {"A", "Z"}                 # Z recovered from the edge
    assert {r["name"]: r["communityId"] for r in rows}["Z"] == "0"  # default


def test_empty_membership_short_circuits_to_no_rows(monkeypatch):
    # This is the REAL fail-open path: _run_graphscope_community fail-opens
    # to {} (import/API error, never raises). Before the fix, every name
    # would fall through membership.get(name, "0") and produce a non-empty
    # list of rows all mapped to the single garbage community "0" — which
    # detect_communities' graphscope branch would then happily persist as a
    # destructive mega-community. The guard must short-circuit to [] instead,
    # matching the leidenalg branch's raise->[] no-op semantics.
    monkeypatch.setattr(cg, "_run_graphscope_community",
                        lambda edges, names, *, gamma, seed: {})
    edges = [("A", "B", 1.0), ("B", "C", 1.0)]
    rows = cg.single_level_rows_graphscope(edges, ["A", "B", "C"], gamma=1.0)
    assert rows == []

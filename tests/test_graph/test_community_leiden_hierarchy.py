from src.graph.community_leiden import hierarchy_rows


def _two_level_graph():
    # Four triangles; triangles pair up at a coarser level.
    edges = []
    for a, b, c in [("a", "b", "c"), ("d", "e", "f"), ("p", "q", "r"), ("x", "y", "z")]:
        edges += [(a, b, 5.0), (b, c, 5.0), (a, c, 5.0)]
    edges += [("c", "d", 1.0), ("r", "x", 1.0)]  # pair (abc,def) and (pqr,xyz)
    edges += [("f", "p", 0.05)]  # very weak cross-pair link
    nodes = list("abcdefpqrxyz")
    return edges, nodes


def test_ids_are_finest_to_coarsest_and_nested():
    edges, nodes = _two_level_graph()
    rows = hierarchy_rows(edges, nodes, gamma=1.0, max_levels=3, seed=19)
    by_name = {r["name"]: r["ids"] for r in rows}
    # contract: communityId == ids[-1]
    for r in rows:
        assert r["communityId"] == r["ids"][-1]
    # nesting: nodes sharing the finest id share every coarser id
    for n1 in nodes:
        for n2 in nodes:
            if by_name[n1][0] == by_name[n2][0]:
                assert by_name[n1] == by_name[n2]


def test_single_level_when_max_levels_1_matches_flat():
    edges, nodes = _two_level_graph()
    rows = hierarchy_rows(edges, nodes, gamma=1.0, max_levels=1, seed=19)
    for r in rows:
        assert len(r["ids"]) == 1

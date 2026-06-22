from src.graph.community_leiden import single_level_rows


def test_two_cliques_split_into_two_communities():
    # Two triangles joined by a single weak edge → two communities.
    edges = [
        ("a", "b", 5.0), ("b", "c", 5.0), ("a", "c", 5.0),
        ("x", "y", 5.0), ("y", "z", 5.0), ("x", "z", 5.0),
        ("c", "x", 0.1),
    ]
    nodes = ["a", "b", "c", "x", "y", "z"]
    rows = single_level_rows(edges, nodes, gamma=1.0, seed=19)

    by_name = {r["name"]: r["communityId"] for r in rows}
    assert by_name["a"] == by_name["b"] == by_name["c"]
    assert by_name["x"] == by_name["y"] == by_name["z"]
    assert by_name["a"] != by_name["x"]
    # contract: communityId == ids[-1]
    for r in rows:
        assert r["ids"][-1] == r["communityId"]


def test_deterministic_with_fixed_seed():
    edges = [("a", "b", 1.0), ("b", "c", 1.0), ("x", "y", 1.0)]
    nodes = ["a", "b", "c", "x", "y"]
    r1 = single_level_rows(edges, nodes, gamma=1.0, seed=19)
    r2 = single_level_rows(edges, nodes, gamma=1.0, seed=19)
    assert {r["name"]: r["communityId"] for r in r1} == \
           {r["name"]: r["communityId"] for r in r2}

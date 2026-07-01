"""Tests that the /admin/graph/materialize POST route is registered."""

from src.api.routes.graph_admin import router


def test_materialize_route_present():
    paths = {r.path for r in router.routes}
    assert "/admin/graph/materialize" in paths

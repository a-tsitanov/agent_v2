# tests/test_analytics/test_catalog.py
from src.analytics import store_query
from src.analytics.catalog import (
    CATALOG,
    Primitive,
    PrimitiveResult,
    register,
    render_catalog_for_planner,
)
from tests.test_analytics.conftest import _FakeStore


async def test_run_rows_failsoft_on_none_store():
    assert await store_query.run_rows(None, "MATCH (n) RETURN n", {}) == []


async def test_run_rows_passes_cypher_and_params():
    store = _FakeStore(rows=[{"n": 1}])
    rows = await store_query.run_rows(store, "MATCH (n) RETURN n", {"x": 1})
    assert rows == [{"n": 1}]
    assert store.last_params == {"x": 1}


async def test_run_rows_failsoft_on_error():
    class _Boom:
        def structured_query(self, *a, **k):
            raise RuntimeError("db down")

    assert await store_query.run_rows(_Boom(), "X", {}) == []


def test_register_and_render():
    from pydantic import BaseModel

    class _P(BaseModel):
        pass

    async def _fn(store, **kw):
        return PrimitiveResult(cypher="C", params={}, rows=[], source_chunks=[], truncated=False)

    register(
        Primitive(name="_demo", fn=_fn, param_model=_P, description="demo desc", tier="online")
    )
    assert "_demo" in CATALOG
    assert "demo desc" in render_catalog_for_planner()

import pytest

from src.analytics.primitives import connections as conn
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_entity_dossier_assembles_sections():
    store = _FakeStore(
        by_call=[
            [
                {
                    "name": "Ромашка",
                    "description": "d",
                    "labels": ["__Entity__", "Organization"],
                    "mention_count": 4,
                }
            ],  # core
            [{"rel": "OWNS", "name": "ООО Лютик", "ntype": "Organization", "w": 2.0}],  # neighbors
            [{"id_type": "INN", "value": "7701234567"}],  # identifiers
            [{"level": 0, "title": "Поставки"}],  # communities
        ]
    )
    res = await conn.entity_dossier(store, name="Ромашка")
    row = res.rows[0]
    assert row["core"]["name"] == "Ромашка"
    assert row["connections"][0]["rel"] == "OWNS"
    assert row["identifiers"][0]["id_type"] == "INN"
    assert row["communities"][0]["title"] == "Поставки"
    assert res.params["name"] == "Ромашка"


@pytest.mark.asyncio
async def test_shared_identifier_entities_min_owners():
    store = _FakeStore(rows=[{"value": "7701234567", "id_type": "INN", "owners": ["A", "B"]}])
    res = await conn.shared_identifier_entities(store, min_owners=2)
    assert res.params["min_owners"] == 2
    assert "size(owners) >= $min_owners" in res.cypher


@pytest.mark.asyncio
async def test_connection_path_clamps_hops_inline():
    store = _FakeStore(rows=[{"path": ["A", "B"], "rels": ["OWNS"], "hops": 1}])
    res = await conn.connection_path(store, source="A", target="B", max_hops=99)
    # hops clamped into the Cypher literal (shortestPath bound), max 12
    assert "*..12" in res.cypher


@pytest.mark.asyncio
async def test_cooccurrence_via_shared_chunks():
    store = _FakeStore(rows=[{"name": "B", "shared": 3}])
    res = await conn.cooccurrence(store, name="A")
    assert ":MENTIONS" in res.cypher


@pytest.mark.asyncio
async def test_failsoft():
    assert (await conn.entity_dossier(None, name="X")).rows == []
    assert (await conn.identifier_lookup(None, value="x")).rows == []

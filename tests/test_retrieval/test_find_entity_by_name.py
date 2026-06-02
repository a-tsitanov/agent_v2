"""find_entity_by_name atomic tool."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from src.retrieval.atomic_tools import find_entity_by_name


@dataclass
class _Data:
    entities: list = field(default_factory=list)


class _Retriever:
    def __init__(self, entities):
        self._entities = entities
        self.seen = None

    async def afind_entities_by_name(self, query, *, limit=None):
        self.seen = (query, limit)
        return _Data(entities=self._entities)


@pytest.mark.asyncio
async def test_find_entity_by_name_returns_entities():
    r = _Retriever([{"entity_name": "Иванов Иван Иванович", "entity_type": "Person"}])
    res = await find_entity_by_name(r, query="Иванов", limit=7)
    obs = json.loads(res.observation)
    assert obs["entities"][0]["entity_name"] == "Иванов Иван Иванович"
    assert r.seen == ("Иванов", 7)
    assert res.sources == []


@pytest.mark.asyncio
async def test_find_entity_by_name_none_retriever():
    res = await find_entity_by_name(None, query="Иванов")
    assert json.loads(res.observation) == {"entities": []}
    assert res.sources == []

"""Unit tests for offline GDS-Leiden community detection (Search R6).

No live Neo4j / GDS here: a FAKE store records every ``structured_query``
call and returns canned rows for the Leiden-stream read.  We assert:

  * canned ``communityId`` rows are grouped into ``CommunityRef``s,
  * the ``min_size`` floor drops tiny communities,
  * the ``:Community`` MERGE + ``IN_COMMUNITY`` link writes were issued,
  * any store error → ``[]`` (fail-safe, no raise through the boundary).
"""

from __future__ import annotations

import pytest

from src.graph.communities import detect_communities


class _FakeStore:
    """Records Cypher calls; returns canned rows for the leiden-stream
    read (the query that RETURNs ``name``/``communityId``)."""

    def __init__(self, stream_rows, *, raise_on=None):
        self._stream_rows = stream_rows
        self._raise_on = raise_on
        self.calls: list[tuple[str, dict]] = []

    def structured_query(self, cypher, param_map=None):
        self.calls.append((cypher, param_map or {}))
        if self._raise_on and self._raise_on in cypher:
            raise RuntimeError("boom")
        # The leiden read is the one that returns communityId rows.
        if "communityId" in cypher and cypher.strip().upper().startswith(("CALL", "MATCH")):
            if "gds.leiden.stream" in cypher or "RETURN" in cypher and "communityId" in cypher:
                return self._stream_rows
        return []


@pytest.mark.asyncio
async def test_detect_groups_members_by_community_id():
    rows = [
        {"name": "Иванов", "communityId": 7},
        {"name": "Петров", "communityId": 7},
        {"name": "Сидоров", "communityId": 7},
        {"name": "Ромашка", "communityId": 2},
        {"name": "СтройИнвест", "communityId": 2},
        {"name": "ТехноСтрой", "communityId": 2},
    ]
    store = _FakeStore(rows)
    comms = await detect_communities(store, min_size=3)

    by_id = {c.community_id: c for c in comms}
    assert set(by_id) == {7, 2}
    assert sorted(by_id[7].members) == ["Иванов", "Петров", "Сидоров"]
    assert by_id[2].member_count == 3


@pytest.mark.asyncio
async def test_detect_drops_communities_below_min_size():
    rows = [
        {"name": "A", "communityId": 1},
        {"name": "B", "communityId": 1},
        {"name": "C", "communityId": 1},
        {"name": "D", "communityId": 9},  # singleton — dropped
    ]
    store = _FakeStore(rows)
    comms = await detect_communities(store, min_size=3)
    assert [c.community_id for c in comms] == [1]


@pytest.mark.asyncio
async def test_detect_issues_community_merge_writes():
    rows = [
        {"name": "A", "communityId": 5},
        {"name": "B", "communityId": 5},
        {"name": "C", "communityId": 5},
    ]
    store = _FakeStore(rows)
    await detect_communities(store, min_size=3, level=0)

    joined = "\n".join(c for c, _ in store.calls)
    # Idempotent node + link writes.
    assert "MERGE (c:Community" in joined
    assert "IN_COMMUNITY" in joined
    # member_count persisted on the node.
    assert "member_count" in joined


@pytest.mark.asyncio
async def test_detect_failsafe_on_store_error_returns_empty():
    store = _FakeStore([], raise_on="gds.leiden")
    comms = await detect_communities(store, min_size=3)
    assert comms == []


@pytest.mark.asyncio
async def test_detect_none_store_returns_empty():
    assert await detect_communities(None, min_size=3) == []

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
    assert set(by_id) == {"7", "2"}
    assert sorted(by_id["7"].members) == ["Иванов", "Петров", "Сидоров"]
    assert by_id["2"].member_count == 3


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
    assert [c.community_id for c in comms] == ["1"]


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
async def test_detect_prunes_prior_level_before_merges():
    """A rebuild must DETACH DELETE the prior level's communities (scoped
    to $level) BEFORE re-creating them, so ghost nodes/summaries from a
    renumbered Leiden run don't linger."""
    rows = [
        {"name": "A", "communityId": 5},
        {"name": "B", "communityId": 5},
        {"name": "C", "communityId": 5},
    ]
    store = _FakeStore(rows)
    await detect_communities(store, min_size=3, level=2)

    cyphers = [c for c, _ in store.calls]
    prune_idx = next(
        i for i, c in enumerate(cyphers)
        if "DETACH DELETE c" in c and "Community{level:$level}" in c.replace(" ", "")
    )
    first_merge_idx = next(
        i for i, c in enumerate(cyphers) if "MERGE (c:Community" in c
    )
    assert prune_idx < first_merge_idx
    # Scoped to the requested level only (parameterized, never global).
    _, prune_params = store.calls[prune_idx]
    assert prune_params == {"level": 2}


@pytest.mark.asyncio
async def test_detect_uses_per_call_projection_name():
    """The GDS projection name must be unique per call (not a module
    constant) so concurrent rebuilds don't drop each other's projection."""
    rows = [{"name": n, "communityId": 1} for n in ("A", "B", "C")]
    store = _FakeStore(rows)
    await detect_communities(store, min_size=3)

    project_calls = [c for c, _ in store.calls if "gds.graph.project" in c]
    drop_calls = [c for c, _ in store.calls if "gds.graph.drop" in c]
    assert project_calls and drop_calls
    # Per-call name carries the prefix + a random suffix.
    assert "'kb-communities-" in project_calls[0]
    # The drop targets the SAME per-call name used for the projection.
    name = project_calls[0].split("'")[1]
    assert all(f"'{name}'" in c for c in drop_calls)


@pytest.mark.asyncio
async def test_detect_failsafe_on_store_error_returns_empty():
    store = _FakeStore([], raise_on="gds.leiden")
    comms = await detect_communities(store, min_size=3)
    assert comms == []


@pytest.mark.asyncio
async def test_detect_none_store_returns_empty():
    assert await detect_communities(None, min_size=3) == []


def test_members_hash_order_insensitive():
    from src.graph.communities import members_hash
    assert members_hash(["B","A"]) == members_hash(["A","B"])
    assert len(members_hash(["A"])) == 64


def test_group_by_levels_maps_dendrogram_and_parents():
    from src.graph.communities import _group_by_levels
    # node → intermediateCommunityIds (finest..coarsest). 3 nodes, 2 levels.
    rows = [
        {"name":"a","ids":[10, 1]},
        {"name":"b","ids":[10, 1]},
        {"name":"c","ids":[11, 1]},
    ]
    levels = _group_by_levels(rows, min_size=1, max_levels=10)
    # level 0 = coarsest (id 1) holds a,b,c; level 1 (finer) has {a,b}=10, {c}=11
    l0 = {c.community_id: set(c.members) for c in levels if c.level == 0}
    l1 = {c.community_id: set(c.members) for c in levels if c.level == 1}
    assert l0 == {"1": {"a","b","c"}}
    assert l1 == {"10": {"a","b"}, "11": {"c"}}
    # parent of finer 10/11 is coarser 1
    assert all(c.parent_id == "1" for c in levels if c.level == 1)

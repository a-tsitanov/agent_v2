"""Stage-9 unit tests for the merge job.

Pure helpers cover the reusable logic.  Live Neo4j is exercised
manually via ``python -m scripts.merge_identifier_duplicates``.
"""

from __future__ import annotations

import importlib

import pytest

merge_mod = importlib.import_module("scripts.merge_identifier_duplicates")


@pytest.mark.parametrize(
    "name,etype,expected",
    [
        ("+7 (495) 234-56-78", "PhoneNumber", "+74952345678"),
        ("8 495 234-56-78", "PhoneNumber", "+74952345678"),
        ("+74952345678", "PhoneNumber", "+74952345678"),
        ("Bob@EXAMPLE.com", "Email", "bob@example.com"),
        ("7707083893", "INN", "7707083893"),
        ("15.03.2024", "DocumentDate", "2024-03-15"),
        ("№ дп-2024/178-К", "ContractNumber", "ДП-2024/178-К"),
    ],
)
def test_canonicalize_for_type(name, etype, expected) -> None:
    assert merge_mod.canonicalize_for_type(name, etype) == expected


def test_group_by_canonical_collapses_phone_variants() -> None:
    nodes = [
        ("+7 (495) 234-56-78", "PhoneNumber"),
        ("8 495 234 56 78", "PhoneNumber"),
        ("+74952345678", "PhoneNumber"),
    ]
    groups = merge_mod.group_by_canonical(nodes, frozenset({"PhoneNumber"}))
    assert len(groups) == 1
    assert set(groups[("PhoneNumber", "+74952345678")]) == {
        "+7 (495) 234-56-78", "8 495 234 56 78", "+74952345678",
    }


def test_group_by_canonical_skips_already_canonical_singleton() -> None:
    groups = merge_mod.group_by_canonical(
        [("+74952345678", "PhoneNumber")], frozenset({"PhoneNumber"}),
    )
    assert groups == {}


def test_group_by_canonical_filters_by_types() -> None:
    nodes = [
        ("Bob@EXAMPLE.com", "Email"),
        ("+7 (495) 234-56-78", "PhoneNumber"),
    ]
    groups = merge_mod.group_by_canonical(nodes, frozenset({"Email"}))
    assert set(groups.keys()) == {("Email", "bob@example.com")}


@pytest.mark.asyncio
async def test_apply_merges_dry_run_records_nothing() -> None:
    class _FakeStore:
        def __init__(self): self.calls = []
        def structured_query(self, q, p):
            self.calls.append((q, p))

    store = _FakeStore()
    groups = {("PhoneNumber", "+74952345678"): ["+7 (495) 234-56-78"]}
    summary = await merge_mod.apply_merges(store, groups, dry_run=True)
    assert summary == {"groups": 1, "merged_sources": 0, "errors": 0}
    assert store.calls == []


@pytest.mark.asyncio
async def test_apply_merges_real_run_invokes_cypher() -> None:
    class _FakeStore:
        def __init__(self): self.calls = []
        def structured_query(self, q, p):
            self.calls.append((q, p))
            return []

    store = _FakeStore()
    groups = {
        ("Email", "bob@example.com"): ["Bob@EXAMPLE.com", "BOB@example.com"],
    }
    summary = await merge_mod.apply_merges(store, groups, dry_run=False)
    assert summary["groups"] == 1
    assert summary["merged_sources"] == 2
    assert summary["errors"] == 0
    assert len(store.calls) == 1
    _, params = store.calls[0]
    assert params["target_name"] == "bob@example.com"
    assert "Bob@EXAMPLE.com" in params["source_names"]


@pytest.mark.asyncio
async def test_apply_merges_continues_on_individual_failure() -> None:
    class _FakeStore:
        def __init__(self): self.fail_next = True; self.calls = []
        def structured_query(self, q, p):
            self.calls.append((q, p))
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("boom")
            return []

    store = _FakeStore()
    groups = {
        ("Email", "bob@example.com"): ["Bob@EXAMPLE.com"],
        ("INN", "7707083893"): ["7707083893"],
    }
    summary = await merge_mod.apply_merges(store, groups, dry_run=False)
    assert summary["errors"] == 1
    assert summary["merged_sources"] == 0  # second group: 1 source equals target → no rename
    assert len(store.calls) == 2  # both attempted

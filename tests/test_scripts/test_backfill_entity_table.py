import pytest

from scripts.backfill_entity_table import backfill, build_page_query


def test_first_page_starts_at_the_index_head_and_is_ordered():
    """Both the WHERE range and ORDER BY are required, not alternatives:
    NebulaGraph shards vertices, so WHERE alone only orders within a
    shard/scan chunk — a live run with no ORDER BY covered min..max but
    silently skipped entities (loaded 3599 of 166241). `>= ""` (empty
    string sorts first) starts the range at the index head; `ORDER BY
    $-.name` gives the total order across shards, same as
    scripts/migrate_er_verdicts.py."""
    q = build_page_query(None, 500)
    assert 'WHERE `Entity`.name >= "" ' in q
    assert "ORDER BY $-.name" in q
    assert "LIMIT 500" in q


def test_resumed_page_filters_past_the_last_name():
    q = build_page_query("Кремль", 500)
    assert 'WHERE `Entity`.name > "Кремль"' in q
    assert "ORDER BY $-.name" in q


def test_page_query_never_uses_offset_pagination():
    """Offset pagination fails on the live store with StorageMemoryExceeded."""
    for q in (build_page_query(None, 500), build_page_query("x", 500)):
        assert "," not in q.split("LIMIT")[-1]


def test_page_query_escapes_quotes():
    q = build_page_query('ООО "Ромашка"', 10)
    assert '\\"' in q


class _StubStore:
    def __init__(self, names):
        self.rows = [{"vid": f"v{i}", "name": n, "label": "X",
                      "description": "", "mc": 1} for i, n in enumerate(sorted(names))]
    def structured_query(self, q):
        page = int(q.rsplit("LIMIT", 1)[1])
        after = None
        if "name > " in q:
            after = q.split('name > "', 1)[1].split('" ', 1)[0]
        rest = [r for r in self.rows if after is None or r["name"] > after]
        return rest[:page]


def test_backfill_copies_every_row_across_pages():
    seen = []
    store = _StubStore(["Аня", "Борис", "Вера", "Глеб", "Дима"])
    copied, last = backfill(store, page=2, sink=seen.extend)
    assert copied == 5
    assert last == "Дима"
    assert {r["name"] for r in seen} == {"Аня", "Борис", "Вера", "Глеб", "Дима"}


def test_backfill_empty_store():
    copied, last = backfill(_StubStore([]), page=10, sink=lambda rows: None)
    assert (copied, last) == (0, None)


# ── retry on a transient GraphMemoryExceeded-style refusal ──────────────
#
# ORDER BY makes graphd sort the matched set in memory, which can raise
# GraphMemoryExceeded (-2600) transiently under load. Mirrors
# scripts/migrate_er_verdicts.py's _FlakyStore / retry tests.


class _FlakyStore(_StubStore):
    def __init__(self, names, fail_times: int) -> None:
        super().__init__(names)
        self.remaining_failures = fail_times

    def structured_query(self, q):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RuntimeError("Used memory hits the high watermark(0.800000)")
        return super().structured_query(q)


def test_a_transient_store_refusal_is_retried(monkeypatch):
    monkeypatch.setattr("scripts.backfill_entity_table.time.sleep", lambda _s: None)
    store = _FlakyStore(["Аня", "Борис", "Вера"], fail_times=2)
    copied, last = backfill(store, page=10, retries=3, sink=lambda rows: None)
    assert copied == 3
    assert last == "Вера"
    assert store.remaining_failures == 0


def test_retries_are_bounded_and_the_error_surfaces(monkeypatch):
    """Retrying forever on a host that is genuinely out of memory would
    hang the backfill instead of reporting it."""
    monkeypatch.setattr("scripts.backfill_entity_table.time.sleep", lambda _s: None)
    store = _FlakyStore(["Аня"], fail_times=99)
    with pytest.raises(RuntimeError, match="high watermark"):
        backfill(store, page=10, retries=2, sink=lambda rows: None)

from scripts.backfill_entity_table import backfill, build_page_query


def test_first_page_has_no_range_filter():
    q = build_page_query(None, 500)
    assert "WHERE" not in q
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

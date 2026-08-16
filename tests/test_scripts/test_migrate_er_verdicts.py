"""The verdict-cache migration, tested without a live Nebula.

The query builder is the part that matters most: the wrong shape here is
exactly what fails on the live store.  Offset pagination was measured
failing with `StorageMemoryExceeded (-3600)` on 1.4M rows, so the
generated statement must be a KEY RANGE with a total order — anything
else silently works on a small space and dies on the real one.
"""

from __future__ import annotations

from scripts.migrate_er_verdicts import build_page_query, escape_ngql, migrate

# ── the page query ───────────────────────────────────────────────────


def test_first_page_has_no_range_filter():
    q = build_page_query(None, 500)
    assert "WHERE" not in q
    assert "ORDER BY $-.k" in q
    assert "LIMIT 500" in q


def test_resumed_page_filters_past_the_last_key():
    q = build_page_query("abc", 500)
    assert 'WHERE `ERVerdict`.er_key > "abc"' in q
    assert "ORDER BY $-.k" in q
    assert "LIMIT 500" in q


def test_page_query_never_uses_offset_pagination():
    """`| LIMIT <offset>, <n>` is the form that dies on the live store."""
    for q in (build_page_query(None, 500), build_page_query("k", 500)):
        assert "," not in q.split("LIMIT")[-1]


def test_page_query_escapes_quotes_in_the_key():
    """Keys are JSON text and contain quotes — a real one looks like
    `[["#tag", "Organization"], ["@other", "Organization"]]`.  Unescaped,
    the statement breaks at the first inner quote."""
    key = '[["#tag", "Organization"]]'
    q = build_page_query(key, 10)
    assert '\\"' in q
    # Everything after the opening quote of the literal up to its close
    # must be escaped, so the raw key must NOT appear verbatim.
    assert f'> "{key}"' not in q


def test_escape_handles_backslashes_before_quotes():
    assert escape_ngql(r'a\b"c') == r'a\\b\"c'


# ── the copy loop ────────────────────────────────────────────────────


class _StubStore:
    """Serves pages from a sorted key list, honouring the range filter."""

    def __init__(self, rows: dict[str, bool]) -> None:
        self.items = sorted(rows.items())
        self.queries: list[str] = []

    def structured_query(self, q: str):
        self.queries.append(q)
        page = int(q.rsplit("LIMIT", 1)[1])
        after = None
        if "er_key > " in q:
            after = q.split('er_key > "', 1)[1].split('" ', 1)[0]
            after = after.replace('\\"', '"').replace("\\\\", "\\")
        rest = [(k, v) for k, v in self.items if after is None or k > after]
        return [{"k": k, "s": v} for k, v in rest[:page]]


class _StubCache:
    def __init__(self) -> None:
        self.stored: dict[str, bool] = {}
        self.calls = 0

    def store_verdicts(self, entries: dict[str, bool]) -> None:
        self.calls += 1
        self.stored.update(entries)


def test_copies_every_row_across_pages():
    rows = {f"k{i:03d}": i % 2 == 0 for i in range(25)}
    store, cache = _StubStore(rows), _StubCache()
    copied, last_key = migrate(store, cache, page=10)
    assert copied == 25
    assert cache.stored == rows
    assert last_key == "k024"


def test_writes_one_batch_per_page():
    """Per-page batching, not per-row — 1.4M single-row round trips would
    take hours."""
    rows = {f"k{i:03d}": True for i in range(25)}
    store, cache = _StubStore(rows), _StubCache()
    migrate(store, cache, page=10)
    assert cache.calls == 3


def test_preserves_both_verdict_values():
    rows = {"a": True, "b": False}
    store, cache = _StubStore(rows), _StubCache()
    migrate(store, cache, page=10)
    assert cache.stored == {"a": True, "b": False}


def test_a_short_page_does_not_end_the_migration():
    """Termination is on an EMPTY page, never on a short one. A page that
    comes back short for any reason other than exhaustion — this is a
    distributed store — would otherwise silently drop the tail."""
    rows = {f"k{i:03d}": True for i in range(30)}
    store = _StubStore(rows)
    real_query = store.structured_query
    seen: list[int] = []

    def _short_second_page(q: str):
        out = real_query(q)
        seen.append(len(out))
        # Serve page 2 short (3 rows instead of 10) while more remain.
        return out[:3] if len(seen) == 2 else out

    store.structured_query = _short_second_page
    cache = _StubCache()
    copied, _ = migrate(store, cache, page=10)
    assert copied == 30
    assert cache.stored == rows


def test_empty_store_copies_nothing_and_does_not_raise():
    store, cache = _StubStore({}), _StubCache()
    copied, last_key = migrate(store, cache, page=10)
    assert (copied, last_key) == (0, None)
    assert cache.calls == 0


def test_resumes_from_a_given_key():
    rows = {f"k{i:03d}": True for i in range(10)}
    store, cache = _StubStore(rows), _StubCache()
    copied, _ = migrate(store, cache, page=100, start_after="k004")
    assert copied == 5
    assert set(cache.stored) == {f"k{i:03d}" for i in range(5, 10)}


def test_limit_pages_stops_early_and_returns_a_usable_resume_point():
    rows = {f"k{i:03d}": True for i in range(100)}
    store, cache = _StubStore(rows), _StubCache()
    copied, last_key = migrate(store, cache, page=10, limit_pages=2)
    assert copied == 20
    assert last_key == "k019"
    # The resume point must actually resume, not re-copy or skip.
    copied2, _ = migrate(store, _StubCache(), page=10, start_after=last_key)
    assert copied2 == 80


def test_dry_run_reads_but_does_not_write():
    rows = {f"k{i:03d}": True for i in range(15)}
    store, cache = _StubStore(rows), _StubCache()
    copied, _ = migrate(store, cache, page=10, dry_run=True)
    assert copied == 15
    assert cache.calls == 0
    assert cache.stored == {}

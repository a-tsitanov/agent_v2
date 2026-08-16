"""Copy the ER verdict cache out of the graph and into Postgres.

One-off, resumable, idempotent.  See
`docs/superpowers/plans/2026-08-16-er-verdict-cache-postgres.md`.

Enumeration uses KEY-RANGE pagination, not offsets.  Measured on the
live store: `LOOKUP ON ERVerdict … | LIMIT 100000, 5` fails with
`StorageMemoryExceeded (-3600)`, because the offset form materialises
everything up to the offset.  A keyed page does not.

That also means `er_verdict_key_idx` — the index nothing reads at query
time — MUST NOT be dropped until this migration has finished and been
verified.  It is the only way to enumerate these vertices at all.

Usage:

    uv run python -m scripts.migrate_er_verdicts --page 2000
    uv run python -m scripts.migrate_er_verdicts --start-after '<last key>'
    uv run python -m scripts.migrate_er_verdicts --dry-run --limit-pages 2
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any


def escape_ngql(value: str) -> str:
    """Escape a string for inline use inside a double-quoted nGQL literal.

    Verdict keys are JSON text and really do contain quotes — a live key
    looks like `[["#ОсторожноСобчак", "Organization"], ["@balkanar",
    "Organization"]]`.  Unescaped, the generated statement breaks at the
    first inner quote.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_page_query(last_key: str | None, page: int) -> str:
    """One page of verdicts, ordered by key, after `last_key`.

    `ORDER BY` is what makes the range resumable: without a total order
    on the key, "everything after X" has no meaning.
    """
    where = (
        f'WHERE `ERVerdict`.er_key > "{escape_ngql(last_key)}" '
        if last_key is not None
        else ""
    )
    return (
        f"LOOKUP ON `ERVerdict` {where}"
        "YIELD `ERVerdict`.er_key AS k, `ERVerdict`.same AS s "
        f"| ORDER BY $-.k | LIMIT {int(page)}"
    )


def migrate(
    store: Any,
    cache: Any,
    *,
    page: int = 2000,
    start_after: str | None = None,
    limit_pages: int | None = None,
    dry_run: bool = False,
    progress: Any = None,
) -> tuple[int, str | None]:
    """Copy every verdict into `cache`.  Returns `(copied, last_key)`.

    `last_key` is the resume point: pass it back as `start_after` after
    an interruption.  Writes are upserts, so re-running over already
    copied rows is harmless.
    """
    copied = 0
    last_key = start_after
    pages = 0
    started = time.time()

    while True:
        rows = store.structured_query(build_page_query(last_key, page)) or []
        rows = [r for r in rows if isinstance(r, dict) and r.get("k") is not None]
        if not rows:
            break

        entries = {str(r["k"]): bool(r["s"]) for r in rows}
        if not dry_run:
            cache.store_verdicts(entries)
        copied += len(entries)
        last_key = str(rows[-1]["k"])
        pages += 1

        if progress is not None:
            progress(
                f"page {pages}: +{len(entries)} rows, {copied} total, "
                f"{time.time() - started:.0f}s elapsed"
            )

        # Deliberately NOT stopping on a short page. It is tempting —
        # under `ORDER BY … LIMIT n` a short page should mean the range is
        # exhausted — but "should" is doing real work there: this is a
        # distributed store, and a page that comes back short for any
        # other reason would silently truncate the migration and lose the
        # tail of 1.4M rows. Termination is on an EMPTY page only, which
        # costs one extra LOOKUP per run and cannot mistake a short read
        # for the end.
        if limit_pages is not None and pages >= limit_pages:
            break

    return copied, last_key


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page", type=int, default=2000)
    ap.add_argument("--start-after", default=None)
    ap.add_argument("--limit-pages", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    from src.graph.er_graph_ops import PostgresERVerdictCache
    from src.graph.store import build_graph_store

    store = build_graph_store()
    cache = PostgresERVerdictCache()
    if not args.dry_run:
        cache.ensure_verdict_schema()

    def _say(msg: str) -> None:
        print(msg, flush=True)

    try:
        copied, last_key = migrate(
            store,
            cache,
            page=args.page,
            start_after=args.start_after,
            limit_pages=args.limit_pages,
            dry_run=args.dry_run,
            progress=_say,
        )
    except Exception as exc:
        # Print the resume point BEFORE failing: without it the next run
        # restarts from the beginning of a 1.4M-row scan.
        print(f"FAILED after the page starting at {args.start_after!r}: {exc}")
        print("resume with --start-after '<the last key printed above>'")
        sys.exit(1)

    print(f"copied={copied}  last_key={last_key!r}")


if __name__ == "__main__":
    main()

"""One-off fill of the Postgres `entity` table from the graph.

Resumable, idempotent. Key-range pagination over `name` (NOT offset —
offset fails on the live store with StorageMemoryExceeded, per the ER
verdict migration). Reads by index, never a full scan.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from typing import Any


def escape_ngql(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_page_query(last_name: str | None, page: int) -> str:
    """One page of entities, in name order, after `last_name`.

    No `ORDER BY`: on the live store `LOOKUP | ORDER BY $-.name | LIMIT n`
    sorts the ENTIRE ~163k-row scan in graphd memory before the first page
    can be cut, which is what blew up as GraphMemoryExceeded (-2600) even
    at page=50. An index-range `LOOKUP ... WHERE name >= "..."` already
    returns rows in index (name-sorted) order — verified live — so the
    WHERE range does double duty: it is both the resume filter and the
    only source of ordering. A bare `LOOKUP` with no WHERE returns
    unsorted scan order, so the first page still needs `>= ""` (the empty
    string sorts before every name) rather than dropping the WHERE too.
    """
    last = last_name if last_name is not None else ""
    op = ">" if last_name is not None else ">="
    where = f'WHERE `Entity`.name {op} "{escape_ngql(last)}" '
    return (
        f"LOOKUP ON `Entity` {where}"
        "YIELD id(vertex) AS vid, `Entity`.name AS name, "
        "`Entity`.label AS label, `Entity`.description AS description, "
        "`Entity`.mention_count AS mc "
        f"| LIMIT {int(page)}"
    )


def backfill(
    store: Any, *, page: int = 2000, start_after: str | None = None,
    sink: Callable[[list[dict]], None] | None = None,
    progress: Any = None,
) -> tuple[int, str | None]:
    """Copy every entity into the sink. Returns (copied, last_name)."""
    from src.graph.entity_table import mirror_entities
    write = sink if sink is not None else mirror_entities

    copied, last_name, pages = 0, start_after, 0
    while True:
        rows = store.structured_query(build_page_query(last_name, page)) or []
        rows = [r for r in rows if isinstance(r, dict) and r.get("name")]
        if not rows:
            break
        write([
            {"vid": r["vid"], "name": r["name"], "label": r.get("label") or "",
             "description": r.get("description") or "",
             "mention_count": int(r.get("mc") or 1)}
            for r in rows
        ])
        copied += len(rows)
        last_name = str(rows[-1]["name"])
        pages += 1
        if progress:
            progress(f"page {pages}: +{len(rows)}, {copied} total, last={last_name!r}")
    return copied, last_name


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page", type=int, default=2000)
    ap.add_argument("--start-after", default=None)
    args = ap.parse_args()
    from src.graph.store import build_graph_store
    store = build_graph_store()
    try:
        copied, last = backfill(
            store, page=args.page, start_after=args.start_after,
            progress=lambda m: print(m, flush=True),
        )
    except Exception as exc:
        print(f"FAILED: {exc}")
        print("resume with --start-after '<last name printed above>'")
        sys.exit(1)
    print(f"copied={copied} last_name={last!r}")


if __name__ == "__main__":
    main()

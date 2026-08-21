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
    where = (
        f'WHERE `Entity`.name > "{escape_ngql(last_name)}" '
        if last_name is not None else ""
    )
    return (
        f"LOOKUP ON `Entity` {where}"
        "YIELD id(vertex) AS vid, `Entity`.name AS name, "
        "`Entity`.label AS label, `Entity`.description AS description, "
        "`Entity`.mention_count AS mc "
        f"| ORDER BY $-.name | LIMIT {int(page)}"
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

"""One-off fill of the Postgres `entity` table from the graph.

Resumable, idempotent. Key-range pagination over `name` (NOT offset —
offset fails on the live store with StorageMemoryExceeded, per the ER
verdict migration). Reads by index, never a full scan.
"""

from __future__ import annotations

import argparse
import ast
import sys
import time
from collections.abc import Callable
from typing import Any


def escape_ngql(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_page_query(last_name: str | None, page: int) -> str:
    """One page of entities, in name order, after `last_name`.

    NebulaGraph shards vertices, so the WHERE range alone (no ORDER BY)
    only orders WITHIN a shard/scan chunk, not globally — measured live:
    a full run with no ORDER BY covered min..max but silently skipped
    entities (e.g. "Московия"), loading 3599 of 166241. `ORDER BY
    $-.name` is what gives a total order ACROSS shards, exactly as
    `scripts/migrate_er_verdicts.py` documents for its own key range.

    The WHERE range is still required alongside it — it is the resume
    cursor (first page `>= ""`, resumed pages `> "<last>"`) — but it does
    NOT replace the ORDER BY; the two are complementary, not alternatives.
    Sorting the matched set in graphd memory is what can raise
    `GraphMemoryExceeded (-2600)` under load; `_read_page` retries that
    transient failure instead of dropping the ORDER BY.
    """
    last = last_name if last_name is not None else ""
    op = ">" if last_name is not None else ">="
    where = f'WHERE `Entity`.name {op} "{escape_ngql(last)}" '
    return (
        f"LOOKUP ON `Entity` {where}"
        "YIELD id(vertex) AS vid, `Entity`.name AS name, "
        "`Entity`.label AS label, `Entity`.description AS description, "
        "`Entity`.mention_count AS mc "
        f"| ORDER BY $-.name | LIMIT {int(page)}"
    )


def _read_page(
    store: Any,
    last_name: str | None,
    page: int,
    *,
    retries: int = 8,
    progress: Any = None,
) -> list:
    """One page, retried on a transient store refusal.

    `ORDER BY` makes graphd sort the matched set in memory, which can hit
    `GraphMemoryExceeded (-2600)` or the memory high-watermark transiently
    under load. The same page (same `last_name`) usually succeeds a few
    seconds later — copied from `scripts/migrate_er_verdicts.py::_read_page`.
    """
    delay = 5.0
    for attempt in range(retries + 1):
        try:
            return store.structured_query(build_page_query(last_name, page)) or []
        except Exception as exc:
            if attempt >= retries:
                raise
            if progress is not None:
                progress(
                    f"  page read failed ({exc}); retry {attempt + 1}/{retries} "
                    f"in {delay:.0f}s"
                )
            time.sleep(delay)
            delay *= 2
    return []


def backfill(
    store: Any, *, page: int = 2000, start_after: str | None = None,
    retries: int = 8,
    sink: Callable[[list[dict]], None] | None = None,
    progress: Any = None,
) -> tuple[int, str | None]:
    """Copy every entity into the sink. Returns (copied, last_name)."""
    from src.graph.entity_table import mirror_entities
    write = sink if sink is not None else mirror_entities

    copied, last_name, pages = 0, start_after, 0
    while True:
        rows = _read_page(store, last_name, page, retries=retries, progress=progress)
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
    ap.add_argument("--retries", type=int, default=8)
    args = ap.parse_args()
    from src.graph.store import build_graph_store
    store = build_graph_store()

    # Every progress line carries `last=…`; keep the newest so a failure
    # can name the exact resume point instead of the (possibly stale)
    # --start-after the run was started with. Same pattern as
    # migrate_er_verdicts.main.
    resume_from: str | None = args.start_after

    def _say(msg: str) -> None:
        nonlocal resume_from
        marker = "last="
        if marker in msg:
            resume_from = ast.literal_eval(msg.split(marker, 1)[1])
        print(msg, flush=True)

    try:
        copied, last = backfill(
            store, page=args.page, start_after=args.start_after,
            retries=args.retries,
            progress=_say,
        )
    except Exception as exc:
        print(f"FAILED: {exc}")
        print(f"resume with:  --start-after {resume_from!r}")
        sys.exit(1)
    print(f"copied={copied} last_name={last!r}")


if __name__ == "__main__":
    main()

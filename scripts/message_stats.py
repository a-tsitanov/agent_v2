"""CLI for processed-message statistics — a thin wrapper over the same
AsyncPostgres aggregation methods the /stats endpoints use.

Usage::

    python -m scripts.message_stats channels [--since 2026-07-01] [--until 2026-07-23]
    python -m scripts.message_stats groups
    python -m scripts.message_stats timeline [--date-field doc_date]
                 [--group-by channel] [--channel acme] [--group news]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.storage.postgres import DOC_STATUSES, AsyncPostgres

_COLS = ("key", "total", *DOC_STATUSES)


def format_status_rows(rows: list[dict]) -> str:
    """Render status_counts_by output as an aligned text table."""
    widths = {c: len(c) for c in _COLS}
    for r in rows:
        for c in _COLS:
            widths[c] = max(widths[c], len(str(r.get(c, ""))))
    header = "  ".join(c.ljust(widths[c]) for c in _COLS)
    lines = [header, "  ".join("-" * widths[c] for c in _COLS)]
    for r in rows:
        lines.append("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in _COLS))
    return "\n".join(lines)


def format_timeline(buckets: list[dict]) -> str:
    if buckets and "key" in buckets[0]:
        return "\n".join(f"{b['day']}  {b['key']}  {b['count']}" for b in buckets)
    return "\n".join(f"{b['day']}  {b['count']}" for b in buckets)


async def _run(args: argparse.Namespace) -> None:
    pg = AsyncPostgres(settings.postgres.dsn)
    if args.cmd == "channels":
        rows = await pg.status_counts_by(
            "source_channel", since=args.since, until=args.until,
        )
        print(format_status_rows(rows))
    elif args.cmd == "groups":
        rows = await pg.status_counts_by(
            "source_group", since=args.since, until=args.until,
        )
        print(format_status_rows(rows))
    elif args.cmd == "timeline":
        buckets = await pg.timeline_counts(
            date_field=args.date_field, group_by=args.group_by,
            channel=args.channel, group=args.group,
            since=args.since, until=args.until,
        )
        print(format_timeline(buckets))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="message_stats")
    sub = parser.add_subparsers(dest="cmd", required=True)

    for name in ("channels", "groups"):
        p = sub.add_parser(name)
        p.add_argument("--since")
        p.add_argument("--until")

    t = sub.add_parser("timeline")
    t.add_argument("--date-field", dest="date_field", default="created_at",
                   choices=["created_at", "doc_date"])
    t.add_argument("--group-by", dest="group_by", default=None,
                   choices=["channel", "group"])
    t.add_argument("--channel", default=None)
    t.add_argument("--group", default=None)
    t.add_argument("--since")
    t.add_argument("--until")

    args = parser.parse_args(argv)
    # channels/groups have no timeline-only attrs; default them so _run is uniform.
    for attr in ("date_field", "group_by", "channel", "group"):
        if not hasattr(args, attr):
            setattr(args, attr, None)
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()

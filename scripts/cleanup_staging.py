"""Remove orphaned ``kb-staging/{workflow_run_id}/`` prefixes from MinIO.

Workflows clean their own staging blobs from ``finalize`` /
``mark_failed``.  Anything left behind belongs to a workflow that died
before either of those activities ran (worker OOM during heavy LLM
stage, infra blip, manual cancellation).  Sweep them so the bucket
doesn't grow unbounded.

Usage::

    uv run python -m scripts.cleanup_staging                 # 24h threshold
    uv run python -m scripts.cleanup_staging --hours 48      # custom
    uv run python -m scripts.cleanup_staging --dry-run       # just report

Schedule daily via cron::

    0 3 * * *  cd /opt/kb-llamaindex && uv run python -m scripts.cleanup_staging
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loguru import logger

from src.workflow.staging import build_staging_store


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Age threshold in hours (default: 24).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List orphans without deleting.",
    )
    args = parser.parse_args()

    store = build_staging_store()

    if args.dry_run:
        orphans = store.list_orphan_runs(older_than_hours=args.hours)
        logger.info(
            "dry-run  threshold={h}h  orphans={n}", h=args.hours, n=len(orphans),
        )
        for run_id in orphans:
            print(run_id)
        return 0

    deleted = store.cleanup_orphans(older_than_hours=args.hours)
    logger.info(
        "cleanup done  threshold={h}h  deleted={n}",
        h=args.hours, n=len(deleted),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

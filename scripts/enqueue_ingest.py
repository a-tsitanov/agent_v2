"""Enqueue an ingest task directly, bypassing the API.

Useful when API isn't running but worker + broker are: writes the
Postgres row, fires the taskiq task, polls for completion.

Usage::

    uv run python -m scripts.enqueue_ingest /tmp/medical_small.txt
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings  # noqa: E402
from src.ingestion.tasks import broker, process_document  # noqa: E402
from src.storage.postgres import AsyncPostgres  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("file", type=Path)
    p.add_argument("--department", default="cli")
    p.add_argument("--timeout", type=float, default=900.0)
    return p.parse_args()


async def main() -> int:
    args = _parse_args()
    if not args.file.is_file():
        print(f"file not found: {args.file}", file=sys.stderr)
        return 1

    doc_id = uuid.uuid4()
    upload_dir = Path(settings.api.upload_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)
    target = upload_dir / f"{doc_id}_{args.file.name}"
    shutil.copy2(args.file, target)
    print(f"copied → {target}")

    pg = AsyncPostgres()
    await pg.insert_pending(
        doc_id, str(target),
        department=args.department,
        doc_type=target.suffix.lstrip("."),
    )

    await broker.startup()
    try:
        await process_document.kiq(str(doc_id), str(target))
        print(f"enqueued job_id={doc_id}")
    finally:
        await broker.shutdown()

    # poll
    t0 = time.monotonic()
    last = None
    while True:
        row = await pg.get(doc_id)
        if row.status != last:
            print(f"  [{int(time.monotonic() - t0):4d}s]  status={row.status}  "
                  f"err={row.error or '-'}")
            last = row.status
        if row.status in ("completed", "failed"):
            return 0 if row.status == "completed" else 2
        if time.monotonic() - t0 > args.timeout:
            print("TIMEOUT", file=sys.stderr)
            return 3
        await asyncio.sleep(3)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

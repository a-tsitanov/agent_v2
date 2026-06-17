"""Live-update the ingest admission ceiling K (max_inflight) on the running
``ingest-scheduler`` singleton WITHOUT terminating it.

Usage: uv run python -m scripts.set_admission <N>
(Changing INGEST_ADMISSION_MAX_INFLIGHT in .env only affects a FRESH scheduler;
this signals the live one. Run after editing .env to apply immediately.)
"""
from __future__ import annotations

import asyncio
import sys

from loguru import logger

from src.utils.logging import configure_logging
from src.workflow.client import get_temporal_client
from src.workflow.ingest_scheduler import IngestSchedulerWorkflow


async def _main(n: int) -> None:
    client = await get_temporal_client()
    handle = client.get_workflow_handle("ingest-scheduler")
    await handle.signal(IngestSchedulerWorkflow.set_max_inflight, n)
    logger.info("signalled ingest-scheduler  set_max_inflight={n}", n=n)


def main() -> int:
    configure_logging()
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("usage: python -m scripts.set_admission <N>", file=sys.stderr)
        return 2
    asyncio.run(_main(int(sys.argv[1])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

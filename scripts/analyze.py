"""Run one analytical query against the knowledge graph.

Usage::

    python -m scripts.analyze "Сколько организаций в графе?"
    python -m scripts.analyze "Кто чаще всего упоминается с Ромашкой?" --top-n 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from temporalio.common import WorkflowIDReusePolicy

from src.analytics.contracts import AnalyzeParams
from src.config import settings
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow
from src.workflow.client import get_temporal_client


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()

    client = await get_temporal_client()
    handle = await client.start_workflow(
        AnalyticalQueryWorkflow.run,
        AnalyzeParams(query=args.query, top_n=args.top_n),
        id=f"cli-analyze-{uuid.uuid4().hex}",
        task_queue=settings.temporal.search_task_queue,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )
    outcome = await handle.result()
    print(outcome.answer)
    print("\n--- provenance ---")
    print(json.dumps(outcome.provenance.model_dump(), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())

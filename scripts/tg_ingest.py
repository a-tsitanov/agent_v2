"""TG → ingest test harness: read last-N channel messages (Telethon) and
enqueue each via POST /api/v1/ingest (which uploads to MinIO + publishes to
the rabbit queue). One-shot backfill, text-only, document_date = post date.

Runbook:
  1. docker compose --profile rabbitmq up -d rabbitmq
  2. export INGEST_QUEUE_BACKEND=rabbitmq RABBITMQ_URL=amqp://guest:guest@localhost:5672/
     export RABBITMQ_QUEUES=<name>
  3. uv run python -m src.ingest_queue.consumer        # queue → DocumentIngestWorkflow
  4. uv run uvicorn src.api.main:app --port 8000       # the API
  5. TG_API_ID=… TG_API_HASH=… uv run python -m scripts.tg_ingest \
       --channels @foo --limit 20 [--queue <name>] [--api-key dev-local-key]

TG_API_ID / TG_API_HASH come from https://my.telegram.org. First run does an
interactive Telethon login (phone + code) and writes the session file.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from loguru import logger


def _message_to_doc(msg: Any, channel: str) -> tuple[str, str, str] | None:
    """Map a Telethon message → (filename, text, document_date), or None if empty."""
    text = (getattr(msg, "message", None) or "").strip()
    if not text:
        return None
    filename = f"tg_{channel.lstrip('@')}_{msg.id}.txt"
    document_date = msg.date.date().isoformat()
    return filename, text, document_date


async def post_ingest(
    http: Any,
    api_base: str,
    api_key: str,
    filename: str,
    text: str,
    document_date: str,
    queue: str | None,
) -> bool:
    """POST one document to /api/v1/ingest (multipart). True on 2xx; fail-soft."""
    data: dict[str, str] = {"document_date": document_date}
    if queue:
        data["queue"] = queue
    try:
        resp = await http.post(
            f"{api_base}/api/v1/ingest",
            headers={"X-API-Key": api_key},
            files={"file": (filename, text.encode("utf-8"), "text/plain")},
            data=data,
        )
        return 200 <= resp.status_code < 300
    except Exception as exc:
        logger.warning("post_ingest failed file={f}: {e}", f=filename, e=exc)
        return False


async def read_and_enqueue(
    tg_client: Any,
    http: Any,
    *,
    channels: list[str],
    limit: int,
    api_base: str,
    api_key: str,
    queue: str | None,
) -> Counter:
    """Backfill: read last-`limit` messages per channel (oldest→newest) and enqueue."""
    tally: Counter = Counter()
    for channel in channels:
        async for msg in tg_client.iter_messages(channel, limit=limit, reverse=True):
            doc = _message_to_doc(msg, channel)
            if doc is None:
                tally["skipped"] += 1
                continue
            filename, text, document_date = doc
            ok = await post_ingest(http, api_base, api_key, filename, text, document_date, queue)
            tally["sent" if ok else "failed"] += 1
    logger.info("tg_ingest tally: {t}", t=dict(tally))
    return tally


def main() -> int:
    import argparse
    import asyncio
    import os

    p = argparse.ArgumentParser(description="Backfill TG channel messages into the ingest queue.")
    p.add_argument("--channels", required=True, help="comma-separated, e.g. @a,@b")
    p.add_argument("--limit", type=int, default=50, help="messages per channel")
    p.add_argument("--queue", default=None, help="target ingest queue (rabbitmq backend)")
    p.add_argument("--api-base", default="http://localhost:8000")
    p.add_argument("--api-key", default=os.environ.get("KB_API_KEY", "dev-local-key"))
    p.add_argument("--session", default=".tg_ingest.session")
    args = p.parse_args()

    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    channels = [c.strip() for c in args.channels.split(",") if c.strip()]

    async def _run() -> None:
        import httpx
        from telethon import TelegramClient

        async with (
            TelegramClient(args.session, api_id, api_hash) as tg,
            httpx.AsyncClient(timeout=30.0) as http,
        ):
            await read_and_enqueue(
                tg,
                http,
                channels=channels,
                limit=args.limit,
                api_base=args.api_base,
                api_key=args.api_key,
                queue=args.queue,
            )

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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

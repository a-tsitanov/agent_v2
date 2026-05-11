"""Ingest the Medical benchmark corpus through `/api/v1/ingest`.

Converts `tests/eval/corpora/medical/medical.json` (which wraps the
text in a JSON `context` field — not directly ingestable since
SimpleDirectoryReader would treat the JSON braces as content) into
a plain `.txt` file, uploads it, then polls the job status until
completed or failed.

Usage::

    uv run python -m scripts.ingest_medical
    uv run python -m scripts.ingest_medical --api-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from tests.eval.medical_fixture import MEDICAL_CORPUS_PATH  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api-url", default="http://localhost:8000")
    p.add_argument("--api-key", default="dev-local-key")
    p.add_argument("--department", default="medical")
    p.add_argument(
        "--poll-interval", type=float, default=2.0,
        help="seconds between status polls",
    )
    p.add_argument(
        "--timeout", type=float, default=900.0,
        help="max seconds to wait for status=completed",
    )
    return p.parse_args()


def prepare_txt() -> Path:
    """Materialize medical.json → medical.txt in /tmp for upload."""
    payload = json.loads(MEDICAL_CORPUS_PATH.read_text())
    text = payload["context"]
    target = Path("/tmp/medical_corpus.txt")
    target.write_text(text, encoding="utf-8")
    print(f"prepared {target}  ({len(text):,} chars)")
    return target


def main() -> int:
    args = _parse_args()
    txt_path = prepare_txt()

    with httpx.Client(base_url=args.api_url, timeout=60.0) as client:
        # ── upload ────────────────────────────────────────────────
        with txt_path.open("rb") as fh:
            resp = client.post(
                "/api/v1/ingest",
                headers={"X-API-Key": args.api_key},
                files={"file": (txt_path.name, fh, "text/plain")},
                data={"department": args.department},
            )
        resp.raise_for_status()
        job_id = resp.json()["job_id"]
        print(f"enqueued job_id={job_id}")

        # ── poll ─────────────────────────────────────────────────
        t0 = time.monotonic()
        last_status = None
        while True:
            r = client.get(
                f"/api/v1/ingest/{job_id}",
                headers={"X-API-Key": args.api_key},
            )
            r.raise_for_status()
            row = r.json()
            status = row["status"]
            if status != last_status:
                print(
                    f"  [{int(time.monotonic() - t0):4d}s]  status={status}  "
                    f"path={row.get('path','')}  err={row.get('error') or '-'}"
                )
                last_status = status
            if status == "completed":
                print("done.")
                return 0
            if status == "failed":
                print(f"FAILED: {row.get('error')}", file=sys.stderr)
                return 1
            if time.monotonic() - t0 > args.timeout:
                print(f"TIMEOUT after {args.timeout}s waiting for completion",
                      file=sys.stderr)
                return 2
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    sys.exit(main())

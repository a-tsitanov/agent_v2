"""Benchmark: LLM extraction latency with thinking ON vs OFF.

Phase-0 cheap win for high-volume ingest.  The extraction role makes one
LLM call per chunk (the dominant ingest cost).  Qwen3/gemma-class
"thinking" models emit a `<think>...</think>` block that the pipeline
then *throws away* (`src.retrieval._common.strip_thinking` is applied in
extraction/merge/ER) — so those tokens are pure decode-time waste on the
ingest path.  Sending `extra_body={"think": false}` (the lever added in
`src.retrieval.llm` / `LiteLLMSettings.extra_body`) suppresses them.

This script measures the real win against the CONFIGURED LiteLLM backend
so you can decide per the project's "benchmark before adopting" rule —
it is NOT a unit test (needs a live thinking-model endpoint).

Run:
    uv run python -m tests.eval.scale.bench_think                 # defaults
    uv run python -m tests.eval.scale.bench_think --n 10 --role extraction

It does nothing destructive (read-only chat calls).  Exits non-zero with
a clear message if the backend is unreachable.
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from pathlib import Path

from llama_index.core.llms import ChatMessage

from src.config import settings
from src.retrieval._common import strip_thinking
from src.retrieval.llm import _build

_SAMPLE = Path(__file__).parents[2] / "test_ingestion" / "fixtures" / "sample.txt"

_EXTRACT_INSTRUCTION = (
    "Extract every named entity (people, organizations, locations, "
    "products, identifiers) and the relations between them from the text "
    "below. Return one entity or relation per line. Be exhaustive.\n\n"
    "TEXT:\n{chunk}"
)


def _load_chunk(max_chars: int) -> str:
    if _SAMPLE.exists():
        return _SAMPLE.read_text(encoding="utf-8", errors="ignore")[:max_chars]
    # Fallback synthetic chunk so the bench still runs without the fixture.
    return (
        "Acme Corp, headquartered in Berlin, signed a contract with "
        "Globex Ltd on 2024-03-01. CEO Jane Doe (passport 1234567890) "
        "approved the deal. Contact: +49 30 1234567."
    ) * (max_chars // 160 + 1)


async def _timed_call(llm, chunk: str) -> tuple[float, str]:
    msg = ChatMessage(role="user", content=_EXTRACT_INSTRUCTION.format(chunk=chunk))
    t0 = time.perf_counter()
    resp = await llm.achat([msg])
    dt = time.perf_counter() - t0
    return dt, (resp.message.content or "")


async def _run_variant(model: str, extra_body, chunk: str, n: int, warmup: int):
    llm = _build(model, extra_body)
    # Warm up (model load / first-token cost) — not counted.
    for _ in range(warmup):
        await _timed_call(llm, chunk)
    lats: list[float] = []
    out_chars: list[int] = []
    think_chars: list[int] = []
    for _ in range(n):
        dt, content = await _timed_call(llm, chunk)
        lats.append(dt)
        out_chars.append(len(content))
        think_chars.append(len(content) - len(strip_thinking(content)))
    return lats, out_chars, think_chars


def _summary(lats: list[float]) -> dict:
    s = sorted(lats)
    return {
        "median": statistics.median(s),
        "mean": statistics.fmean(s),
        "p90": s[min(len(s) - 1, int(0.9 * len(s)))],
    }


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=5, help="timed calls per variant")
    ap.add_argument("--warmup", type=int, default=1, help="untimed warmup calls")
    ap.add_argument("--role", default="extraction", help="LLM role to resolve the model")
    ap.add_argument("--max-chars", type=int, default=4000, help="chunk size")
    args = ap.parse_args()

    cfg = settings.litellm
    model = cfg.model_for(args.role)
    chunk = _load_chunk(args.max_chars)

    print(f"backend : {cfg.base_url}")
    print(f"model   : {model}  (role={args.role})")
    print(f"chunk   : {len(chunk)} chars   calls/variant: {args.n} (+{args.warmup} warmup)\n")

    try:
        on_lats, on_out, on_think = await _run_variant(
            model, {}, chunk, args.n, args.warmup
        )
        off_lats, off_out, off_think = await _run_variant(
            model, {"think": False}, chunk, args.n, args.warmup
        )
    except Exception as exc:
        print(f"ERROR: backend call failed — is LiteLLM at {cfg.base_url} up "
              f"and serving a thinking model? ({exc})", file=sys.stderr)
        return 1

    on, off = _summary(on_lats), _summary(off_lats)
    speedup = on["median"] / off["median"] if off["median"] else float("nan")

    def _avg(xs: list[int]) -> float:
        return statistics.fmean(xs) if xs else 0.0

    print(f"{'variant':<18}{'median s':>10}{'mean s':>10}{'p90 s':>10}{'out chars':>12}{'think chars':>14}")
    print(f"{'think ON (default)':<18}{on['median']:>10.2f}{on['mean']:>10.2f}{on['p90']:>10.2f}"
          f"{_avg(on_out):>12.0f}{_avg(on_think):>14.0f}")
    print(f"{'think OFF':<18}{off['median']:>10.2f}{off['mean']:>10.2f}{off['p90']:>10.2f}"
          f"{_avg(off_out):>12.0f}{_avg(off_think):>14.0f}")
    print(f"\nspeedup (median ON/OFF): {speedup:.2f}x"
          f"   — think tokens are stripped on the ingest path, so this is pure waste when ON.")
    print("To activate: LITELLM_EXTRA_BODY='{\"think\": false}' "
          "(keep synthesis thinking via LITELLM_EXTRA_BODY_ROLES='{\"synthesis\": {\"think\": true}}').")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

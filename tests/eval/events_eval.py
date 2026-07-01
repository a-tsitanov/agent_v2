"""Event extraction-quality benchmark (E2 / Phase-5 activation gate).

Scores an events extractor callable of shape
``extractor(text: str) -> list[dict]`` — each dict ``{event_type,
participants, event_ts}`` — against golden cases under ``golden_events/``.
Matching reuses the pipeline's own de-dup key ``event_merge.event_key``
(``event_type`` lower + sorted normalized participants + ts-bucket), so the
eval scores exactly the identity the ingest merge collapses on. Reports
per-``event_type`` and micro P/R/F1 plus wall-clock latency.

Run the LLM backend (needs an extraction LLM + EVENTS_EXTRACTION_ENABLED):
    uv run python -m tests.eval.events_eval

No-regression check (entities/relations must not degrade when events are on)
is the separate ``ner_eval`` run with EVENTS_EXTRACTION_ENABLED off vs on.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from src.graph.event_merge import event_key

GOLDEN_DIR_DEFAULT = Path(__file__).resolve().parent / "golden_events"


@dataclass
class EventStats:
    tp: int = 0
    fp: int = 0
    fn: int = 0
    miss_examples: list[str] = field(default_factory=list)

    @property
    def precision(self) -> float:
        denom = self.tp + self.fp
        return self.tp / denom if denom else 1.0

    @property
    def recall(self) -> float:
        denom = self.tp + self.fn
        return self.tp / denom if denom else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) else 0.0


class EventsExtractor(Protocol):
    def __call__(self, text: str) -> list[dict]: ...


def _keys_by_type(events: list[dict], bucket_days: int) -> dict[str, set[tuple]]:
    by_type: dict[str, set[tuple]] = {}
    for ev in events:
        etype = (ev.get("event_type") or "event").strip().lower()
        key = event_key(
            etype,
            list(ev.get("participants") or []),
            ev.get("event_ts"),
            bucket_days=bucket_days,
        )
        by_type.setdefault(etype, set()).add(key)
    return by_type


def score_case(
    expected: list[dict],
    predicted: list[dict],
    stats: dict[str, EventStats],
    *,
    bucket_days: int,
) -> None:
    exp_by = _keys_by_type(expected, bucket_days)
    got_by = _keys_by_type(predicted, bucket_days)
    for etype in set(exp_by) | set(got_by):
        s = stats.setdefault(etype, EventStats())
        exp = exp_by.get(etype, set())
        got = got_by.get(etype, set())
        s.tp += len(exp & got)
        s.fp += len(got - exp)
        missed = exp - got
        s.fn += len(missed)
        s.miss_examples.extend(str(m) for m in sorted(missed)[:3])


def run_eval(
    extractor: EventsExtractor,
    golden_dir: Path = GOLDEN_DIR_DEFAULT,
    *,
    bucket_days: int = 7,
) -> tuple[dict[str, EventStats], float]:
    """Return (per_type_stats, total_seconds)."""
    files = sorted(golden_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no golden event cases under {golden_dir}")
    per_type: dict[str, EventStats] = {}
    elapsed = 0.0
    for f in files:
        case = json.loads(f.read_text())
        t0 = time.perf_counter()
        predicted = extractor(case["text"])
        elapsed += time.perf_counter() - t0
        score_case(case.get("expected", []), predicted, per_type, bucket_days=bucket_days)
    return per_type, elapsed


def format_report(per_type: dict[str, EventStats], elapsed: float) -> str:
    lines = [f"{'event_type':16s} {'P':>7s} {'R':>7s} {'F1':>7s} {'tp':>5s} {'fp':>5s} {'fn':>5s}"]
    lines.append("-" * 60)
    micro = EventStats()
    for k in sorted(per_type):
        s = per_type[k]
        micro.tp += s.tp
        micro.fp += s.fp
        micro.fn += s.fn
        lines.append(
            f"{k:16s} {s.precision:7.2%} {s.recall:7.2%} {s.f1:7.2%} {s.tp:5d} {s.fp:5d} {s.fn:5d}"
        )
    lines.append("-" * 60)
    lines.append(
        f"{'MICRO':16s} {micro.precision:7.2%} {micro.recall:7.2%} {micro.f1:7.2%} "
        f"{micro.tp:5d} {micro.fp:5d} {micro.fn:5d}"
    )
    lines.append(f"\ntotal extraction time: {elapsed:.3f}s")
    return "\n".join(lines)


def _llm_events_extractor_factory() -> EventsExtractor:  # pragma: no cover - integration
    """Wrap the LightRAG extractor (events forced on) as an events-only callable."""
    import asyncio

    from llama_index.core.graph_stores.types import KG_NODES_KEY
    from llama_index.core.schema import TextNode

    from src.config import settings
    from src.graph.lightrag_extract import LightRAGExtractor
    from src.retrieval.llm import build_extraction_llm

    settings.events.extraction_enabled = True  # force events on for the benchmark
    extractor = LightRAGExtractor(llm=build_extraction_llm())

    def _run(text: str) -> list[dict]:
        node = TextNode(text=text)
        out = asyncio.run(extractor.acall([node]))
        nodes = out[0].metadata.get(KG_NODES_KEY, [])
        events: list[dict] = []
        for n in nodes:
            if getattr(n, "label", None) == "EventOrAction":
                p = n.properties or {}
                events.append(
                    {
                        "event_type": p.get("event_type", ""),
                        "participants": list(p.get("participants") or []),
                        "event_ts": p.get("event_ts"),
                    }
                )
        return events

    return _run


def main() -> int:  # pragma: no cover - CLI
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--golden", type=Path, default=GOLDEN_DIR_DEFAULT)
    p.add_argument("--bucket-days", type=int, default=7)
    args = p.parse_args()
    extractor = _llm_events_extractor_factory()
    per_type, elapsed = run_eval(extractor, args.golden, bucket_days=args.bucket_days)
    print(format_report(per_type, elapsed))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

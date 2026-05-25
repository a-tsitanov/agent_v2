"""Pluggable multilingual NER benchmark.

Scores any extractor callable of shape
``extractor(text: str, types: list[str]) -> list[tuple[surface, type]]``
against golden cases under ``golden_entities/``.  Reports per-type and
micro P/R/F1 PLUS per-language breakdown and wall-clock latency, so a
GLiNER-vs-LLM decision is never made on a language-averaged number.

Surface matching is casefold + whitespace-collapsed equality (NER spans
are surface forms, not canonical IDs — canonicalisation is a separate
stage and out of scope here).
"""
from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


GOLDEN_DIR_DEFAULT = Path(__file__).resolve().parent / "golden_entities"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().casefold()


@dataclass
class NERStats:
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


class Extractor(Protocol):
    def __call__(self, text: str, types: list[str]) -> list[tuple[str, str]]: ...


def score_case(
    expected: dict[str, list[str]],
    predicted: list[tuple[str, str]],
    stats: dict[str, NERStats],
    lang: str,
) -> None:
    # ``lang`` is accepted for call-site symmetry with ``_accumulate_lang``
    # (both run per case); per-TYPE stats are intentionally language-agnostic
    # — the per-language view is built separately by ``_accumulate_lang``.
    pred_by_type: dict[str, set[str]] = {}
    for surface, etype in predicted:
        pred_by_type.setdefault(etype, set()).add(_norm(surface))

    for etype in set(expected) | set(pred_by_type):
        s = stats.setdefault(etype, NERStats())
        exp = {_norm(x) for x in expected.get(etype, [])}
        got = pred_by_type.get(etype, set())
        s.tp += len(exp & got)
        s.fp += len(got - exp)
        missed = exp - got
        s.fn += len(missed)
        s.miss_examples.extend(sorted(missed)[:3])


def run_eval(
    extractor: Extractor,
    types: list[str],
    golden_dir: Path = GOLDEN_DIR_DEFAULT,
) -> tuple[dict[str, NERStats], dict[str, NERStats], float]:
    """Return (per_type_stats, per_lang_stats, total_seconds)."""
    files = sorted(golden_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"no golden NER cases under {golden_dir}")
    per_type: dict[str, NERStats] = {}
    per_lang: dict[str, NERStats] = {}
    elapsed = 0.0
    for f in files:
        case = json.loads(f.read_text())
        lang = case.get("lang", "unknown")
        t0 = time.perf_counter()
        predicted = extractor(case["text"], types)
        elapsed += time.perf_counter() - t0
        score_case(case.get("expected", {}), predicted, per_type, lang)
        _accumulate_lang(case.get("expected", {}), predicted, per_lang, lang)
    return per_type, per_lang, elapsed


def _accumulate_lang(
    expected: dict[str, list[str]],
    predicted: list[tuple[str, str]],
    per_lang: dict[str, NERStats],
    lang: str,
) -> None:
    s = per_lang.setdefault(lang, NERStats())
    exp = {(_norm(x), t) for t, xs in expected.items() for x in xs}
    got = {(_norm(x), t) for x, t in predicted}
    s.tp += len(exp & got)
    s.fp += len(got - exp)
    s.fn += len(exp - got)


def format_report(
    per_type: dict[str, NERStats],
    per_lang: dict[str, NERStats],
    elapsed: float,
) -> str:
    lines = [f"{'type':16s} {'P':>7s} {'R':>7s} {'F1':>7s} {'tp':>5s} {'fp':>5s} {'fn':>5s}"]
    lines.append("-" * 60)
    for k in sorted(per_type):
        s = per_type[k]
        lines.append(f"{k:16s} {s.precision:7.2%} {s.recall:7.2%} {s.f1:7.2%} {s.tp:5d} {s.fp:5d} {s.fn:5d}")
    misses = {k: per_type[k].miss_examples for k in sorted(per_type) if per_type[k].miss_examples}
    if misses:
        lines.append("\nmissed surfaces (first few per type):")
        for k, examples in misses.items():
            lines.append(f"  {k:16s} {', '.join(examples[:5])}")
    lines.append("\nper-language:")
    for k in sorted(per_lang):
        s = per_lang[k]
        lines.append(f"  {k:8s} F1={s.f1:6.2%}  (tp={s.tp} fp={s.fp} fn={s.fn})")
    lines.append(f"\ntotal extraction time: {elapsed:.3f}s")
    return "\n".join(lines)


def _llm_only_extractor_factory() -> Extractor:  # pragma: no cover - integration
    """Wrap the existing LightRAG extractor as a NER-only callable."""
    from src.graph.lightrag_extract import LightRAGExtractor
    from src.retrieval.llm import build_extraction_llm
    from llama_index.core.schema import TextNode
    from llama_index.core.graph_stores.types import KG_NODES_KEY
    import asyncio

    extractor = LightRAGExtractor(llm=build_extraction_llm())

    def _run(text: str, types: list[str]) -> list[tuple[str, str]]:
        node = TextNode(text=text)
        out = asyncio.run(extractor.acall([node]))
        ents = out[0].metadata.get(KG_NODES_KEY, [])
        return [(e.name, e.label) for e in ents]

    return _run


def main() -> int:  # pragma: no cover - CLI
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--backend", choices=["llm", "gliner"], default="llm")
    p.add_argument("--golden", type=Path, default=GOLDEN_DIR_DEFAULT)
    args = p.parse_args()
    types = ["Person", "Organization", "Location", "Product", "Concept"]
    if args.backend == "llm":
        extractor = _llm_only_extractor_factory()
    else:
        from src.graph.gliner_extract import gliner_ner_callable
        extractor = gliner_ner_callable()
    per_type, per_lang, elapsed = run_eval(extractor, types, args.golden)
    print(format_report(per_type, per_lang, elapsed))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

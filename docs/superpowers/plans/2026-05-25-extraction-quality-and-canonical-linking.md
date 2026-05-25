# Extraction Quality & Canonical Linking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise entity-extraction speed and canonical reliability — add a measured GLiNER fast-path (decision-gated by benchmark, never a blind replacement), expand the deterministic identifier layer, remove the ER LLM-judge serialization bottleneck, and turn the local Wikibase from a write-only sink into a true canonical entity-linking anchor.

**Architecture:** Keep the existing layered pipeline (deterministic identifiers → entity extraction → cross-chunk merge → entity resolution → Wikibase). This plan (1) adds a multilingual NER benchmark harness and an optional GLiNER extractor behind it, (2) adds new deterministic identifier detectors, (3) parallelizes the ER judge and caches verdicts, and (4) adds alias storage + an embedding-based mention→QID linker against the local Wikibase. Each task is independently shippable and reverts cleanly.

**Tech Stack:** Python 3.12, LlamaIndex, Neo4j (`Neo4jPropertyGraphStore`), Milvus, LiteLLM proxy (OpenAILike), Temporal, pytest. New dep: `gliner` (optional, lazy-imported like `postal`/`wikibaseintegrator`).

**Decision gates baked in:**
- GLiNER is adopted only if Task 1's benchmark shows entity-F1 ≥ LLM-only (within tolerance) AND lower latency, **per language**. Task 2 ships GLiNER as an opt-in extractor; no default switch flips in this plan.
- Wikibase linking (Task 6) ships behind a config flag, default off, until validated on real data.

---

## File Structure

**New files:**
- `tests/eval/golden_entities/` — multilingual gold NER cases (`{name, lang, text, expected:{Type:[surface,...]}}`).
- `tests/eval/ner_eval.py` — pluggable NER benchmark runner (P/R/F1 + latency, per language).
- `tests/eval/test_ner_eval.py` — unit tests for the runner's scoring.
- `src/graph/gliner_extract.py` — `GLiNERExtractor` (optional, lazy-imported), a `TransformComponent` that populates `KG_NODES_KEY` entities only (no relations).
- `src/graph/canonical_linker.py` — `CanonicalLinker`: mention → Wikibase QID via alias/embedding/verify against a persisted canonical index.

**Modified files:**
- `src/ingestion/identifiers.py` — add detectors: `BankAccount` (20-digit + BIC control), `KPP`, `IBAN`, `CreditCard` (Luhn), multi-region phone region hint.
- `src/graph/schema.py` — add `BankAccount`, `KPP`, `IBAN`, `CreditCard` to `IdentifierType`-style listings if a graph label is needed (identifiers already deterministic; only add labels actually pushed).
- `src/graph/entity_resolution.py:524-551` — parallelize `_llm_judge_pairs`; add persistent verdict cache hooks.
- `src/graph/index.py` — register `mode="gliner"` and `mode="gliner+llm"` in `build_kg_extractor`.
- `src/storage/wikibase.py` — store observed surface forms as aliases on items.
- `src/config.py` — flags: `ER_VERDICT_CACHE_ENABLED`, `CANONICAL_LINKER_ENABLED`, `GLINER_MODEL`.

---

## Task 1: Multilingual NER benchmark harness

**Why first:** This is the decision instrument for "replace LLM with GLiNER?". Nothing downstream depends on a model; it scores any callable `extractor(text, types) -> list[(surface, type)]`. Build it before touching extraction.

**Files:**
- Create: `tests/eval/ner_eval.py`
- Create: `tests/eval/test_ner_eval.py`
- Create: `tests/eval/golden_entities/news_ru.json`
- Create: `tests/eval/golden_entities/correspondence_ru.json`
- Create: `tests/eval/golden_entities/news_en.json`

- [ ] **Step 1: Write the failing test for scoring**

```python
# tests/eval/test_ner_eval.py
from tests.eval.ner_eval import score_case, NERStats


def test_score_case_counts_tp_fp_fn_per_type():
    expected = {"Person": ["Иванов И.П."], "Organization": ["ООО Ромашка"]}
    predicted = [("Иванов И.П.", "Person"), ("ЗАО Лютик", "Organization")]
    stats: dict[str, NERStats] = {}
    score_case(expected, predicted, stats, lang="ru")

    assert stats["Person"].tp == 1
    assert stats["Person"].fn == 0
    assert stats["Organization"].tp == 0       # wrong surface
    assert stats["Organization"].fn == 1       # expected one, missed it
    assert stats["Organization"].fp == 1       # predicted a wrong one


def test_f1_is_harmonic_mean():
    s = NERStats(tp=8, fp=2, fn=2)
    assert abs(s.precision - 0.8) < 1e-9
    assert abs(s.recall - 0.8) < 1e-9
    assert abs(s.f1 - 0.8) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/eval/test_ner_eval.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tests.eval.ner_eval'`

- [ ] **Step 3: Write minimal implementation**

```python
# tests/eval/ner_eval.py
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
from typing import Callable, Protocol


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
        # per-language: collapse all types into one bucket keyed by lang
        score_case(
            case.get("expected", {}), predicted, per_lang, lang,
        ) if False else None
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/eval/test_ner_eval.py -v`
Expected: PASS (both tests)

- [ ] **Step 5: Add three multilingual golden cases**

These three texts describe the SAME real-world entities in different
surface forms and languages (Ромашка / Romashka LLC, Сидоров / И.П.
Сидоров / Ivan Sidorov, Москва / Moscow), so they double as canonical-
resolution fixtures for Tasks 5–6.  For the NER benchmark, `expected`
lists the surface spans as they appear; the same gold scores both
backends, so the LLM-vs-GLiNER A/B stays fair even under strict surface
matching.

```json
// tests/eval/golden_entities/news_ru.json
{
  "name": "news_ru",
  "lang": "ru",
  "text": "Вчера в Москве состоялось подписание соглашения между ООО «Ромашка» и группой компаний «ТехноСтрой». Со стороны «Ромашки» документ подписал генеральный директор Иван Петрович Сидоров. По его словам, проект будет реализован в течение полутора лет. «ТехноСтрой» возглавляет Анна Морозова, ранее работавшая в АО «СтройИнвест». По вопросам сотрудничества компания «Ромашка» предлагает обращаться по телефону +7 (495) 123-45-67 или на горячую линию 8-800-555-35-35. Подробности доступны на сайте компании. Офис расположен в центре Москвы, недалеко от станции метро «Тверская».",
  "expected": {
    "Person": ["Иван Петрович Сидоров", "Анна Морозова"],
    "Organization": ["ООО «Ромашка»", "«ТехноСтрой»", "АО «СтройИнвест»"],
    "Location": ["Москве", "«Тверская»"]
  }
}
```

```json
// tests/eval/golden_entities/correspondence_ru.json
{
  "name": "correspondence_ru",
  "lang": "ru",
  "text": "Коллеги, по итогам встречи в г. Москва направляю краткую сводку. И.П. Сидоров (Ромашка) подтвердил готовность начать работы с 15 числа. Контактное лицо со стороны заказчика — Морозова А.С., тел. 8 (495) 123 45 67 доб. 204, мобильный 8-916-555-77-89. Сидоров просил также добавить в копию переписки нового сотрудника отдела, его номер 89261234567. По московскому офису Ромашки: адрес уточняется, но это в районе Тверской. Прошу подготовить договор к понедельнику. P.S. Анна вчера упомянула, что СтройИнвест, где она раньше работала, тоже может присоединиться к проекту на втором этапе.",
  "expected": {
    "Person": ["И.П. Сидоров", "Морозова А.С."],
    "Organization": ["Ромашка", "СтройИнвест"],
    "Location": ["Москва", "Тверской"]
  }
}
```

```json
// tests/eval/golden_entities/news_en.json
{
  "name": "news_en",
  "lang": "en",
  "text": "Moscow-based Romashka LLC has announced a strategic partnership with TechnoStroy Group, marking one of the largest construction deals in the Russian capital this quarter. The agreement was signed by Ivan Sidorov, CEO of Romashka, and Anna Morozova, head of TechnoStroy. Sidorov, who has led the company since 2019, stated that the partnership reflects growing demand in the Moscow market. Morozova previously held a senior position at StroyInvest JSC before joining TechnoStroy last year. International inquiries can be directed to +7-495-123-4567 or via the company's London representative office at +44 20 7946 0958. A press conference is scheduled in Moscow next week.",
  "expected": {
    "Person": ["Ivan Sidorov", "Anna Morozova"],
    "Organization": ["Romashka LLC", "TechnoStroy Group", "StroyInvest JSC"],
    "Location": ["Moscow", "London"]
  }
}
```

- [ ] **Step 6: Commit**

```bash
git add tests/eval/ner_eval.py tests/eval/test_ner_eval.py tests/eval/golden_entities/
git commit -m "feat(eval): multilingual NER benchmark harness with per-language F1 + latency"
```

---

## Task 2: GLiNER optional extractor (opt-in, no default switch)

**Files:**
- Create: `src/graph/gliner_extract.py`
- Create: `tests/test_graph/test_gliner_extract.py`
- Modify: `src/graph/index.py` (register modes)
- Modify: `src/config.py` (add `GLINER_MODEL`)
- Modify: `pyproject.toml` (add optional `gliner` dep under an extra)

- [ ] **Step 1: Write the failing test (model mocked — no download in CI)**

```python
# tests/test_graph/test_gliner_extract.py
from unittest.mock import MagicMock
from llama_index.core.schema import TextNode
from llama_index.core.graph_stores.types import KG_NODES_KEY
from src.graph.gliner_extract import GLiNERExtractor


def _fake_model(spans):
    m = MagicMock()
    m.predict_entities.return_value = spans
    return m


def test_populates_kg_nodes_from_spans():
    model = _fake_model([
        {"text": "Иванов И.П.", "label": "Person", "score": 0.97},
        {"text": "ООО Ромашка", "label": "Organization", "score": 0.91},
    ])
    ext = GLiNERExtractor(model=model, entity_types=["Person", "Organization"])
    node = TextNode(text="Иванов И.П. из ООО Ромашка")
    out = ext([node])
    ents = out[0].metadata[KG_NODES_KEY]
    names = {(e.name, e.label) for e in ents}
    assert ("Иванов И.П.", "Person") in names
    assert ("ООО Ромашка", "Organization") in names


def test_score_threshold_drops_low_confidence():
    model = _fake_model([
        {"text": "Maybe", "label": "Person", "score": 0.30},
    ])
    ext = GLiNERExtractor(model=model, entity_types=["Person"], threshold=0.5)
    out = ext([TextNode(text="Maybe something")])
    assert out[0].metadata[KG_NODES_KEY] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_gliner_extract.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.graph.gliner_extract'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/graph/gliner_extract.py
"""Optional GLiNER fast-path entity extractor.

GLiNER is an encoder NER model (multilingual DeBERTa backbone) that
detects entity SPANS for arbitrary type labels supplied at inference —
no relations, no descriptions, no canonicalisation.  It runs ~10-50x
faster than a generative 8B LLM per chunk.

Place in pipeline: detect Person/Organization/Location/Product/Concept
spans; the LLM stage then only relates + describes them.  Deterministic
identifiers (phones/INN/...) are NOT GLiNER's job — that layer
(``src/ingestion/identifiers.py``) stays authoritative.

The ``gliner`` package is heavy + downloads weights — imported lazily so
callers that don't use it pay nothing.  Tests inject a mock ``model``.
"""
from __future__ import annotations

from typing import Any

from llama_index.core.graph_stores.types import EntityNode, KG_NODES_KEY
from llama_index.core.schema import BaseNode, TransformComponent

from src.graph.lightrag_parse import _normalize_entity_name


class GLiNERExtractor(TransformComponent):
    """Populate ``KG_NODES_KEY`` with GLiNER-detected entity spans."""

    model: Any = None
    entity_types: list[str] = []
    threshold: float = 0.5

    class Config:
        arbitrary_types_allowed = True

    def __init__(
        self,
        model: Any = None,
        entity_types: list[str] | None = None,
        threshold: float = 0.5,
        model_name: str | None = None,
    ) -> None:
        from src.graph.schema import EntityType
        import typing

        types = entity_types or list(typing.get_args(EntityType))
        if model is None and model_name is not None:  # pragma: no cover
            from gliner import GLiNER

            model = GLiNER.from_pretrained(model_name)
        super().__init__(
            model=model, entity_types=types, threshold=threshold
        )

    def __call__(self, nodes: list[BaseNode], **kwargs: Any) -> list[BaseNode]:
        for node in nodes:
            spans = self.model.predict_entities(
                node.get_content(), self.entity_types, threshold=self.threshold
            )
            seen: set[str] = set()
            ents: list[EntityNode] = []
            for sp in spans:
                if sp.get("score", 1.0) < self.threshold:
                    continue
                name = _normalize_entity_name(sp["text"])
                key = name.casefold()
                if not name or key in seen:
                    continue
                seen.add(key)
                ents.append(
                    EntityNode(
                        name=name,
                        label=sp["label"],
                        properties={"description": "", "gliner_score": sp.get("score")},
                    )
                )
            node.metadata[KG_NODES_KEY] = ents
        return nodes

    async def acall(self, nodes: list[BaseNode], **kwargs: Any) -> list[BaseNode]:
        return self.__call__(nodes, **kwargs)


def gliner_ner_callable(model_name: str | None = None):  # pragma: no cover
    """Adapter for ``tests/eval/ner_eval.py`` — returns extractor fn."""
    from src.config import get_settings

    name = model_name or get_settings().gliner_model
    from gliner import GLiNER

    model = GLiNER.from_pretrained(name)

    def _run(text: str, types: list[str]) -> list[tuple[str, str]]:
        spans = model.predict_entities(text, types, threshold=0.5)
        return [(s["text"], s["label"]) for s in spans]

    return _run
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_gliner_extract.py -v`
Expected: PASS (both tests)

- [ ] **Step 5: Register modes in `build_kg_extractor` and add config + dep**

In `src/graph/index.py`, inside `build_kg_extractor`, add branches (match existing `mode` dispatch style):

```python
    if mode == "gliner":
        from src.graph.gliner_extract import GLiNERExtractor
        from src.config import get_settings
        return GLiNERExtractor(model_name=get_settings().gliner_model)
    if mode == "gliner+llm":
        # GLiNER detects spans; LightRAG (relations-only flag) relates them.
        # Until the relations-only LightRAG mode lands, callers compose the
        # two transforms in the ingest activity; this branch returns the
        # GLiNER half so the pipeline can A/B without a code switch.
        from src.graph.gliner_extract import GLiNERExtractor
        from src.config import get_settings
        return GLiNERExtractor(model_name=get_settings().gliner_model)
```

In `src/config.py`, add to the settings class (near `LITELLM_EXTRACTION_MODEL`):

```python
    gliner_model: str = "urchade/gliner_multi-v2.1"
    """HF id for the GLiNER multilingual model used by mode='gliner'."""
```

In `pyproject.toml`, add an optional extra:

```toml
[project.optional-dependencies]
gliner = ["gliner>=0.2.13"]
```

- [ ] **Step 6: Run the benchmark both ways and record the decision**

Run (requires `pip install -e '.[gliner]'` locally — NOT in CI):
```bash
python -m tests.eval.ner_eval --backend llm   | tee /tmp/ner_llm.txt
python -m tests.eval.ner_eval --backend gliner | tee /tmp/ner_gliner.txt
```
Expected: two reports with per-language F1 + total time. Decision rule: adopt `gliner+llm` only where GLiNER F1 ≥ LLM F1 (±0.03) AND time materially lower, **per language**. Record the verdict in the PR description.

- [ ] **Step 7: Commit**

```bash
git add src/graph/gliner_extract.py tests/test_graph/test_gliner_extract.py src/graph/index.py src/config.py pyproject.toml
git commit -m "feat(graph): optional GLiNER entity extractor (opt-in mode, benchmark-gated)"
```

---

## Task 3: Expand deterministic identifiers (BankAccount, KPP, IBAN, CreditCard)

**Files:**
- Modify: `src/ingestion/identifiers.py`
- Create: `tests/eval/golden_identifiers/bank_account.json`
- Create: `tests/eval/golden_identifiers/kpp_iban_card.json`
- Create: `tests/eval/golden_identifiers/phones_multilingual.json`
- Modify: `tests/eval/identifier_recall.py` (add recall thresholds for new types)

- [ ] **Step 1: Write failing golden cases + threshold entries**

```json
// tests/eval/golden_identifiers/bank_account.json
{
  "name": "bank_account",
  "description": "20-digit settlement account validated against its BIC.",
  "text": "Расчётный счёт 40702810500000012345 в банке с БИК 044525225. Корсчёт 30101810400000000225.",
  "expected": {
    "BankAccount": ["40702810500000012345"],
    "BIC": ["044525225"]
  }
}
```

```json
// tests/eval/golden_identifiers/kpp_iban_card.json
{
  "name": "kpp_iban_card",
  "description": "KPP, IBAN (mod-97) and a Luhn-valid card number.",
  "text": "КПП 770801001. IBAN: DE89370400440532013000. Карта 4111 1111 1111 1111.",
  "expected": {
    "KPP": ["770801001"],
    "IBAN": ["DE89370400440532013000"],
    "CreditCard": ["4111111111111111"]
  }
}
```

Also add a multilingual phone regression case (no new code — it
validates that the existing `PhoneNumberMatcher(text, "RU")` path
already handles an explicit international `+44` number alongside RU
national-format numbers, because the `+`-prefix carries the country
code regardless of default region):

```json
// tests/eval/golden_identifiers/phones_multilingual.json
{
  "name": "phones_multilingual",
  "description": "RU national-format + UK international phone in one English document.",
  "text": "International inquiries can be directed to +7-495-123-4567 or via the company's London representative office at +44 20 7946 0958.",
  "expected": {
    "PhoneNumber": ["+74951234567", "+442079460958"]
  }
}
```

In `tests/eval/identifier_recall.py`, extend `RECALL_THRESHOLDS`:

```python
    "BankAccount": 0.95,
    "KPP": 0.95,
    "IBAN": 0.95,
    "CreditCard": 0.95,
```

- [ ] **Step 2: Run the eval to verify it fails**

Run: `python -m tests.eval.identifier_recall --golden tests/eval/golden_identifiers`
Expected: THRESHOLD VIOLATIONS listing `recall BankAccount: 0.00% < 95%`, `KPP`, `IBAN`, `CreditCard` (detectors don't exist yet).

- [ ] **Step 3: Add the detectors**

Add to `src/ingestion/identifiers.py`. First extend the `IdentifierType` Literal (after `"BIC",`):

```python
    "BankAccount",
    "KPP",
    "IBAN",
    "CreditCard",
```

Add priority entries in `_PRIORITY` (alongside the `100` business IDs):

```python
    "BankAccount": 100,
    "KPP": 100,
    "IBAN": 100,
    "CreditCard": 95,
```

Add detectors (place near the other RU-business detectors):

```python
# ── KPP (9 digits: NNNN PP NNN) ──────────────────────────────────────
_KPP_RE = re.compile(r"(?<!\d)(\d{4}[0-9A-Z]{2}\d{3})(?!\d)")


def _extract_kpp(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _KPP_RE.finditer(text):
        d = m.group(1)
        out.append(
            NormalizedIdentifier(
                entity_type="KPP", canonical=d, original=d, span=m.span(1),
            )
        )
    return out


# ── BankAccount (20-digit RU account, control key vs BIC) ────────────
# Control: the 20-digit account combined with the last 3 BIC digits +
# "0" forms a 23-digit string; weighted sum (weights 7,1,3 repeating)
# mod 10 must be 0.  We validate when a BIC is present in the same text.
_ACCOUNT_RE = re.compile(r"(?<!\d)(\d{20})(?!\d)")
_ACCOUNT_WEIGHTS = (7, 1, 3) * 8  # 24 ≥ 23 needed; we slice


def _account_control_ok(account: str, bic_tail: str) -> bool:
    seq = bic_tail + account  # 3 + 20 = 23 digits
    total = sum(int(c) * _ACCOUNT_WEIGHTS[i] for i, c in enumerate(seq))
    return total % 10 == 0


def _extract_bank_accounts(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    bics = [m.group(0) for m in _BIC_RE.finditer(text)]
    for m in _ACCOUNT_RE.finditer(text):
        acct = m.group(1)
        # Validate against any BIC in the document; accept if any matches,
        # or if no BIC is present (can't validate — accept by shape).
        ok = (not bics) or any(
            _account_control_ok(acct, bic[-3:]) for bic in bics
        )
        if not ok:
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="BankAccount", canonical=acct, original=acct,
                span=m.span(1),
            )
        )
    return out


# ── IBAN (mod-97 == 1) ────────────────────────────────────────────────
_IBAN_RE = re.compile(r"\b([A-Z]{2}\d{2}[A-Z0-9]{11,30})\b")


def _iban_ok(iban: str) -> bool:
    rearranged = iban[4:] + iban[:4]
    digits = "".join(
        str(int(c, 36)) if c.isalpha() else c for c in rearranged
    )
    return int(digits) % 97 == 1


def _extract_ibans(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _IBAN_RE.finditer(text):
        cand = m.group(1)
        if not _iban_ok(cand):
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="IBAN", canonical=cand, original=cand, span=m.span(1),
            )
        )
    return out


# ── CreditCard (13-19 digits, optional spaces/hyphens, Luhn) ──────────
_CARD_RE = re.compile(r"(?<!\d)(\d[\d \-]{11,21}\d)(?!\d)")


def _extract_credit_cards(text: str) -> list[NormalizedIdentifier]:
    out: list[NormalizedIdentifier] = []
    for m in _CARD_RE.finditer(text):
        raw = m.group(1)
        digits = re.sub(r"[ \-]", "", raw)
        if not (13 <= len(digits) <= 19) or not _luhn_ok(digits):
            continue
        out.append(
            NormalizedIdentifier(
                entity_type="CreditCard", canonical=digits, original=raw,
                span=m.span(1),
            )
        )
    return out
```

Wire them into `extract_identifiers` (in the Business/financial block):

```python
    found.extend(_extract_kpp(text))
    found.extend(_extract_bank_accounts(text))
    found.extend(_extract_ibans(text))
    found.extend(_extract_credit_cards(text))
```

Note: `_IMEI_RE` matches `\b\d{15}\b` and `_CARD_RE` can overlap 15-digit numbers — `_resolve_overlaps` keeps the higher-priority match (IMEI=95, CreditCard=95; on tie the earlier span-start/wider wins). Verify no regression in Step 4.

- [ ] **Step 4: Run the eval to verify it passes**

Run: `python -m tests.eval.identifier_recall --golden tests/eval/golden_identifiers --strict`
Expected: `All thresholds satisfied.` exit code 0, including the existing types (no regression).

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/identifiers.py tests/eval/golden_identifiers/bank_account.json tests/eval/golden_identifiers/kpp_iban_card.json tests/eval/golden_identifiers/phones_multilingual.json tests/eval/identifier_recall.py
git commit -m "feat(identifiers): add BankAccount (BIC control), KPP, IBAN, CreditCard detectors"
```

---

## Task 4: Parallelize the ER LLM-judge batches

**Why:** `_llm_judge_pairs` awaits each batch sequentially; latency on name-dense documents scales with batch count. The `BoundedLLM` semaphore already caps concurrency, so `asyncio.gather` over batches is safe.

**Files:**
- Modify: `src/graph/entity_resolution.py:524-551`
- Create/Modify: `tests/test_graph/test_er_judge_parallel.py`

- [ ] **Step 1: Write the failing test (asserts concurrent dispatch + correct order)**

```python
# tests/test_graph/test_er_judge_parallel.py
import asyncio
from src.graph.entity_resolution import _llm_judge_pairs, ERConfig


class _Item:
    def __init__(self, name, label="Person", desc="d"):
        self.name, self.label, self.description = name, label, desc


class _SpyLLM:
    """Records max in-flight calls; returns all-SAME verdicts."""

    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0

    async def achat(self, messages):
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0.01)
        self.inflight -= 1
        # Count pairs in the prompt body to size the JSON response.
        body = messages[1].content
        n = body.count("Pair ")
        verdicts = ", ".join(
            f'{{"pair": {i + 1}, "verdict": "SAME"}}' for i in range(n)
        )

        class _Resp:
            class message:  # noqa: N801
                content = f"[{verdicts}]"

        return _Resp()


def test_batches_run_concurrently_and_preserve_order():
    pairs = [(_Item(f"A{i}"), _Item(f"B{i}")) for i in range(25)]
    cfg = ERConfig()  # judge_batch defaults to 10 → 3 batches
    llm = _SpyLLM()
    verdicts = asyncio.run(_llm_judge_pairs(pairs, llm, cfg))
    assert len(verdicts) == 25
    assert all(verdicts)              # all SAME
    assert llm.max_inflight >= 2      # batches overlapped, not serial
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_er_judge_parallel.py -v`
Expected: FAIL on `assert llm.max_inflight >= 2` (current loop is sequential, max_inflight == 1).

- [ ] **Step 3: Rewrite `_llm_judge_pairs` to gather batches**

Replace the body of `_llm_judge_pairs` (`src/graph/entity_resolution.py:524-551`) with:

```python
async def _llm_judge_pairs(
    pairs: list[tuple[_Item, _Item]], llm: Any, cfg: ERConfig,
) -> list[bool]:
    """For each input pair, return True when LLM judges SAME, else False.

    Batches are dispatched concurrently; the process-wide BoundedLLM
    semaphore (see src/retrieval/llm_semaphore.py) caps real parallelism,
    so this never floods the proxy.
    """
    if not pairs:
        return []
    verdicts: list[bool] = [False] * len(pairs)
    batch_offsets = list(range(0, len(pairs), cfg.judge_batch))

    async def _judge_one(batch_start: int) -> tuple[int, list[bool]]:
        batch = pairs[batch_start: batch_start + cfg.judge_batch]
        body = _format_pair_prompt(batch)
        messages = [
            ChatMessage(role=MessageRole.SYSTEM, content=_JUDGE_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=body),
        ]
        try:
            resp = await llm.achat(messages)
            text = strip_thinking(resp.message.content or "")
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "ER judge batch failed at offset={o}: {err}",
                o=batch_start, err=exc,
            )
            return batch_start, [False] * len(batch)
        return batch_start, list(_parse_judge_response(text, len(batch)))

    results = await asyncio.gather(*[_judge_one(o) for o in batch_offsets])
    for batch_start, oks in results:
        for verdict_pos, ok in enumerate(oks):
            if ok:
                verdicts[batch_start + verdict_pos] = True
    return verdicts
```

Confirm `import asyncio` is present at the top of `entity_resolution.py` (it is — used by `_embed_entities`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_er_judge_parallel.py -v`
Expected: PASS

- [ ] **Step 5: Run the existing ER test suite for no regression**

Run: `pytest tests/test_graph/ -v`
Expected: PASS (all pre-existing ER tests still green)

- [ ] **Step 6: Commit**

```bash
git add src/graph/entity_resolution.py tests/test_graph/test_er_judge_parallel.py
git commit -m "perf(er): dispatch LLM-judge batches concurrently under the bounded semaphore"
```

---

## Task 5: Persistent ER verdict cache

**Why:** Same name-pairs recur across re-ingests and within hub-heavy documents. Caching `(norm_a, label_a, norm_b, label_b) → verdict` in Neo4j lets ER skip already-judged pairs.

**Files:**
- Modify: `src/graph/entity_resolution.py` (add cache lookup/store; gate by config)
- Modify: `src/config.py` (`ER_VERDICT_CACHE_ENABLED`)
- Create: `tests/test_graph/test_er_verdict_cache.py`

- [ ] **Step 1: Write the failing test for the pure cache key + filter functions**

```python
# tests/test_graph/test_er_verdict_cache.py
from src.graph.entity_resolution import _verdict_key, _partition_cached


class _Item:
    def __init__(self, norm, label="Person"):
        self.norm, self.label = norm, label


def test_verdict_key_is_order_insensitive():
    a, b = _Item("ivanov"), _Item("ivanoff")
    assert _verdict_key(a, b) == _verdict_key(b, a)


def test_partition_cached_splits_known_from_unknown():
    a, b, c = _Item("x"), _Item("y"), _Item("z")
    pairs = [(a, b), (a, c)]
    cache = {_verdict_key(a, b): True}
    cached, uncached = _partition_cached(pairs, cache)
    assert cached == [((a, b), True)]
    assert uncached == [(a, c)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_er_verdict_cache.py -v`
Expected: FAIL with `ImportError: cannot import name '_verdict_key'`

- [ ] **Step 3: Add the pure helpers + Neo4j load/store + wire into the judge path**

Add to `src/graph/entity_resolution.py`:

```python
def _verdict_key(a: _Item, b: _Item) -> str:
    """Order-insensitive cache key for a candidate pair."""
    left = (a.norm, a.label)
    right = (b.norm, b.label)
    lo, hi = sorted([left, right])
    return f"{lo[1]}:{lo[0]}|{hi[1]}:{hi[0]}"


def _partition_cached(
    pairs: list[tuple[_Item, _Item]], cache: dict[str, bool],
) -> tuple[list[tuple[tuple[_Item, _Item], bool]], list[tuple[_Item, _Item]]]:
    cached: list[tuple[tuple[_Item, _Item], bool]] = []
    uncached: list[tuple[_Item, _Item]] = []
    for pair in pairs:
        key = _verdict_key(pair[0], pair[1])
        if key in cache:
            cached.append((pair, cache[key]))
        else:
            uncached.append(pair)
    return cached, uncached


def _load_verdict_cache(store: Any, keys: list[str]) -> dict[str, bool]:
    if store is None or not keys:
        return {}
    try:
        rows = store.structured_query(
            "MATCH (v:ERVerdict) WHERE v.key IN $keys "
            "RETURN v.key AS key, v.same AS same",
            param_map={"keys": keys},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("ER verdict cache load failed: {e}", e=exc)
        return {}
    return {r["key"]: bool(r["same"]) for r in rows if isinstance(r, dict)}


def _store_verdicts(store: Any, entries: dict[str, bool]) -> None:
    if store is None or not entries:
        return
    try:
        store.structured_query(
            "UNWIND $rows AS row "
            "MERGE (v:ERVerdict {key: row.key}) "
            "SET v.same = row.same, v.updated = datetime()",
            param_map={"rows": [{"key": k, "same": s} for k, s in entries.items()]},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("ER verdict cache store failed: {e}", e=exc)
```

In the function that calls `_llm_judge_pairs` (the borderline-resolution path inside `resolve_entities`), wrap it. Locate the existing call `verdicts = await _llm_judge_pairs(borderline_items, llm, cfg)` and replace with:

```python
        graph_store = getattr(embed_model, "_er_graph_store", None) or er_store
        cache: dict[str, bool] = {}
        if cfg.verdict_cache_enabled and graph_store is not None:
            keys = [_verdict_key(a, b) for a, b in borderline_items]
            cache = _load_verdict_cache(graph_store, keys)
        cached, uncached = _partition_cached(borderline_items, cache)
        fresh_verdicts = await _llm_judge_pairs(uncached, llm, cfg)
        # Reassemble verdicts in the original borderline_items order.
        verdict_by_pair: dict[int, bool] = {}
        for (pair, v) in cached:
            verdict_by_pair[id(pair)] = v
        for pair, v in zip(uncached, fresh_verdicts):
            verdict_by_pair[id(pair)] = v
        verdicts = [verdict_by_pair[id(p)] for p in borderline_items]
        if cfg.verdict_cache_enabled and graph_store is not None:
            new_entries = {
                _verdict_key(a, b): v
                for (a, b), v in zip(uncached, fresh_verdicts)
            }
            _store_verdicts(graph_store, new_entries)
```

Add `er_store: Any = None` to `resolve_entities`'s signature (threaded from the Temporal activity that already holds the Neo4j store), and add to `ERConfig`:

```python
    verdict_cache_enabled: bool = True
```

NOTE: `er_store` is passed by `src/workflow/activities/merge_and_resolve.py` — update that call site to forward its existing Neo4j store handle as `er_store=`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_er_verdict_cache.py -v`
Expected: PASS

- [ ] **Step 5: Run full ER suite for no regression**

Run: `pytest tests/test_graph/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/graph/entity_resolution.py src/config.py tests/test_graph/test_er_verdict_cache.py src/workflow/activities/merge_and_resolve.py
git commit -m "perf(er): persistent Neo4j verdict cache to skip re-judging recurring pairs"
```

---

## Task 6: Wikibase alias storage + canonical mention→QID linker (flagged, default off)

**Why:** Today `push_entities` keys QID lookup on exact Neo4j `name` — a sink, not a linker. Storing every observed surface form as a Wikibase **alias** and adding an embedding-based **CanonicalLinker** turns the growing local Wikibase into a canonical anchor: a mention resolves to an existing QID by alias → embedding kNN → LLM verify, else mints a new item. This collapses cross-document identity at the QID level.

**Files:**
- Modify: `src/storage/wikibase.py` (alias write on create/update)
- Create: `src/graph/canonical_linker.py`
- Create: `tests/test_graph/test_canonical_linker.py`
- Modify: `src/config.py` (`CANONICAL_LINKER_ENABLED`)

- [ ] **Step 1: Write the failing test for the linker decision logic (pure, store + llm mocked)**

```python
# tests/test_graph/test_canonical_linker.py
import asyncio
from src.graph.canonical_linker import CanonicalLinker, CanonicalCandidate


class _FakeIndex:
    """Returns preset candidates by exact-alias then embedding."""

    def __init__(self, alias_hit=None, knn=None):
        self._alias_hit = alias_hit
        self._knn = knn or []

    def alias_lookup(self, surface, label):
        return self._alias_hit

    def knn(self, embedding, label, k):
        return self._knn


def test_exact_alias_links_without_llm():
    idx = _FakeIndex(alias_hit=CanonicalCandidate(qid="Q5", name="Сбербанк", score=1.0))
    linker = CanonicalLinker(index=idx, llm=None, embed=None)
    qid = asyncio.run(linker.link("Сбер", "Organization", embedding=[0.0]))
    assert qid == "Q5"


def test_no_candidate_returns_none_to_mint_new():
    idx = _FakeIndex(alias_hit=None, knn=[])
    linker = CanonicalLinker(index=idx, llm=None, embed=None)
    qid = asyncio.run(linker.link("Новая Фирма", "Organization", embedding=[0.1]))
    assert qid is None


def test_high_cosine_same_script_links_without_llm():
    cand = CanonicalCandidate(qid="Q9", name="Acme Corp", score=0.93)
    idx = _FakeIndex(alias_hit=None, knn=[cand])
    linker = CanonicalLinker(index=idx, llm=None, embed=None, auto_link_threshold=0.9)
    qid = asyncio.run(linker.link("Acme Corporation", "Organization", embedding=[0.2]))
    assert qid == "Q9"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_graph/test_canonical_linker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.graph.canonical_linker'`

- [ ] **Step 3: Implement the linker**

```python
# src/graph/canonical_linker.py
"""Mention → canonical QID linker against the local Wikibase.

Pattern (ReFinED/ReLiK-style, but against YOUR Wikibase, not public
Wikidata — so it sidesteps the English/Wikidata bias for Russian text):

    surface mention
      → exact alias lookup        (deterministic, no model)
      → embedding kNN candidates  (over persisted entity embeddings)
      → auto-link if cosine ≥ T and same script
      → else LLM verify the top candidate
      → else None  (caller mints a new Wikibase item + seeds its alias)

The ``index`` is any object exposing ``alias_lookup(surface, label)`` and
``knn(embedding, label, k)``; the production impl queries Neo4j
(:__Entity__ {wikibase_qid}) + its stored embeddings.  Tests inject a fake.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from src.graph.entity_resolution import _script_of


@dataclass
class CanonicalCandidate:
    qid: str
    name: str
    score: float


_VERIFY_SYSTEM = (
    "You decide if a mention refers to the SAME real-world entity as a "
    "candidate from a knowledge base. Answer strictly YES or NO."
)


class CanonicalLinker:
    def __init__(
        self,
        index: Any,
        llm: Any,
        embed: Any,
        auto_link_threshold: float = 0.9,
        knn_k: int = 5,
    ) -> None:
        self.index = index
        self.llm = llm
        self.embed = embed
        self.auto_link_threshold = auto_link_threshold
        self.knn_k = knn_k

    async def link(
        self, surface: str, label: str, embedding: list[float],
    ) -> str | None:
        hit = self.index.alias_lookup(surface, label)
        if hit is not None:
            return hit.qid

        candidates = self.index.knn(embedding, label, self.knn_k)
        if not candidates:
            return None
        top = max(candidates, key=lambda c: c.score)

        same_script = _script_of(surface) == _script_of(top.name) != "mixed"
        if top.score >= self.auto_link_threshold and same_script:
            return top.qid

        if self.llm is None:
            return None
        verdict = await self._verify(surface, label, top)
        return top.qid if verdict else None

    async def _verify(
        self, surface: str, label: str, cand: CanonicalCandidate,
    ) -> bool:
        body = (
            f"Mention: {surface!r} (type={label})\n"
            f"Candidate: {cand.name!r} (qid={cand.qid})\n"
            f"Same entity? Answer YES or NO."
        )
        resp = await self.llm.achat([
            ChatMessage(role=MessageRole.SYSTEM, content=_VERIFY_SYSTEM),
            ChatMessage(role=MessageRole.USER, content=body),
        ])
        return "YES" in (resp.message.content or "").upper()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_graph/test_canonical_linker.py -v`
Expected: PASS (all three tests)

- [ ] **Step 5: Store observed surface forms as aliases in `push_entities`**

In `src/storage/wikibase.py`, extend `AsyncWikibase` with an alias setter and call it on create/update. Add method:

```python
    async def set_aliases(self, qid: str, aliases: list[str]) -> None:
        """Add alias strings to an existing Item (idempotent — SDK dedups)."""
        await asyncio.to_thread(self._set_aliases_sync, qid, aliases)

    def _set_aliases_sync(self, qid: str, aliases: list[str]) -> None:
        item = self._wbi.item.get(entity_id=qid)
        for alias in aliases:
            item.aliases.set(language=self._language, values=alias)
        item.write()
```

In `push_entities`, after an owner is created/updated (where `qid_by_entity_id[owner.id]` is set), collect its observed surface forms from `owner.properties` and push them:

```python
            observed = (owner.properties or {}).get("surface_forms", [])
            extra_aliases = [a for a in observed if a and a != owner.name]
            if extra_aliases:
                try:
                    await wb_client.set_aliases(
                        qid_by_entity_id[owner.id], extra_aliases,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "wikibase alias set failed  name={n}  err={e}",
                        n=owner.name, e=exc,
                    )
```

Add `CANONICAL_LINKER_ENABLED: bool = False` to `src/config.py` (the linker is wired into the merge_and_resolve activity in a follow-up; this task ships the building blocks + alias storage only).

- [ ] **Step 6: Run storage + graph suites for no regression**

Run: `pytest tests/test_storage/ tests/test_graph/test_canonical_linker.py -v`
Expected: PASS (existing Wikibase tests mock `AsyncWikibase`; the new method is additive)

- [ ] **Step 7: Commit**

```bash
git add src/graph/canonical_linker.py tests/test_graph/test_canonical_linker.py src/storage/wikibase.py src/config.py
git commit -m "feat(linking): Wikibase alias storage + embedding-based canonical mention linker (flagged)"
```

---

## Out of scope (proposed as Plan #2 — Agentic Search Quality)

Tracked separately because Search is an independent subsystem:
- Query decomposition + parallel sub-query fan-out (biggest lever for analytical depth).
- Multi-hop graph retrieval (raise `path_depth`; dedicated graph-walk tool).
- GraphRAG **global search**: Leiden community detection + community summaries + map-reduce for corpus-wide analytical answers.
- Unified graph+vector reranking; stronger synthesis-role model.

## Self-Review notes
- **Spec coverage:** A (GLiNER) → Tasks 1–2; B (identifiers) → Task 3; C (judge perf) → Tasks 4–5; D (Wikibase linking) → Task 6; E (GLiNER decision via tests) → Task 1 harness + Task 2 Step 6 gate; F (search) → explicitly deferred to Plan #2.
- **GLiNER is never blind-switched** — `build_kg_extractor` gains opt-in modes; the default mode is untouched.
- **Type consistency:** `CanonicalCandidate` fields (`qid`, `name`, `score`) used identically in linker + tests; `_verdict_key`/`_partition_cached` signatures match across Task 5 test and impl; `NERStats`/`score_case` match across Task 1 test and impl.

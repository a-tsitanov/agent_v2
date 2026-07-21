# Analytics Query Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the analytical-query layer (`/api/v1/analyze`) so "главные темы дня / самые упоминаемые сущности / новые события" returns real entities + clean events instead of empty top-entities and identifier/number noise.

**Architecture:** Two root causes, both in the analytics layer (backend-agnostic Python, no graph-op/nGQL changes needed):
1. The planner LLM over-fills the primitive `type` param with a bogus value (`type="entity"` — not a real entity label), which zeroes any type-filtered result. Fix: coerce an invalid `type` to `None` (= all types) centrally in `parse_plan`.
2. Entity-listing primitives (`new_events`, `top_entities_by_mentions`) emit identifier/degenerate nodes (URLs, "60%", "Concept", "занимает должность"). Fix: a reusable quality gate that drops identifier-typed + degenerate-named rows, applied in the primitives (Python, on the returned row dicts).

**Tech Stack:** Python 3.12, pydantic v2, pytest (`uv run --extra dev pytest`). Analytics layer: `src/analytics/`.

## Global Constraints

- Canonical entity types are the `EntityType` Literal in `src/graph/schema.py` (12 real: Person, Organization, Location, Role, Concept, Topic, Metric, Product, Document, Issue, Resolution, EventOrAction; 12 identifier: Email, PhoneNumber, PostalAddress, DocumentDate, Amount, ContractNumber, OrderNumber, InvoiceNumber, INN, OGRN, BIC, BankAccount). Matching is case-insensitive but the canonical stored casing is TitleCase ("Organization").
- Identifier types = `src/analytics/ids.py::ID_TYPES` (the 12 identifier types above).
- Do NOT touch the nGQL/Cypher graph-ops — all fixes are in the analytics Python layer, operating on row dicts (`{"name":..., "type":..., "mentions"/"created_at":...}`).
- No behavior change for correct planner calls: a valid `type` (e.g. "Organization") must still pass through unchanged; `type=None` stays "all types".
- Backend is Nebula; entity type is exposed to the analytics layer as the row `"type"` key (already mapped by the graph-ops).
- Tests: `uv run --extra dev pytest`.

## Out of scope (deferred — note, do not build)

- Dispatch layer routing analytical NL questions from `/search` to `/analyze` (product decision — user explicitly does NOT want search+analytics merged; a thin intent classifier is a separate future plan).
- "Metric/Amount as attribute of an event" role modelling (bigger design change).
- KG-extraction quality (entities created from verb-phrases like "занимает должность") — an ingest/extractor issue, not analytics.
- Ranking `top_entities_by_mentions` by degree instead of mention_count (separate tuning).

---

### Task 1: Coerce invalid planner `type` → None

**Files:**
- Modify: `src/analytics/ids.py` (add `VALID_ENTITY_TYPES` + `coerce_entity_type`)
- Modify: `src/analytics/planner.py:69-73` (apply coercion in `parse_plan`)
- Test: `tests/test_analytics/test_type_coercion.py`

**Interfaces:**
- Consumes: `src.graph.schema.EntityType`.
- Produces: `VALID_ENTITY_TYPES: frozenset[str]` (lowercased canonical names), `coerce_entity_type(v: str | None) -> str | None` (returns the canonical-cased type if `v` case-insensitively matches a known type, else `None`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_type_coercion.py
from src.analytics.ids import coerce_entity_type
from src.analytics.planner import parse_plan


def test_coerce_valid_type_canonicalizes():
    assert coerce_entity_type("organization") == "Organization"
    assert coerce_entity_type("Person") == "Person"


def test_coerce_invalid_type_to_none():
    assert coerce_entity_type("entity") is None      # the LLM's bogus value
    assert coerce_entity_type("сущность") is None
    assert coerce_entity_type("") is None
    assert coerce_entity_type(None) is None


def test_parse_plan_strips_bogus_type():
    # planner emitted type="entity" — must be coerced to None so the
    # primitive filters on all types instead of matching zero.
    raw = '{"route":"catalog","steps":[{"primitive":"top_entities_by_mentions","params":{"type":"entity","top_n":10}}],"reason":"x"}'
    plan = parse_plan(raw, max_steps=3)
    assert len(plan.steps) == 1
    assert plan.steps[0].params.get("type") is None
    assert plan.steps[0].params.get("top_n") == 10


def test_parse_plan_keeps_valid_type():
    raw = '{"route":"catalog","steps":[{"primitive":"top_entities_by_mentions","params":{"type":"Organization"}}],"reason":"x"}'
    plan = parse_plan(raw, max_steps=3)
    assert plan.steps[0].params.get("type") == "Organization"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_analytics/test_type_coercion.py -v`
Expected: FAIL — `coerce_entity_type` not defined; `parse_plan` keeps `type="entity"`.

- [ ] **Step 3: Add the helper to `ids.py`**

In `src/analytics/ids.py`, add near the top (after the existing imports / `ID_TYPES`):

```python
from src.graph.schema import EntityType

# Canonical entity-type names (lowercased for case-insensitive lookup →
# canonical TitleCase). Sourced from the single EntityType Literal so this
# never drifts from the schema.
_CANON_BY_LOWER: dict[str, str] = {t.lower(): t for t in EntityType.__args__}
VALID_ENTITY_TYPES: frozenset[str] = frozenset(_CANON_BY_LOWER)


def coerce_entity_type(v: str | None) -> str | None:
    """Map a user/LLM-supplied entity type to its canonical casing, or None
    if it is not a real EntityType. Guards against the planner filling the
    `type` param with a bogus value (e.g. "entity"/"сущность") that would
    otherwise match zero rows."""
    if not v:
        return None
    return _CANON_BY_LOWER.get(v.strip().lower())
```

- [ ] **Step 4: Apply coercion in `parse_plan`**

In `src/analytics/planner.py`, add the import (with the other `src.analytics` imports near the top):

```python
from src.analytics.ids import coerce_entity_type
```

Then in `parse_plan`, replace the validated-append block (currently lines 69-73):

```python
            try:
                model = prim.param_model(**params)  # validates required + types
            except Exception:
                continue  # bad params → drop
            dumped = model.model_dump()
            # The planner LLM tends to fill a `type` param with a generic word
            # from the question ("entity"/"сущности"); coerce anything that
            # isn't a real EntityType to None (= all types) so type-filtered
            # primitives don't silently match zero rows.
            if "type" in dumped and dumped["type"]:
                dumped["type"] = coerce_entity_type(dumped["type"])
            validated.append(PrimitiveCall(primitive=name, params=dumped))
            if len(validated) >= max_steps:
                break
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_analytics/test_type_coercion.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add src/analytics/ids.py src/analytics/planner.py tests/test_analytics/test_type_coercion.py
git commit -m "fix(analytics): coerce bogus planner entity type to None (all-types)"
```

---

### Task 2: Quality gate for entity-listing output (identifiers + degenerate names)

**Files:**
- Modify: `src/analytics/ids.py` (add `is_meaningful_entity`)
- Modify: `src/analytics/primitives/events.py` (`NewEventsParams` + `new_events` apply the gate)
- Test: `tests/test_analytics/test_entity_quality_gate.py`

**Interfaces:**
- Consumes: `ID_TYPES`.
- Produces: `is_meaningful_entity(name: str | None, type: str | None, *, exclude_identifiers: bool = True) -> bool` — False for identifier-typed rows (when excluding), degenerate names (empty, pure number/percent/amount, single char, or name equal to its own type), else True.
- `NewEventsParams` gains `exclude_identifiers: bool = True`; `new_events` filters its returned rows through `is_meaningful_entity`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_entity_quality_gate.py
from src.analytics.ids import is_meaningful_entity


def test_drops_identifier_types():
    assert not is_meaningful_entity("+7 900 000", "PhoneNumber")
    assert not is_meaningful_entity("822", "Amount")
    assert is_meaningful_entity("822", "Amount", exclude_identifiers=False)  # opt-in override


def test_drops_degenerate_names():
    assert not is_meaningful_entity("60%", "Metric")
    assert not is_meaningful_entity("7,2 трлн", "Metric")
    assert not is_meaningful_entity("Concept", "Concept")   # name == type
    assert not is_meaningful_entity("X", "Person")          # single char
    assert not is_meaningful_entity("", "Organization")
    assert not is_meaningful_entity(None, "Organization")


def test_keeps_real_entities():
    assert is_meaningful_entity("BAE Systems", "Organization")
    assert is_meaningful_entity("Вячеслав Володин", "Person")
    assert is_meaningful_entity("Дагестан", "Location")
    assert is_meaningful_entity("Anti-Access And Area-Denial", "Concept")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_analytics/test_entity_quality_gate.py -v`
Expected: FAIL — `is_meaningful_entity` not defined.

- [ ] **Step 3: Add the quality gate to `ids.py`**

In `src/analytics/ids.py`:

```python
import re

_ID_TYPES_SET = frozenset(ID_TYPES)
# A name is "number-ish" if, stripped of digits/%/spaces/currency/punctuation,
# nothing meaningful remains (e.g. "60%", "7,2 трлн" → drop; "822" → drop).
_NUMERISH_RE = re.compile(r"^[\d\s.,%€$₽+–—-]*(?:трлн|млрд|млн|тыс|%)?[\d\s.,%€$₽+–—-]*$", re.IGNORECASE)


def is_meaningful_entity(
    name: str | None, type: str | None, *, exclude_identifiers: bool = True
) -> bool:
    """Whether an entity row is worth showing in a themes/events answer.

    Drops: identifier-typed rows (when exclude_identifiers), empty / single-char
    names, pure number/percent/amount names, and names equal to their own type
    label. Keeps genuine named entities/events."""
    if exclude_identifiers and (type or "") in _ID_TYPES_SET:
        return False
    n = (name or "").strip()
    if len(n) < 2:
        return False
    if type and n.lower() == type.lower():
        return False
    if _NUMERISH_RE.match(n):
        return False
    return True
```

- [ ] **Step 4: Apply the gate in `new_events`**

In `src/analytics/primitives/events.py`, add `exclude_identifiers` to `NewEventsParams` (after `type`):

```python
class NewEventsParams(_Params):
    window_days: int | None = None
    type: str | None = None
    exclude_identifiers: bool = True
    top_n: int = 25
```

Update the `new_events` function signature + body. Add `exclude_identifiers: bool = True` to the signature (after `type`), add the import at the top:

```python
from src.analytics.ids import clamp_top_n, is_meaningful_entity
```

and replace the entity-filtering block (currently the `if type: ents = [...]` around lines 44-45) with:

```python
    if type:
        ents = [e for e in ents if e.get("type") == type]
    ents = [e for e in ents
            if is_meaningful_entity(e.get("name"), e.get("type"),
                                    exclude_identifiers=exclude_identifiers)]
```

(Keep the existing `params` dict / `rows` assembly; just ensure `params` includes `"exclude_identifiers": exclude_identifiers` for provenance.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_analytics/test_entity_quality_gate.py tests/test_analytics/ -v`
Expected: PASS (quality-gate tests + no regression in existing analytics tests)

- [ ] **Step 6: Commit**

```bash
git add src/analytics/ids.py src/analytics/primitives/events.py tests/test_analytics/test_entity_quality_gate.py
git commit -m "fix(analytics): quality-gate new_events (drop identifiers + degenerate names)"
```

---

### Task 3: Broaden trend-fallback keywords to catch "самые упоминаемые"

**Files:**
- Modify: `src/analytics/planner.py:89-93` (`_TREND_KEYWORDS`)
- Test: `tests/test_analytics/test_trend_fallback.py`

**Interfaces:**
- Consumes: existing `trend_fallback_steps`.
- Produces: no new symbol — `_TREND_KEYWORDS` gains "упоминаем" (covers "самые упоминаемые", "упоминаемые сущности").

**Rationale:** defense-in-depth — even if the planner LLM plans poorly for "самые упоминаемые сущности", the deterministic fallback (which uses default params, i.e. `type=None`) guarantees the backend-working primitives run. Task 1 already fixes the direct path; this covers the LLM-plans-nothing path for this phrasing.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_analytics/test_trend_fallback.py
from src.analytics.planner import trend_fallback_steps


def test_upominaemye_triggers_fallback():
    steps = trend_fallback_steps("самые упоминаемые сущности за сегодня")
    assert [s.primitive for s in steps] == ["top_entities_by_mentions", "new_events"]


def test_non_trend_no_fallback():
    assert trend_fallback_steps("кто такой Вячеслав Володин") == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --extra dev pytest tests/test_analytics/test_trend_fallback.py -v`
Expected: FAIL — "упоминаем" not in `_TREND_KEYWORDS` (current list has "часто упомин" only), so the first test gets `[]`.

- [ ] **Step 3: Add the keyword**

In `src/analytics/planner.py`, in `_TREND_KEYWORDS`, replace `"часто упомин"` with `"упомин"` (broader — matches "часто упоминается", "самые упоминаемые", "упоминаемые"):

```python
    "тренд", "в тренде", "трендах", "trending", "популярн", "упомин",
    "самое обсуждаемое", "обсуждаем", "на слуху", "хайп", "burst", "surge",
    "most mentioned", "most talked", "most discussed", "hot topic", "hottest",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --extra dev pytest tests/test_analytics/test_trend_fallback.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/analytics/planner.py tests/test_analytics/test_trend_fallback.py
git commit -m "fix(analytics): trend fallback also matches 'упоминаемые' phrasing"
```

---

### Task 4: End-to-end verification + full suite

**Files:**
- Verify only (no new source).

- [ ] **Step 1: Full analytics suite green**

Run: `uv run --extra dev pytest tests/test_analytics/ -q`
Expected: PASS (all new + existing analytics tests). Fix any regression before proceeding.

- [ ] **Step 2: Deploy the analytics-path services (manual, not a code step)**

The analytics workflow runs in the `worker` (search task queue) and is fronted by `api`. Per the deploy model (code baked at build time, no bind-mounts): rebuild + recreate BOTH:

```bash
docker compose -f docker-compose.prod.yml build api worker
docker compose -f docker-compose.prod.yml up -d api worker
```

- [ ] **Step 3: Verify the real query end-to-end**

```bash
docker exec -i agent_v2-api-1 python -c "
import httpx
from src.config import settings as s
key=s.api.keys_list[0]
q='Меня интересуют главные темы дня как самые упоминаемые сущности и новые события в мире'
r=httpx.post('http://localhost:8000/api/v1/analyze', headers={'X-API-Key':key}, json={'query':q,'top_n':20}, timeout=280)
d=r.json(); print(d.get('answer'))
"
```

Expected: top entities now non-empty (real named entities, not "Данные отсутствуют"); new-events section free of URLs / "60%" / bare "Concept" / phone-number noise.

- [ ] **Step 4: Note deferred follow-ups**

Confirm the out-of-scope items are still tracked for the user: dispatch layer (search→analyze), Metric/Amount-as-attribute, KG verb-phrase entities ("занимает должность"), rank-by-degree.

## Notes for the implementer

- `EntityType.__args__` is the tuple of Literal members — that's how Task 1 sources the canonical list without hardcoding.
- The graph-ops already return a `"type"` key per row (mapped from the Nebula `Entity.label`), so the quality gate works on the row dicts without any nGQL change — do NOT edit `events_graph_ops.py` / `aggregations_graph_ops.py`.
- If `tests/test_analytics/__init__.py` is absent and sibling test dirs have one, mirror the convention.

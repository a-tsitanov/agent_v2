# Analytical Layer — Wave 2 Implementation Plan (E2 + Arc 2 + P3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Wave 2 — turn "ask-and-answer" into "the system finds it": structured **LLM event extraction** with cross-document de-duplication (`E2`), **continuous monitoring + alerts** on first_seen/risk via a scheduled sweep (`Arc 2`), and **domain rollups** for the issue/communication domains (`P3`).

**Architecture:** E2 extends the existing LightRAG per-chunk extraction with an `event` tuple kind → `EventOrAction` entity nodes + argument edges, de-duplicated by a new deterministic `(event_type, participants, ts-bucket)` match-key during merge (cross-chunk and cross-document), gated behind `EVENTS_EXTRACTION_ENABLED` (default OFF, dark). Arc 2 mirrors the proven wiki dirty-mark/scheduled-sweep pattern: a one-shot `MonitorSweepWorkflow` (its own `kb-monitor` queue + a Temporal Schedule from a setup script) detects new first_seen edges on watched entities and risk-score rises, persisting `:Alert` records readable via a catalog primitive. P3 adds online rollup primitives over existing Issue/Resolution and CONTACT/RESPONDED_TO relations.

**Tech Stack:** Python 3.12, Temporal (`temporalio`, Schedules), Neo4j via `Neo4jPropertyGraphStore`, LightRAG-style delimited extraction, LlamaIndex LLM via `LLMPool`, pydantic v2 / pydantic-settings, pytest (`asyncio_mode=auto`), ruff. Builds on Wave 0 (`src/analytics/` catalog, E1 first_seen, planner) and Wave 1 (materialized risk_score).

## Global Constraints

- **E2 ships dark.** All event extraction is gated behind `settings.events.extraction_enabled` (default `False`). When off, the extraction path, merge, and Neo4j writes are byte-for-byte unchanged — Wave 2 must not alter live ingest/extraction quality until explicitly enabled. (Extraction quality is a core project track; do not regress it.)
- **Event de-duplication is the load-bearing correctness risk.** The same real-world event re-reported by a later document MUST merge to the existing event node (so its `created_at`/first_seen stays old and it is not re-flagged as new). Use a deterministic match-key `(event_type, sorted participant names, event_ts bucket)`; this is the anti-re-report invariant — every event-merge test asserts it.
- **Two timestamps, kept distinct:** `event_ts` = when the event happened (from text); `created_at` = when we first learned it (ingest, epoch-days, E1). Novelty uses `created_at`; temporal queries use `event_ts`/`valid_from`.
- **Events are a specialization of the entity model**, NOT a parallel store: `:__Entity__:EventOrAction` nodes + argument edges (reuse `PARTICIPATED_IN`/`DATED`/`RESPONDED_TO`/`REPORTED`/`AFFECTS`), so they inherit merge/first_seen/index/communities for free. (The active `lightrag` path derives edge labels from keywords and ignores `RelationType`, so event-argument edges must be emitted EXPLICITLY by the event writer — do not rely on the LLM emitting the right relation keyword.)
- **Arc 2 alerts = persisted records + read primitive.** There is NO notification/push infra (no webhook/Slack/email/Grafana-annotation) and none is in scope. An alert is an idempotent `:Alert` node; a push channel is explicitly future work.
- **Arc 2 uses the scheduled-sweep model** (Temporal Schedules exist and are proven by `scripts/setup_wiki_schedule.py`): a one-shot `MonitorSweepWorkflow` fired by a Schedule, NOT a long-lived signal loop.
- **Determinism / provenance unchanged (Waves 0/1):** read primitives are fail-soft (`store is None`/error → empty via `src/analytics/store_query.run_rows`); numbers come from Cypher rows; the LLM only verbalizes. Activities never raise across the Temporal boundary.
- **Idempotency:** event MERGE on the event-key; `:Alert` MERGE on an alert-key (no duplicate alerts on re-sweep); first_seen `ON CREATE` stamping unchanged.
- **Conventions (reuse exactly):** catalog primitive = `async def fn(store, *, ...) -> PrimitiveResult` + `register(Primitive(name, fn, param_model, description, tier))`, param models subclass a base with `ConfigDict(extra="ignore")`, `clamp_top_n`, entity label literal `"__Entity__"`, `ID_TYPES` from `src/analytics/ids`, NULL-safe polarity filter `(r.polarity IS NULL OR r.polarity <> 'negated')`. Frozen contracts mirror `src/workflow/contracts.py` (`_Frozen`).
- **Quality gates** (before every commit): `uv run ruff check <changed files>` · `uv run ruff format <changed files>` · the task's pytest. ruff: line-length 100, py312, ruleset `E,F,I,B,UP,SIM,RUF` (no `# noqa: BLE001`; `# noqa: F401` on import-for-side-effect is legitimate). Cyrillic allowed. New env vars get a Russian entry in `scripts/make_env.py::_ENV_DESCRIPTIONS`.
- **Git:** commit locally on `worktree-anal`; never push, never `main`. Controller commits at phase checkpoints.

---

## Codebase-grounded facts (verified — build on these)

1. **LightRAG output format** (`src/graph/lightrag_prompts.py`): line-oriented tuples; `TUPLE_DELIM = "<|#|>"`, `COMPLETE_DELIM = "<|COMPLETE|>"`. Entity line: `entity<|#|>name<|#|>type<|#|>description`. Relation line: `relation<|#|>src<|#|>tgt<|#|>keywords<|#|>description<|#|>polarity<|#|>temporal`.
2. **Parser** (`src/graph/lightrag_parse.py`): `parse_lightrag_output(raw, *, source_chunk_id, file_path, tuple_delimiter, completion_delimiter) -> ParseResult`. `_LEADING_KIND_RE` (line 36) recognizes ONLY `entity`/`relation`; dispatch at lines 178-196. `ParseResult` (`entities: list[EntityNode]`, `relations: list[ParsedRelation]`). `_normalize_entity_name` (39-55) title-cases ASCII, preserves Cyrillic. To add events: extend the regex + add an `event` branch + add an `events` container.
3. **Extractor** (`src/graph/lightrag_extract.py`): `LightRAGExtractor` (line 96); `_aextract` (154-255) builds the prompt with `entity_types`, calls `_chat` (→`llm.achat`), parses, resolves names→ids (`id_by_name`), builds `Relation`s via `parsed_relations_to_relations` (262-298). `_default_entity_types` (87-93) = `list(get_args(EntityType))`.
4. **Merge** (`src/graph/merge.py`): `merge_kg_extraction(nodes, llm, *, ...) -> (list[EntityNode], list[Relation])` (162). Entity key = `_normalize_entity_name(name)` (199); `_EntityAgg` (59-69) with `type_votes: Counter` + `most_common`. Relation key = `tuple(sorted([src_name, tgt_name]))` (217-249); `_RelationAgg` (71-83) with `polarity_votes`, temporal min/max widening (307-308). No structural/temporal MATCH-keying exists — event keying is net-new.
5. **Write path** (`src/workflow/activities/build_property_graph.py`): `upsert_nodes(entities)` (103) + `upsert_relations(relations)` (106) — event nodes/edges flow through these as extra entities/relations (no new persistence plumbing). `stamp_first_seen` (117-133, gated). Index DDL via `ensure_*` (142-145).
6. **merge_and_resolve activity** (`src/workflow/activities/merge_and_resolve.py`): `merge_kg_extraction` (114) → `consolidate_phone_entities` (127) → `resolve_entities` (138-166, gated `settings.agent.er_enabled`) → staging `(entities, relations, nodes)` tuple (173).
7. **Schema** (`src/graph/schema.py`): `EventOrAction` ∈ `EntityType` (38). Relations present: `PARTICIPATED_IN` (70), `DATED` (76), `RESPONDED_TO` (72), `REPORTED` (83), `RESOLVED_BY` (84), `AFFECTS` (85), `CONTACT` (65), `AMOUNT_OF` (75). Domain triples: `REPORTED` Person→Issue, `RESOLVED_BY` Issue→Resolution, `AFFECTS` Issue→Product, `CONTACT` Person→Phone/Email, `RESPONDED_TO` Person→Person.
8. **Wiki sweep pattern** (the Arc-2 template): `WikiSweepWorkflow` (`src/workflow/wiki/wiki_sweep.py:98-122`) is ONE-SHOT (select dirty → process → return). Dirty = Neo4j props (`src/graph/wiki_dirty.py`: `_MARK`/`_SELECT`/`_CLEAR` set/read `e.wiki_dirty`/`wiki_dirty_at`). Schedule created by `scripts/setup_wiki_schedule.py:33-40` (`client.create_schedule(id, Schedule(action=ScheduleActionStartWorkflow(WikiSweepWorkflow.run, id=..., task_queue=...), spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=interval)])))`). Admin kick: `POST /admin/wiki/rebuild` (`src/api/routes/admin.py:14-28`).
9. **Worker** (`src/workflow/worker.py`): `WORKER_GROUPS` (80-89) includes `"wiki"`; `_build_worker` `wiki` branch (228-235): `Worker(client, task_queue=settings.wiki.task_queue, workflows=[WikiSweepWorkflow], activities=[...], max_concurrent_activities=...)`. `WikiSettings` (`config.py:531-558`) = the `MonitorSettings` template (enabled, task_queue, activity_concurrency, sweep_batch, sweep_interval_minutes).
10. **Arc-2 inputs:** `new_events`/`entity_new_connections` (`src/analytics/primitives/events.py`, E1); `risk_score` materialized on `e.risk_score`/`risk_band`/`risk_components` by `materialize_risk` (`src/workflow/analytics/materialize_activities.py`). ⚠️ `e.risk_score` is overwritten in place each run → "risk rose" needs a `risk_score_prev` snapshot (added in Task 8). No watchlist exists → net-new `e.watched` flag.
11. **Postgres** has only `documents` + `ingest_metrics` (no alerts table). Alerts will be **Neo4j `:Alert` nodes** (cohesive with the graph, readable via a catalog primitive) — not a new Postgres table.
12. **Tests:** parser → `tests/test_graph/test_lightrag_parse.py` (`_build` delimited payload). extractor → `tests/test_graph/test_lightrag_extract.py` (`_ScriptedLLM.achat`). merge → `tests/test_graph/test_merge.py` (`_StubLLM`, majority-vote/temporal assertions). primitives → `_FakeStore` in `tests/test_analytics/conftest.py`. workflow → Temporal time-skip env (`tests/test_workflow/test_materialize_workflow.py`). ⚠️ Naming: E1 "new_events" (`primitives/events.py`) is distinct from E2 LLM events — name E2 modules `event_*` to avoid collision.

---

## File Structure

**New:**
```
src/graph/event_extract.py          # E2: ParsedEvent + parse_events (extends lightrag parse) + events→nodes/edges
src/graph/event_merge.py            # E2: event match-key + cross-doc/cross-chunk event de-dup (mirrors merge.py aggs)
src/analytics/primitives/events_llm.py   # E2 reads: event_dossier, event_timeline
src/graph/alerts.py                 # Arc 2: :Alert MERGE/read helpers + e.watched watchlist flag helpers
src/workflow/monitor/__init__.py
src/workflow/monitor/activities.py  # Arc 2: detect_alerts activity (+ MONITOR_ACTIVITIES)
src/workflow/monitor/workflow.py    # Arc 2: MonitorSweepWorkflow (one-shot)
src/analytics/primitives/alerts.py  # Arc 2 read: alerts primitive
src/analytics/primitives/domain.py  # P3: issue_resolution_stats, communication_stats
scripts/setup_monitor_schedule.py   # Arc 2: Temporal Schedule (mirror setup_wiki_schedule.py)
```

**Modified:**
```
src/config.py                         # EventsSettings: extraction_enabled, taxonomy; new MonitorSettings; (risk snapshot needs no config)
src/graph/lightrag_prompts.py         # add the `event` tuple format + a few-shot example (behind the event-enabled prompt assembly)
src/graph/lightrag_parse.py           # extend _LEADING_KIND_RE + dispatch + ParseResult.events
src/graph/lightrag_extract.py         # emit event nodes + argument edges when events present
src/workflow/activities/merge_and_resolve.py   # call event_merge (gated) between merge_kg_extraction and ER
src/workflow/analytics/materialize_activities.py  # materialize_risk: snapshot e.risk_score → e.risk_score_prev
src/workflow/worker.py                # register MonitorSweepWorkflow ("monitor" group)
src/api/routes/admin.py (or graph_admin.py)  # POST /admin/monitor/sweep + watchlist add/remove
src/analytics/primitives/__init__.py  # import events_llm, alerts, domain
```

**Tests:** `tests/test_graph/{test_event_extract.py, test_event_merge.py, test_alerts.py}`, `tests/test_analytics/{test_events_llm.py, test_alerts_primitive.py, test_domain.py, test_catalog_complete.py(extend)}`, `tests/test_workflow/{test_monitor_activities.py, test_monitor_workflow.py}`.

---

## Phase A — Config

### Task 1: EventsSettings (extraction) + MonitorSettings

**Files:** Modify `src/config.py`, `scripts/make_env.py`; Test `tests/test_analytics/test_config_wave2.py`

**Interfaces — Produces:** `settings.events.extraction_enabled: bool` (default False), `.taxonomy: list[str]`; `settings.monitor` → `MonitorSettings(enabled: bool=False, task_queue: str="kb-monitor", activity_concurrency: int=2, sweep_interval_minutes: int=30, new_window_days: int=7, risk_rise_delta: float=0.1)`.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_config_wave2.py
from src.config import settings


def test_events_extraction_defaults_off():
    assert settings.events.extraction_enabled is False
    assert "deal" in settings.events.taxonomy or len(settings.events.taxonomy) >= 1


def test_monitor_settings_defaults():
    m = settings.monitor
    assert m.enabled is False and m.task_queue == "kb-monitor"
    assert m.sweep_interval_minutes >= 1 and 0.0 < m.risk_rise_delta <= 1.0
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — extend `EventsSettings` (Wave 0 class):

```python
    extraction_enabled: bool = Field(
        default=False,
        description="Включить извлечение структурных LLM-событий в extract_kg (E2; по умолчанию выкл — доп. стоимость LLM)",
    )
    taxonomy: list[str] = Field(
        default_factory=lambda: ["deal", "appointment", "lawsuit", "incident",
                                 "payment", "meeting", "sanction"],
        description="Закрытый список типов событий (event_type), с открытым fallback для длинного хвоста",
    )
```

Add a new settings class + accessor:

```python
class MonitorSettings(BaseSettings):
    """Arc 2 continuous monitoring + alerts (scheduled sweep)."""

    model_config = SettingsConfigDict(env_prefix="MONITOR_", env_file=".env", extra="ignore")
    enabled: bool = Field(default=False, description="Включить непрерывный мониторинг/алерты (Arc 2)")
    task_queue: str = Field(default="kb-monitor", description="Очередь воркера монитор-свипа")
    activity_concurrency: int = Field(default=2, ge=1, description="Параллелизм активностей монитора")
    sweep_interval_minutes: int = Field(default=30, ge=1, description="Период Temporal-Schedule монитор-свипа, мин")
    new_window_days: int = Field(default=7, ge=1, description="Окно (дни) для детекта новых first_seen-связей")
    risk_rise_delta: float = Field(default=0.1, gt=0.0, le=1.0, description="Порог роста risk_score для алерта")
```

```python
    @cached_property
    def monitor(self) -> MonitorSettings:
        return MonitorSettings()
```

Add the 9 new env vars (EVENTS_EXTRACTION_ENABLED, EVENTS_TAXONOMY, MONITOR_*) to `scripts/make_env.py::_ENV_DESCRIPTIONS` with Russian text.

- [ ] **Step 4: run → PASS** (`test_config_wave2.py`) + confirm the new vars aren't in `test_every_env_var_has_russian_description`'s missing list. **Step 5: lint.**

---

## Phase B — E2 event extraction

### Task 2: LightRAG `event` tuple — prompt + parser

**Files:** Modify `src/graph/lightrag_prompts.py`, `src/graph/lightrag_parse.py`; Test `tests/test_graph/test_event_extract.py` (part 1)

**Interfaces — Produces:**
- `lightrag_parse.ParsedEvent` dataclass: `event_type: str, trigger: str, participants: list[str], event_ts: str|None, location: str|None, polarity: str, source_chunk_id: str, file_path: str`.
- `ParseResult.events: list[ParsedEvent]` (new field, default `[]`).
- Parser recognizes lines `event<|#|>event_type<|#|>trigger<|#|>participants<|#|>time<|#|>location<|#|>polarity` (participants = `;`-joined names).
- `lightrag_prompts`: an `EVENT_INSTRUCTION` block + one few-shot, appended to the system prompt ONLY when events are requested (a flag the extractor passes).

- [ ] **Step 1: failing test**

```python
# tests/test_graph/test_event_extract.py
from src.graph.lightrag_parse import parse_lightrag_output
from src.graph.lightrag_prompts import TUPLE_DELIM, COMPLETE_DELIM

D, C = TUPLE_DELIM, COMPLETE_DELIM


def _ev_line(*fields):
    return D.join(fields)


def test_parser_recognizes_event_kind():
    raw = "\n".join([
        _ev_line("event", "deal", "signed a contract", "Romashka;Lutik", "2024-03-01", "Moscow", "affirmed"),
        C,
    ])
    res = parse_lightrag_output(raw, source_chunk_id="c1", file_path="f")
    assert len(res.events) == 1
    e = res.events[0]
    assert e.event_type == "deal" and e.participants == ["Romashka", "Lutik"]
    assert e.event_ts == "2024-03-01" and e.polarity == "affirmed"


def test_parser_still_handles_entity_relation_unchanged():
    raw = "\n".join([_ev_line("entity", "Romashka", "Organization", "a firm"), C])
    res = parse_lightrag_output(raw, source_chunk_id="c1", file_path="f")
    assert len(res.entities) == 1 and res.events == []
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — in `lightrag_parse.py`: extend `_LEADING_KIND_RE` to also match `event`; add `@dataclass ParsedEvent`; add `events: list[ParsedEvent]` to `ParseResult`; add a dispatch branch:

```python
@dataclass
class ParsedEvent:
    event_type: str
    trigger: str
    participants: list[str]
    event_ts: str | None
    location: str | None
    polarity: str
    source_chunk_id: str
    file_path: str


def _parse_event(fields: list[str], *, source_chunk_id: str, file_path: str) -> ParsedEvent | None:
    # fields: event, event_type, trigger, participants(;), time, location, polarity
    if len(fields) < 3:
        return None
    parts = [p.strip() for p in (fields[3] if len(fields) > 3 else "").split(";") if p.strip()]
    return ParsedEvent(
        event_type=(fields[1] or "").strip() or "event",
        trigger=(fields[2] or "").strip(),
        participants=parts,
        event_ts=(fields[4].strip() or None) if len(fields) > 4 else None,
        location=(fields[5].strip() or None) if len(fields) > 5 else None,
        polarity=_normalize_polarity(fields[6]) if len(fields) > 6 else "affirmed",
        source_chunk_id=source_chunk_id,
        file_path=file_path,
    )
```

In the dispatch loop add `elif kind == "event": ev = _parse_event(fields, ...); if ev: events.append(ev)`. In `lightrag_prompts.py` add `EVENT_INSTRUCTION` (the tuple spec + a one-line few-shot) and a way to append it (a function or a flag-formatted section). Keep the entity/relation prompt unchanged when events are off.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

> Implementer note: confirm `_normalize_polarity`/`_LEADING_KIND_RE`/`ParseResult` exact definitions in `lightrag_parse.py` before editing; match the file's existing dataclass + parse-helper style. The event prompt section must only be injected when the caller requests events (Task 3) so default extraction output is unchanged.

---

### Task 3: Extractor — emit event nodes + argument edges (gated)

**Files:** Modify `src/graph/lightrag_extract.py`; Create `src/graph/event_extract.py`; Test `tests/test_graph/test_event_extract.py` (part 2)

**Interfaces — Produces:**
- `event_extract.events_to_graph(events: list[ParsedEvent], *, id_by_name: dict[str,str]) -> tuple[list[EntityNode], list[Relation]]` — one `EntityNode(label="EventOrAction", name=<stable event name>, properties={event_type, trigger, event_ts, polarity, source_chunks, file_paths})` per event + argument `Relation`s: `PARTICIPATED_IN` (event→participant, resolving participant names to ids, synthesizing orphan entities if needed), `DATED` when `event_ts`.
- `LightRAGExtractor`: when `settings.events.extraction_enabled`, request events in the prompt (Task 2 section), parse them, call `events_to_graph`, and append the event nodes/edges to the chunk's `KG_NODES_KEY`/`KG_RELATIONS_KEY`. When disabled → no change.
- Event node `name` = a stable surface string, e.g. `f"{event_type}: {trigger}"[:120]` (the merge key in Task 4 does the real dedup; the name just needs to be deterministic per (type,trigger)).

- [ ] **Step 1: failing test**

```python
# tests/test_graph/test_event_extract.py  (append)
from src.graph.event_extract import events_to_graph
from src.graph.lightrag_parse import ParsedEvent


def test_events_to_graph_builds_event_node_and_participant_edges():
    ev = ParsedEvent(event_type="deal", trigger="signed", participants=["Romashka", "Lutik"],
                     event_ts="2024-03-01", location=None, polarity="affirmed",
                     source_chunk_id="c1", file_path="f")
    nodes, rels = events_to_graph([ev], id_by_name={"Romashka": "id-r", "Lutik": "id-l"})
    assert len(nodes) == 1 and nodes[0].label == "EventOrAction"
    assert nodes[0].properties["event_type"] == "deal"
    rel_types = {r.label for r in rels}
    assert "PARTICIPATED_IN" in rel_types          # event→participants
    assert sum(1 for r in rels if r.label == "PARTICIPATED_IN") == 2
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `event_extract.py` (`events_to_graph` building `EntityNode`/`Relation` objects, resolving participant names via `id_by_name`, synthesizing an orphan `EntityNode(label="Other")` for unknown participants — mirror `ensure_orphan_entities` in `lightrag_extract.py`). Then wire into `LightRAGExtractor._aextract`: behind `settings.events.extraction_enabled`, add the event prompt section (Task 2), parse `res.events`, call `events_to_graph(res.events, id_by_name=...)`, extend the node's KG nodes/relations. Keep everything unchanged when disabled.

- [ ] **Step 4: run → PASS** + add a gated-off test asserting that with `extraction_enabled=False` the extractor output has no event nodes (monkeypatch the setting). **Step 5: lint.**

> Implementer note: `EntityNode`/`Relation` are llama_index types already used by `lightrag_extract.py` — reuse its `_cypher_safe_label`, `parsed_relations_to_relations`, and orphan-synthesis helpers. Confirm `id_by_name` is available at the call site (it is built in `_aextract`).

---

### Task 4: Event de-duplication match-key (merge) — the anti-re-report invariant

**Files:** Create `src/graph/event_merge.py`; Test `tests/test_graph/test_event_merge.py`

**Interfaces — Produces:**
- `event_key(event_type: str, participants: list[str], event_ts: str | None, *, bucket_days: int = 7) -> tuple` — deterministic key: `(event_type.lower(), frozenset(normalized participant names), ts_bucket)` where `ts_bucket` = the event_ts truncated to a `bucket_days`-window (None → a sentinel). Uses `_normalize_entity_name`.
- `merge_events(event_nodes: list[EntityNode], event_rels: list[Relation]) -> tuple[list[EntityNode], list[Relation]]` — collapse event nodes sharing an `event_key` into ONE node (majority event_type, widened source_chunks, earliest event_ts), rewriting argument edges to the canonical event id. Mirrors `merge.py`'s `_EntityAgg`/Counter/`most_common` scaffolding but keyed by `event_key`.

- [ ] **Step 1: failing test** (the core anti-re-report test)

```python
# tests/test_graph/test_event_merge.py
from src.graph.event_merge import event_key, merge_events
from llama_index.core.graph_stores.types import EntityNode


def test_event_key_is_participant_order_insensitive_and_ts_bucketed():
    k1 = event_key("Deal", ["Romashka", "Lutik"], "2024-03-01")
    k2 = event_key("deal", ["Lutik", "Romashka"], "2024-03-03")   # reordered + within bucket
    assert k1 == k2                                                # same event
    k3 = event_key("deal", ["Romashka", "Lutik"], "2024-09-01")   # far ts → different
    assert k1 != k3


def _ev(name, src_chunk):
    return EntityNode(name=name, label="EventOrAction",
                      properties={"event_type": "deal", "trigger": "signed",
                                  "event_ts": "2024-03-01", "participants": ["Romashka", "Lutik"],
                                  "source_chunks": [src_chunk]})


def test_merge_collapses_same_event_from_two_docs():
    # same event re-reported in a second document → ONE node, source_chunks merged
    nodes, _ = merge_events([_ev("deal: signed", "c1"), _ev("deal: signed", "c2")], [])
    assert len(nodes) == 1
    assert set(nodes[0].properties["source_chunks"]) == {"c1", "c2"}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `event_merge.py`:

```python
# src/graph/event_merge.py
"""E2 event de-duplication: deterministic (type, participants, ts-bucket) match-key."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from llama_index.core.graph_stores.types import EntityNode, Relation

from src.graph.lightrag_parse import _normalize_entity_name

_NO_TS = "∅"


def _ts_bucket(event_ts: str | None, bucket_days: int) -> str:
    if not event_ts:
        return _NO_TS
    # ISO YYYY-MM-DD → bucket index by ordinal // bucket_days; tolerant of partial dates
    from datetime import date

    try:
        d = date.fromisoformat(event_ts[:10])
    except ValueError:
        return event_ts[:7]  # fall back to month string
    return str(d.toordinal() // max(bucket_days, 1))


def event_key(event_type: str, participants: list[str], event_ts: str | None,
              *, bucket_days: int = 7) -> tuple:
    parts = frozenset(_normalize_entity_name(p) for p in participants if p)
    return ((event_type or "event").strip().lower(), parts, _ts_bucket(event_ts, bucket_days))


def merge_events(event_nodes: list[EntityNode], event_rels: list[Relation],
                 *, bucket_days: int = 7) -> tuple[list[EntityNode], list[Relation]]:
    groups: dict[tuple, list[EntityNode]] = defaultdict(list)
    canonical_id: dict[str, str] = {}  # old node id/name -> canonical
    for n in event_nodes:
        p = n.properties or {}
        k = event_key(p.get("event_type", ""), p.get("participants", []) or [],
                      p.get("event_ts"), bucket_days=bucket_days)
        groups[k].append(n)
    merged: list[EntityNode] = []
    for k, members in groups.items():
        first = members[0]
        chunks: list[str] = []
        type_votes: Counter = Counter()
        ts_vals: list[str] = []
        for m in members:
            mp = m.properties or {}
            chunks += list(mp.get("source_chunks", []) or [])
            type_votes[mp.get("event_type", "event")] += 1
            if mp.get("event_ts"):
                ts_vals.append(mp["event_ts"])
            canonical_id[m.name] = first.name
        props = dict(first.properties or {})
        props["event_type"] = type_votes.most_common(1)[0][0]
        props["source_chunks"] = list(dict.fromkeys(chunks))
        props["event_ts"] = min(ts_vals) if ts_vals else props.get("event_ts")
        merged.append(EntityNode(name=first.name, label="EventOrAction", properties=props))
    # rewrite argument edges to the canonical event node
    out_rels: list[Relation] = []
    for r in event_rels:
        src = canonical_id.get(r.source_id, r.source_id)
        tgt = canonical_id.get(r.target_id, r.target_id)
        out_rels.append(Relation(label=r.label, source_id=src, target_id=tgt,
                                 properties=dict(r.properties or {})))
    return merged, out_rels
```

> Implementer note: confirm the exact `EntityNode`/`Relation` import path used elsewhere (`src/graph/merge.py`) and match it. Confirm event nodes carry a `participants` property (set in Task 3's `events_to_graph`) so the key can be rebuilt at merge — if Task 3 stores participants only as edges, add a `participants` list property to the event node so `merge_events` can key on it without a graph round-trip.

- [ ] **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 5: Wire event extraction + dedup into ingest (gated, dark)

**Files:** Modify `src/workflow/activities/merge_and_resolve.py`; Test `tests/test_workflow/test_merge_and_resolve_events.py`

**Interfaces — Consumes:** `event_merge.merge_events`. **Behavior:** after `merge_kg_extraction`, if `settings.events.extraction_enabled`, split event nodes (`label=="EventOrAction"`) from the merged entities, run `merge_events` on them (+ their argument edges), and recombine into the `(entities, relations, nodes)` staging tuple. When disabled → unchanged.

- [ ] **Step 1: failing test** (gated behavior + dedup passthrough)

```python
# tests/test_workflow/test_merge_and_resolve_events.py
# Assert: with extraction_enabled, EventOrAction nodes pass through merge_events (dedup);
# with it disabled, the merge_and_resolve event-merge branch is a no-op.
# Mirror the existing tests/test_workflow/test_merge_and_resolve.py harness (stub merge_kg_extraction).
```

(Write concrete assertions mirroring `test_merge_and_resolve.py`: monkeypatch `settings.events.extraction_enabled`, feed merged entities containing two same-key EventOrAction nodes, assert one survives when enabled / both pass untouched by the event branch when disabled.)

- [ ] **Step 2: run → FAIL.** **Step 3: implement** the gated branch in `merge_and_resolve` (split EventOrAction nodes, `merge_events`, recombine; leave ER as-is). **Step 4: run → PASS.** **Step 5: lint.**

> Implementer note: keep the event-merge branch entirely inside `if settings.events.extraction_enabled:` so the default ingest path is unchanged. Event nodes should NOT go through `resolve_entities` ER (their dedup is `merge_events`); exclude `EventOrAction` from the ER `eligible_labels` set if ER would otherwise touch them (verify `ERConfig.eligible_labels` excludes it — it likely already restricts to specific labels).

---

## Phase C — E2 read primitives

### Task 6: `event_dossier` + `event_timeline`

**Files:** Create `src/analytics/primitives/events_llm.py`; Test `tests/test_analytics/test_events_llm.py`

**Interfaces — Produces (catalog primitives):**
- `event_dossier(name, top_n=25)` — one event's actors/time/place/polarity + source chunks: `MATCH (e:__Entity__:EventOrAction {name:$name})` core + `OPTIONAL MATCH (e)-[r]-(n)` actors.
- `event_timeline(entity, window_days=None, top_n=50)` — events a named entity participated in over time (by `event_ts`): `MATCH (p:__Entity__ {name:$entity})-[]-(e:__Entity__:EventOrAction) RETURN e.name, e.event_type, e.event_ts ORDER BY e.event_ts`.

- [ ] **Step 1: failing test**

```python
# tests/test_analytics/test_events_llm.py
import pytest
from src.analytics.primitives import events_llm as ev
from tests.test_analytics.conftest import _FakeStore


@pytest.mark.asyncio
async def test_event_dossier_reads_event_node():
    store = _FakeStore(by_call=[
        [{"name": "deal: signed", "event_type": "deal", "event_ts": "2024-03-01", "polarity": "affirmed"}],
        [{"rel": "PARTICIPATED_IN", "name": "Romashka"}],
    ])
    res = await ev.event_dossier(store, name="deal: signed")
    assert res.rows[0]["core"]["event_type"] == "deal"
    assert ":EventOrAction" in res.cypher


@pytest.mark.asyncio
async def test_event_timeline_orders_by_event_ts():
    store = _FakeStore(rows=[{"name": "deal: signed", "event_type": "deal", "event_ts": "2024-03-01"}])
    res = await ev.event_timeline(store, entity="Romashka")
    assert "event_ts" in res.cypher and res.params["entity"] == "Romashka"
```

- [ ] **Step 2: run → FAIL.** **Step 3: implement** both primitives (mirror `entity_dossier`/`relationship_timeline` from Wave 0; fail-soft, clamp, register). **Step 4: run → PASS.** **Step 5: lint.**

---

## Phase D — Arc 2 monitoring + alerts

### Task 7: Alert store + watchlist helpers (`alerts.py`)

**Files:** Create `src/graph/alerts.py`; Test `tests/test_graph/test_alerts.py`

**Interfaces — Produces (sync Cypher helpers, called off-loop from activities):**
- `ALERT_KEY` convention: `kind:entity:detail_hash` so re-sweeps don't duplicate.
- `upsert_alert(store, *, kind, entity, detail, created_at) -> None` — `MERGE (a:Alert {key:$key}) ON CREATE SET a.kind,...,a.created_at`.
- `mark_watched(store, names, watched=True) -> None` — `SET e.watched=$watched` on `__Entity__`.
- `read_alerts_cypher` constant used by the read primitive (Task 11).

- [ ] **Step 1: failing test**

```python
# tests/test_graph/test_alerts.py
from src.graph.alerts import upsert_alert, mark_watched, alert_key


class _Rec:
    def __init__(self): self.calls = []
    def structured_query(self, cypher, param_map=None): self.calls.append((cypher, param_map or {})); return []


def test_alert_key_stable_and_dedup_friendly():
    assert alert_key("risk_rise", "Shell", "0.8") == alert_key("risk_rise", "Shell", "0.8")


def test_upsert_alert_merges_on_key():
    s = _Rec(); upsert_alert(s, kind="risk_rise", entity="Shell", detail="0.8", created_at=19900)
    joined = " ".join(c for c, _ in s.calls)
    assert "MERGE (a:Alert" in joined and "ON CREATE" in joined


def test_mark_watched():
    s = _Rec(); mark_watched(s, ["A", "B"])
    assert "e.watched" in s.calls[0][0] and s.calls[0][1]["names"] == ["A", "B"]
```

- [ ] **Step 2: run → FAIL.** **Step 3: implement** (`alert_key` = `f"{kind}:{entity}:{detail}"`; `upsert_alert` MERGE on key + ON CREATE set fields; `mark_watched` UNWIND names SET e.watched; fail-soft try/except WARN). **Step 4: run → PASS.** **Step 5: lint.**

---

### Task 8: Risk-score snapshot for rise detection

**Files:** Modify `src/workflow/analytics/materialize_activities.py` (`materialize_risk`); Test extend `tests/test_workflow/test_materialize_activities.py`

**Interfaces:** `materialize_risk` now snapshots the OLD `e.risk_score` into `e.risk_score_prev` before overwriting, so Arc 2 can detect a rise (`risk_score - risk_score_prev >= delta`). One extra clause in `_RISK_WRITE`: `SET e.risk_score_prev = e.risk_score` BEFORE setting the new `e.risk_score` (Cypher evaluates SET left-to-right within a clause? — use a `WITH` to capture old first).

- [ ] **Step 1: failing test** — assert the risk write captures the prior value into `risk_score_prev`.

```python
# append to tests/test_workflow/test_materialize_activities.py
# Assert the _RISK_WRITE cypher sets risk_score_prev from the existing e.risk_score before overwriting.
```

- [ ] **Step 2: run → FAIL.** **Step 3: implement** — change `_RISK_WRITE` to:

```cypher
UNWIND $rows AS r MATCH (e:__Entity__ {name:r.name})
SET e.risk_score_prev = e.risk_score
SET e.risk_score = r.score, e.risk_band = r.band, e.risk_components = r.components
```

(In Cypher, separate `SET` clauses execute in order, so `risk_score_prev` captures the value before the second SET overwrites `risk_score`.) **Step 4: run → PASS.** **Step 5: lint.**

> Implementer note: verify Neo4j evaluates the two `SET` clauses sequentially (it does — each SET is its own clause). Add a focused test asserting both SETs appear in order in the cypher string.

---

### Task 9: `detect_alerts` activity + `MonitorSweepWorkflow`

**Files:** Create `src/workflow/monitor/__init__.py`, `src/workflow/monitor/activities.py`, `src/workflow/monitor/workflow.py`; Test `tests/test_workflow/test_monitor_activities.py`, `tests/test_workflow/test_monitor_workflow.py`

**Interfaces — Produces:**
- `@activity.defn detect_alerts(p: MonitorIn) -> MonitorResult` — over watched entities: (a) new first_seen edges in-window (reuse the E1 `_NEW_EDGES`-style read filtered to `e.watched`), (b) risk rises (`e.risk_score - coalesce(e.risk_score_prev,0) >= delta` on watched/high entities) → `upsert_alert` per finding; returns counts. Fail-soft.
- `@workflow.defn MonitorSweepWorkflow` (one-shot, mirror `WikiSweepWorkflow`): runs `detect_alerts`, returns the tally.
- Contracts `MonitorIn(window_days, risk_rise_delta)`, `MonitorResult(new_connection_alerts:int, risk_rise_alerts:int, error:str="")` in `src/analytics/contracts.py`.

- [ ] **Step 1: failing tests** — `detect_alerts` writes alerts for a watched entity's new edge + a risk rise (recording fake store); workflow time-skip runs `detect_alerts` (stubbed) and returns the tally.

- [ ] **Step 2: run → FAIL.** **Step 3: implement** the activity (gather watched-entity findings via Cypher, call `upsert_alert`) + the one-shot workflow (mirror `wiki_sweep.py:98-122` + `materialize_workflow.py` retry/timeout idioms). **Step 4: run → PASS.** **Step 5: lint.**

> Implementer note: `detect_alerts` reads `settings.monitor.new_window_days`/`risk_rise_delta` (or the `MonitorIn` values). It only considers `e.watched = true` entities (cheap scope; a future "all high-risk" mode is out of scope). Reuse `today_epoch_days()` for the window. Never raise across the boundary.

---

### Task 10: Worker group + admin endpoint + Schedule setup

**Files:** Modify `src/workflow/worker.py`, `src/api/routes/admin.py` (or `graph_admin.py`); Create `scripts/setup_monitor_schedule.py`; Test `tests/test_workflow/test_monitor_registration.py`, `tests/test_api/test_monitor_route.py`

**Interfaces — Produces:** `"monitor"` ∈ `WORKER_GROUPS` + a `_build_worker` branch registering `MonitorSweepWorkflow` + `MONITOR_ACTIVITIES` on `settings.monitor.task_queue`; `POST /admin/monitor/sweep` (fire-and-forget, mirror `/admin/wiki/rebuild`); `POST /admin/monitor/watch` (add/remove watched entities via `mark_watched`); `scripts/setup_monitor_schedule.py` (mirror `setup_wiki_schedule.py`, interval = `settings.monitor.sweep_interval_minutes`).

- [ ] **Step 1: failing tests** (registration + routes present) → **Step 2: FAIL** → **Step 3: implement** (mirror the wiki branch + admin handlers + schedule script) → **Step 4: PASS** + `python -c "import src.workflow.worker"` and `import src.api.main` → **Step 5: lint.** (Controller runs the full suite.)

> Implementer note: the Schedule script is a standalone `python -m scripts.setup_monitor_schedule` (mirror `setup_wiki_schedule.py` exactly — `client.create_schedule(...)`, idempotent). Don't run it in tests (no live Temporal). Add the new `MONITOR_*` env vars handled in Task 1.

---

### Task 11: `alerts` read primitive

**Files:** Create `src/analytics/primitives/alerts.py`; Test `tests/test_analytics/test_alerts_primitive.py`

**Interfaces — Produces:** `alerts(kind=None, entity=None, window_days=None, top_n=50)` — reads `:Alert` nodes (filter by kind/entity/recency), newest first.

- [ ] Standard primitive task (test asserts `:Alert` read + filters; fail-soft; clamp; register). RED→impl→GREEN→lint.

---

## Phase E — P3 domain rollups

### Task 12: `issue_resolution_stats` + `communication_stats`

**Files:** Create `src/analytics/primitives/domain.py`; Test `tests/test_analytics/test_domain.py`

**Interfaces — Produces:**
- `issue_resolution_stats(top_n=20)` — `MATCH (i:__Entity__:Issue) OPTIONAL MATCH (i)-[:RESOLVED_BY]-(r:__Entity__:Resolution)` → total / unresolved / resolution rate (computed in Python or Cypher).
- `communication_stats(name=None, top_n=20)` — who-talks-to-whom intensity over `CONTACT`/`RESPONDED_TO` (count per pair).
- (Sentiment-over-time is already covered by Wave 0 `polarity_evolution`; numeric by Arc-1 `numeric_rollup` — not re-implemented.)

- [ ] **Step 1: failing tests** (assert RESOLVED_BY / RESPONDED_TO cypher shapes; fail-soft) → **Step 2: FAIL** → **Step 3: implement** (2 primitives + register; confirm relation directions per schema §7) → **Step 4: PASS** → **Step 5: lint.**

---

## Phase F — Integration

### Task 13: Catalog completeness + finalize `__init__` + full gate

**Files:** Modify `src/analytics/primitives/__init__.py`; Test extend `tests/test_analytics/test_catalog_complete.py`

- [ ] Add `event_dossier, event_timeline, alerts, issue_resolution_stats, communication_stats` to `_EXPECTED`; import `events_llm`, `alerts`, `domain` in `primitives/__init__.py`. RED→add imports→GREEN. CATALOG ≥ 41.
- [ ] **Full gate** (controller): `uv run ruff check <changed>` · `uv run ruff format --check <changed>` · `uv run pytest -q` — no new failures beyond the known pre-existing baseline; with `EVENTS_EXTRACTION_ENABLED=false` confirm ingest/extraction tests are unchanged (E2 dark).

---

## Self-Review

**1. Spec coverage**

| Spec item | Task |
|---|---|
| event §1/§2 event model (`:EventOrAction` + args, event_ts vs created_at) | 3 |
| event §3 M2 structured LLM extraction | 2, 3 |
| event §4 event resolution/dedup (the risk) | 4, 5 |
| event §6 `event_dossier` / `event_timeline` | 6 |
| event §9 E2 phasing (gated `EVENTS_EXTRACTION_ENABLED`) | 1, 3, 5 |
| event §3 M3 burst detector / `trending_events` (E3) | **out of scope** → Wave 3 |
| analytical §12 Arc 2 continuous monitoring + alerts | 7–11 |
| Arc 2 watchlist + risk-rise + new-connection alerts | 7, 8, 9 |
| Arc 2 Temporal Schedule (scheduled sweep) | 10 |
| signals §4 Issue/Resolution rollups | 12 |
| signals §4 communications rollups | 12 |
| signals §4 sentiment / numeric rollups | **reused** (Wave 0 `polarity_evolution`, Arc-1 `numeric_rollup`) |
| config (events extraction, monitor) | 1 |
| catalog integration | 13 |

**2. Placeholder scan:** Tasks 5, 6, 9, 10, 11, 12 give interface + behavior + one concrete test sketch rather than full verbatim code for every primitive (they follow the now-established Wave 0/1 catalog template, referenced explicitly). Implementer notes flag the real unknowns to verify (`_LEADING_KIND_RE`/`_normalize_polarity` exact defs; `ERConfig.eligible_labels` excludes EventOrAction; Cypher sequential-SET semantics; participants stored as an event-node property for the merge key). The event-merge and event-key code (the risk) is given in full (Task 4).

**3. Type consistency:** `ParsedEvent` (Task 2) → `events_to_graph` (Task 3) → `merge_events`/`event_key` (Task 4) → `merge_and_resolve` (Task 5). `EntityNode(label="EventOrAction", properties={event_type, trigger, event_ts, participants, source_chunks})` consistent between writer (3) and merger (4) and readers (6). `:Alert {key,kind,entity,detail,created_at}` consistent between `alerts.py` writer (7), `detect_alerts` (9), and the `alerts` read primitive (11). `e.watched` flag consistent (7, 9, 10). `e.risk_score_prev` consistent (8, 9).

**Known boundaries:** E3 burst/`trending_events` → Wave 3. Alert push channels (webhook/Slack) → future. Event ER via embeddings → not used (deterministic key only). E2 ships dark (`EVENTS_EXTRACTION_ENABLED=false`); a live extraction-quality eval of events is a deploy-time step.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-30-analytical-layer-wave2.md`. Two execution options:
1. **Subagent-Driven (recommended)** — fresh subagent per task + per-task review + final whole-branch review.
2. **Inline Execution** — batched with checkpoints.

Which approach?

# E2 Event Time-Frames: raw phrase + deterministic resolver + interval fields

**Date:** 2026-07-05
**Status:** Draft (pending user review)
**Scope:** extraction prompt/parse contract, `event_ts` normalization, `:EventOrAction` schema, event primitives.

## 1. Problem

`event_ts` on `:EventOrAction` nodes is a single free-text string written by the
extraction LLM, stored unvalidated (`lightrag_parse.py` `_parse_event`) and
sorted lexicographically (`event_timeline`). An audit of the freshly ingested
corpus (~295 events, 107 distinct values) shows:

| Category | ~Share | Examples |
|---|---|---|
| honest NULL | 38% | — |
| invented pseudo-dates | ~34% | `2024-XX`, `2024..`, `20XX-MM-DD`, `2024-XX (дата неизвестна)` |
| tuple field misalignment | ~10% | `affirmed` ×16 (polarity), `Константиновка`, `52.164866, 32.929911`, `Бразилия;Норвегия`, full sentences |
| real date, invented year | ~3% | `2024-07-06` for «6 июля» in a 2026 document |
| genuine temporal phrases | ~5% | `6 июля с 12:00 до 18:00 мск`, `1-5 июля`, `на текущей неделе`, `День Независимости США` |

Root causes:
1. The prompt asks the model to output a **normalized ISO date** («ISO date or
   range … leave empty if unknown», `lightrag_prompts.py` EVENT_INSTRUCTION) —
   normalization is exactly what the model does badly: with no date in the
   text it invents one, anchoring on the year from the few-shot example
   (`2024-03-01`), and eager normalization destroys the original phrase.
2. `_parse_event` accepts any string in the ts position — including polarity,
   locations and participant lists that slid over when the model omitted a
   field.
3. Separately: `settings.events.taxonomy` (config) is never injected into the
   prompt and never validated at parse — `event_type` degenerates to the
   generic `event` plus a free-form zoo.

Related decision context: the two-tier normalization discussion concluded that
an LLM second-pass fixes only the ~5% genuine-phrase tail; ~45% of the garbage
is contract violation fixable by prompt + validation, so the deterministic
path ships first (see §7 for the deferred tier-2 criterion).

## 2. Goals / Non-goals

**Goals**
- Event time is stored as a machine-queryable **interval with precision**,
  plus the verbatim source phrase for provenance.
- The LLM only ever copies time words from the text; all date arithmetic is
  deterministic code anchored on the document date.
- Garbage cannot reach the graph: non-temporal strings in the ts position and
  out-of-taxonomy event types are neutralized at parse time.
- Resolve-rate is measurable, so future investment (more rules, LLM tier-2)
  is a data decision.

**Non-goals**
- **LLM tier-2 normalizer** — deferred; see §7 trigger criterion.
- **Backfill of existing events** — the corpus is fresh and small; the plan is
  wipe + re-ingest after merge. No backfill script.
- Taxonomy redesign — only enforcement of the existing configured list.
- Changing burst/trending logic — they key on `created_at` (ingest time) and
  are unaffected.

## 3. Data model (`:EventOrAction` properties)

| Property | Type | Meaning |
|---|---|---|
| `event_ts_raw` | string \| absent | verbatim time phrase from the text, as extracted |
| `event_start_epoch` | int (epoch seconds UTC) \| absent | start of the covering interval |
| `event_end_epoch` | int \| absent | end of the covering interval (inclusive, 23:59:59 for date-granular ends) |
| `event_ts_precision` | `year\|month\|day\|datetime` \| absent | granularity the interval was derived at |

Invariants: `start <= end`; the three resolved fields are set together or not
at all; both-absent ⇒ the event is *untimed* (kept, sorted last). Point events
have `start == end` (or the same day expanded to 00:00:00–23:59:59 for `day`
precision). Partial dates expand to the covering interval: `«2024»` →
[2024-01-01, 2024-12-31] with `precision=year`; `«март»` → the doc-year March
with `precision=month`. For explicit ranges («1-5 июля», «2026–2027 годы»)
`precision` is the granularity of the endpoints (`day`, `year`), not the
interval length.

The legacy `event_ts` string property is no longer written. (Existing nodes
die with the re-ingest wipe; primitives stop reading it — see §6.)

Epoch **seconds** (not days) to represent intra-day phrases like «с 12:00 до
18:00 мск»; consistent with `created_at` stamping elsewhere.

## 4. Extraction contract changes

### 4.1 Prompt (`lightrag_prompts.py` EVENT_INSTRUCTION)

- `event_timestamp` field redefined: *«copy the time expression verbatim from
  the text (e.g. "вчера", "в марте", "6 июля с 12:00"); leave EMPTY if the
  text states no time; NEVER guess or invent a date»*. No ISO requirement.
- Few-shot updated: two event examples — one whose text contains an explicit
  date (copied verbatim), one with no time mention (empty field) — so «empty»
  is demonstrated, not just described, and no single year anchors the model.
- `event_type`: inject the configured closed list — *«one of:
  {settings.events.taxonomy}; use `other` if none fits»*. The instruction is
  built at call time (taxonomy comes from config), not a static constant.

### 4.2 Parse validation (`lightrag_parse.py` `_parse_event`)

- Field-count check: fewer than 7 fields ⇒ ts/location/polarity positions are
  untrusted — set ts to `None` rather than guessing which field slid where.
- ts sanity gate (cheap rejects before storing as raw): value equal to a
  polarity literal (`affirmed|negated|uncertain`) or a known placeholder
  (`empty|unknown|not specified|не указано…`) ⇒ `None`; contains `;` (participant
  list) ⇒ `None`; matches a coordinate pattern ⇒ `None`; longer than 64 chars
  (full sentence) ⇒ `None`.
- polarity position validated against the enum; invalid ⇒ default `affirmed`
  (current behavior) — but no longer able to masquerade as a timestamp.
- `event_type` validated against `settings.events.taxonomy ∪ {other}`
  case-insensitively; anything else ⇒ `other`, original label preserved in
  `event_type_raw` (node property) for taxonomy iteration later.

### 4.3 Resolver (`src/graph/event_ts_resolver.py`, new)

```
resolve(raw: str | None, doc_date_epoch: int | None)
    -> tuple[int, int, str] | None   # (start_epoch, end_epoch, precision)
```

Pure function, no I/O, never raises (any internal error ⇒ `None` + debug log).
Pipeline:

1. **Pre-rules** (regex/string, Russian-first): strip leading prepositions
   («в», «на», «с»); split ranges — «с X до Y», «X–Y июля», «X..Y»; bare
   month(-year) ⇒ month interval; bare year / «YYYY–YYYY годы» ⇒ year
   interval(s); quarters «QN»/«N-й квартал», «первое полугодие» ⇒ quarter /
   half-year intervals; intra-day «с 12:00 до 18:00» ⇒ datetime bounds on the
   resolved day.
2. **dateparser** for the residual point expressions (`languages=['ru','en']`,
   `RELATIVE_BASE = doc_date`): relative words («вчера», «на прошлой неделе»,
   «две недели назад») and explicit dates. Year-less day-month («6 июля»)
   resolves to the candidate **nearest to the document date** (parse with
   past- and future-preference, pick min |delta|) — news text uses both «вчера
   сообщили» and «состоится 6 июля».
3. Anything unresolved ⇒ `None`; the raw phrase remains on the node.

Anchor: the chunk's `doc_date_epoch` metadata (stamped in
`parse_and_chunk.py`); fallback `inserted_at_epoch`. Wired in
`event_extract.events_to_graph` (which receives the source node's metadata) —
the resolved triple is written alongside `event_ts_raw` at node build time.

New optional dependency: `dateparser` (pure-python, offline — no impact on the
air-gapped deploy path; pin in `pyproject.toml`).

### 4.4 Dedup key (`src/graph/event_merge.py`)

`event_key` currently buckets on the `event_ts` string
(`date.fromisoformat(event_ts[:10])`, month-string fallback) and
`merge_events` picks the canonical timestamp as the lexicographic `min()` of
strings — both break under the raw-phrase model. Changes: `_ts_bucket` takes
`event_start_epoch: int | None` (bucket by UTC day ordinal / ISO week);
`event_key(event_type, participants, event_start_epoch, *, bucket_days)`;
`merge_events` selects the member with the smallest `event_start_epoch` and
carries its resolved triple + raw phrase onto the canonical node. Untimed
events keep the `∅` bucket (dedup by type+participants only), as today.
`tests/eval/events_eval.py` feeds golden ISO strings through the resolver
(anchor-free ISO parsing) to build comparable keys.

## 5. Observability

Extraction logs per-batch counters: `events_total`, `ts_present` (raw phrase
survived the sanity gate), `ts_resolved` (by precision), `ts_unresolved`.
`scripts/check_ingestion.py` gains an events section printing the same
counters plus the top-N unresolved raw phrases — that list is the backlog for
new pre-rules and the input to the §7 decision.

## 6. Primitive updates (`src/analytics/primitives/events*.py`)

- `event_timeline`: `ORDER BY e.event_start_epoch IS NULL, e.event_start_epoch
  DESC` (untimed last); `window_days` filters on `event_start_epoch` (falls
  back to `created_at` when absent — current behavior for untimed events);
  returns `event_ts_raw` + resolved fields instead of the legacy string.
- `event_dossier`: surfaces `event_ts_raw`, resolved interval and precision.
- `trending_events` / `new_events` / monitor burst: unchanged (`created_at`).

## 7. Deferred: LLM tier-2 normalizer

Not built now. Trigger criterion: after the corpus stabilizes, if
`ts_unresolved / ts_present > 20%` **and** the top unresolved phrases are not
coverable by cheap pre-rules, spec a batched second-pass call (unresolved
phrases + doc date in, strict «ISO interval or empty» out, output validated by
code, `EVENTS_TS_LLM_FALLBACK` flag default off). Until then the tail stays
untimed by design.

## 8. Testing

TDD throughout (`superpowers:test-driven-development`):

- `tests/test_graph/test_event_ts_resolver.py` — table-driven over the real
  audited phrases: resolvable set («вчера», «6 июля», «1-5 июля», «в марте»,
  «первое полугодие», «с 12:00 до 18:00», explicit dates), reject set
  (coordinates, `affirmed`, participant lists, sentences, `2024-XX`
  pseudo-dates ⇒ `None`), anchor semantics (year-less dates near doc date,
  RELATIVE_BASE), interval expansion + precision, `doc_date_epoch=None`
  fallback.
- `tests/test_graph/test_event_extract.py` — extended: node gets the four new
  properties; misaligned tuples produce untimed events; taxonomy enforcement
  (`other` + `event_type_raw`).
- `tests/test_graph/test_lightrag_parse.py` — ts sanity gate cases.
- `tests/test_analytics/` — `event_timeline` ordering (untimed last) and
  window filtering on the new fields (fake-store rows).
- `tests/eval/events_eval.py` — gains ts resolve-rate reporting so the quality
  gate (still pending) covers time frames.

Regression gate: full suite must stay at the 13 known pre-existing baseline
failures; any new failure is a regression.

## 9. Rollout

Code-only change gated by the existing `EVENTS_EXTRACTION_ENABLED` (now
default-on). After merge: wipe + re-ingest the current corpus (user-driven, as
today) — no migration or backfill code. Old `event_ts` strings disappear with
the wipe; nothing reads them afterwards.

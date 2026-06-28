# Automatic event detection — design

Date: 2026-06-25
Status: **design (draft), pending review**
Branch: `feat/event-detection` (proposed — not yet created)
Companion to: [`2026-06-24-analytical-layer-design.md`](2026-06-24-analytical-layer-design.md)

## Goal

As documents are ingested and the graph forms, **automatically surface
"new events"** — without a manual query. An "event" here is a **union of four
signals** (see Decisions), and **"new" is defined as `first_seen` — first
appearance in the graph**.

This is a **companion to the analytical layer**, not part of it:
- This spec covers the **write side** — extracting/deriving events and
  stamping novelty *during ingest* (touches `extract_kg`, `merge`,
  `build_property_graph`, a small migration).
- The analytical-layer spec covers the **read side** — the event-family
  primitives that *surface* what this spec records, and the Arc 2 continuous
  monitoring/alerts that act on it.

Boundary: *this spec = "extract and mark new while the graph forms"; the
analytical spec = "compute over the graph on demand".*

The encouraging part: almost every building block already exists
(`EventOrAction` entity type + `PARTICIPATED_IN`/`DATED`/`RESPONDED_TO`/
`REPORTED` relations, `extract_kg`'s one-LLM-call-per-chunk, `merge.py` /
`entity_resolution.py`, `(:Chunk)-[:MENTIONS]->(:__Entity__)` with
`doc_date`, communities, Temporal + dirty-marking). The only genuinely new
piece is **creation-time stamping on graph elements**.

## Locked decisions (from brainstorming)

1. **What is an "event"** = the **union of four sources** (all selected):
   - structured **business occurrence with slots** (deal/appointment/lawsuit/
     incident/payment/meeting…),
   - **appearance of a relation/entity** (graph-derived),
   - **topic burst** (mention spike),
   - **open LLM extraction** (no fixed taxonomy).
2. **What is "new/current"** = **`first_seen`** — first appearance in the
   graph, *independent of document date*. This is the keystone that unifies
   all four sources.

## 1. Keystone — `first_seen` / `created_at` stamping (the one migration)

Because novelty = first appearance in the graph, the unifying mechanism is a
**creation-time stamp on every graph element**:

- On **entity** nodes, **relationships**, and **event** nodes, set on
  creation only (never on update):
  - `created_at` — the ingest timestamp (epoch-days or ISO, matching the
    date-filter plumbing's canonical form),
  - `first_doc_id` / `first_chunk_id` — provenance of first appearance.

Write-path change (idempotent upsert in `build_property_graph` / `merge`):
```cypher
MERGE (e:__Entity__ {name: $name})
ON CREATE SET e.created_at = $ingest_ts, e.first_doc_id = $doc_id
SET e.description = $desc, e.mention_count = $mc        -- updates on every pass
```
```cypher
MERGE (a:__Entity__ {name:$a})-[r:OWNS]->(b:__Entity__ {name:$b})
ON CREATE SET r.created_at = $ingest_ts, r.first_doc_id = $doc_id
SET r.weight = $w, r.polarity = $pol                     -- updates on every pass
```

Migration: a one-time backfill stamps existing nodes/edges with a sentinel
`created_at` (e.g. the earliest known ingest, or `0`) so they are never
mis-flagged as "new". New ingests stamp real timestamps from then on.

**This single change makes "graph-derived events" fall out for free** — a new
edge or node *is* a `first_seen` event — and gives every other event source a
uniform novelty test.

## 2. Event model

Events are a **specialization of the existing entity model**, not a parallel
store — so they inherit `merge` / `entity_resolution` / `first_seen` / vector
index / communities for free:

- Node: `:__Entity__:EventOrAction` (the type already exists) enriched with
  `event_type` (typed-core value or open string), `trigger` (the surface
  phrase), `event_ts` (when the event happened, from text), plus the standard
  `created_at`/`first_*`.
- **Argument edges** (reuse existing relation types): `PARTICIPATED_IN`
  (actors/targets), `DATED` (time), plus `AFFECTS`/`REPORTED`/`RESPONDED_TO`
  where they fit; `ADDRESS_OF`/location via the existing identifier/relation
  set.
- `source_chunks` + `polarity` (reported / negated / uncertain) — same as all
  edges.

Two timestamps, kept distinct (no lookahead): **`event_ts`** = when it
happened (from the document text); **`created_at`** = when we first learned it
(ingest). Novelty uses `created_at`; the analytical layer's temporal
primitives use `event_ts`/`valid_from`.

(Alternative considered: a dedicated `:Event` label separate from
`:__Entity__`. Rejected for v1 — it would duplicate the merge/ER/index
machinery. Revisit only if event semantics diverge from entity semantics.)

## 3. The three detection mechanisms

All four event *sources* collapse onto **three mechanisms**, unified by
`first_seen`:

### M1 — `first_seen` scan (gives graph-derived events for free)
A read over elements created in the current window — no extraction needed:
```cypher
-- new entities
MATCH (e:__Entity__) WHERE e.created_at >= $since
RETURN e.name, [l IN labels(e) WHERE l<>'__Entity__'][0] AS type, e.created_at, e.first_doc_id
-- new relationships (a "new connection" event)
MATCH (a:__Entity__)-[r]->(b:__Entity__) WHERE r.created_at >= $since
RETURN a.name, type(r) AS rel, b.name, r.created_at, r.first_doc_id
```
Covers the **"appearance of a relation/entity"** source directly.

### M2 — structured LLM extraction (business occurrences + open tail)
Extend `extract_kg` (LightRAG prompt) to emit, per chunk, an **event list**:
`{ event_type (typed core OR open), trigger, participants[], time, location,
polarity }`. Typed core = a closed taxonomy (deal/appointment/lawsuit/
incident/payment/meeting/sanction…) with a free `event_type` fallback for the
long tail — this single path serves **both** "business occurrence with slots"
**and** "open LLM extraction". Output → `EventOrAction` nodes + argument edges
in `build_property_graph`. Novelty = the resolved event node's `created_at` is
in-window.

### M3 — burst detector (trending events)
Offline detector over the mention time-series
(`(:Chunk)-[:MENTIONS]->(:__Entity__)` bucketed by `doc_date`): z-score /
Kleinberg burst. A detected onset writes a small burst-state record (e.g.
`:BurstEvent {entity, onset_ts, score, created_at}`); novelty = the burst is
first detected (its `created_at`). Covers the **"topic burst"** source.

## 4. Event resolution / dedup (the re-report problem)

Critical for `first_seen` to mean anything: the **same real-world event
re-reported by a later document must merge to the existing event node** — so
its `created_at` stays old and it is **not** re-flagged as new.

- Reuse `merge.py` / `entity_resolution.py`, with an **event-specific match
  key**: `event_type` + sorted participant set + `event_ts` proximity (fuzzy
  on time). This is harder than entity ER (needs time/argument matching) and
  is the main design risk (see §8).
- Cross-chunk events within one document merge during `merge_and_resolve`
  (existing); cross-document/cross-ingest events merge on the same key at
  upsert time (`MERGE` on the event key, `ON CREATE` stamps `created_at`).

## 5. Pipeline

```
Ingest:    extract_kg (+ event schema, M2) → event nodes + argument edges
           → merge_and_resolve (entity + EVENT dedup, §4)
           → build_property_graph: upsert with ON CREATE created_at/first_seen   ← migration
Detection: M1 first_seen scan (graph-derived) + M2 in-window event nodes
           M3 burst detector over the mention series (offline)
Surface:   event-family analytical primitives (§6)            ← lives in analytical layer
Continuous: Arc 2 — on each ingest, flag new first_seen events + alert
            (Temporal Schedule + dirty-marking)               ← lives in analytical layer
Provenance: every event → source chunks (first_doc_id/first_chunk_id + source_chunks)
```

## 6. Surface — event-family analytical primitives

These are added to the analytical-layer catalog (a fifth family); they *read*
what this spec records:

- `new_events(window, type?)` — `first_seen` events (entities/edges/event
  nodes created in-window), ranked by recency. (M1 + M2)
- `event_dossier(name)` — one event's actors/time/place/polarity + source
  chunks.
- `event_timeline(entity, window?)` — events an entity participated in over
  time (by `event_ts`).
- `trending_events(window)` — burst onsets (M3).
- `entity_new_connections(name, window)` — new edges on a known entity
  (graph-derived, M1) — the "what's new about X" view.

## 7. Continuous detection & alerts (Arc 2 tie-in)

A `detect_new_events` step runs after each ingest (or on a Temporal
Schedule): runs M1/M2 over the just-finished window, optionally M3, and emits
new events. Watchlist/alerting (e.g. "a new edge appeared on a tracked
entity", "a new `Sanction` event involving X") reuses the dirty-marking +
scheduled-sweep pattern already used by the wiki editor. Surfacing/notifying
is the analytical layer's Arc 2.

## 8. Migration & integration points

- **Migration:** add `created_at`/`first_doc_id`/`first_chunk_id` to entity
  nodes, relationships, and `EventOrAction` nodes; backfill existing elements
  with a sentinel so they're not mis-flagged. (Index `created_at` for the M1
  scan.)
- **`extract_kg`** (`src/graph/lightrag_extract.py` / `lightrag_prompts.py`):
  add the event schema to the per-chunk extraction (M2).
- **`merge.py` / `entity_resolution.py`:** add the event match key (§4).
- **`build_property_graph`** (`src/graph/index.py`): `ON CREATE SET
  created_at/first_*` on entities, relations, and event nodes.
- **Config:** `EVENTS_*` namespace — `EVENTS_EXTRACTION_ENABLED` (M2 toggle,
  default off for cost), `EVENTS_TAXONOMY` (typed core list),
  `EVENTS_BURST_ENABLED` + `EVENTS_BURST_Z` (M3), `EVENTS_NEW_WINDOW_DAYS`.
- **Surface/alerts:** event-family primitives + Arc 2 — in the analytical
  layer.

## 9. Phasing (by cost)

- **E1 — `first_seen` + graph-derived events** (cheapest, high leverage):
  the migration + `ON CREATE` stamping + the M1 scan + `new_events` /
  `entity_new_connections` primitives. Immediately answers "what newly
  appeared in the graph". No LLM change.
- **E2 — structured LLM events:** the `extract_kg` event schema (M2) + event
  resolution/dedup (§4) + `event_dossier` / `event_timeline`.
- **E3 — burst / trending:** the M3 detector + `trending_events`.
- **(Arc 2)** continuous alerts on top of E1+.

## 10. Testing

Mirror the project conventions (pytest, `asyncio_mode=auto`,
`_FakeStore.structured_query`):
- **`first_seen` stamping:** assert `ON CREATE` sets `created_at` once and
  re-ingest does **not** overwrite it (re-reports keep their old stamp).
- **M1 scan:** fake store rows → assert window filtering + shape.
- **M2 extraction:** stub the LLM → assert event schema → nodes/edges.
- **Event dedup (§4):** the same event from two docs → one node, `created_at`
  unchanged, **not** re-flagged as new (the core anti-re-report test).
- **M3 burst:** deterministic mention series → known onset.
- **Migration backfill:** existing elements get the sentinel, are not flagged.

## 11. Risks & open questions

- **Event resolution is the hard part (§4).** Weak event dedup → re-reports
  look "new" → alert noise. Needs an event-specific match key (type +
  participants + time proximity); budget design/iteration here.
- **`created_at` = ingest time, not event time.** A backfill of old documents
  later would flag long-past events as "new" (they're new *to the graph*).
  This matches the chosen definition (`first_seen`), but operators must know
  it: a bulk re-ingest = a burst of "new" events. Surface `event_ts` next to
  `created_at` so consumers can tell "happened long ago, learned now".
- **Migration correctness:** the sentinel backfill must run before the first
  stamped ingest, or pre-existing elements pollute the first `new_events`
  window.
- **M2 cost/consistency:** event extraction adds load to the per-chunk LLM
  call; keep it behind `EVENTS_EXTRACTION_ENABLED` and a typed taxonomy to
  bound variance. E1 delivers value with **no** LLM change.
- **Burst on a thin corpus:** z-score/Kleinberg need enough history; M3 is
  deferred (E3) and gated.
- **Label choice** (`:__Entity__:EventOrAction` vs dedicated `:Event`) —
  starting with the existing type; confirm it doesn't overload entity
  semantics as event volume grows.

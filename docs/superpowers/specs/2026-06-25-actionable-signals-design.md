# Actionable signals — from graph metrics to decisions

Date: 2026-06-25
Status: **design (draft), pending review**
Branch: `feat/actionable-signals` (proposed — not yet created)
Companions:
[`2026-06-24-analytical-layer-design.md`](2026-06-24-analytical-layer-design.md),
[`2026-06-25-event-detection-design.md`](2026-06-25-event-detection-design.md)

## Goal

Turn **technical graph analysis** (centrality, components, paths,
distributions — the analytical-layer outputs) into **practical,
decision-ready data**: composite scored signals, thresholds, ranked queues,
quality flags — each with provenance. This is the layer *above* raw metrics:
it answers **"what to do / what to look at"**, not "what the graph looks
like".

Boundary across the three specs:
- **Analytical layer** = the *compute primitives* (the inputs: counts,
  centrality, paths, co-occurrence, temporal slices).
- **Event detection** = *what is new* (`first_seen`) as the graph forms.
- **This spec** = the *composite, actionable outputs* built on those — scored
  signals, queues, leads, knowledge-quality flags. It adds a **new primitive
  family ("quality & action")**, a few **materialized composite scores**, and
  **action surfaces** (review queue, leads).

## The principle (metric → practical signal)

A raw metric becomes practical data when it is folded into a **named,
thresholded, ranked, provenance-backed verdict**:

```
metric(s)  →  composite + normalize + threshold + rank + provenance + human verdict  =  practical signal
```

Example: `betweenness high` + `shares a phone with 3 firms` + `burst of new
links this month` → **`risk_score = high`, added to the review queue, here are
the 5 source chunks**.

Technical analysis produces the *inputs*; this layer produces the
*decisions-ready outputs*.

## 1. Catalog of practical data — what is already covered vs added here

| Practical data | Source signal | Status |
|---|---|---|
| Affiliation via shared identifiers | `shared_identifier_entities` | ✓ analytical |
| Indirect connection / common contacts / association | `connection_path` / `common_connections` / `cooccurrence` | ✓ analytical |
| Likely undisclosed links | `link_prediction` | ✓ analytical (mat) |
| Key players / brokers / seed-relative importance | `top_central_entities` / betweenness / `personalized_pagerank` | ✓ analytical (mat) |
| Thematic clusters + entity dossier | `community_overview` / `entity_dossier` | ✓ analytical |
| What is new / changed / trending | `new_events` / `whats_changed` / `trending_events` | ✓ event + analytical |
| Corpus composition (counts/distributions) | `distribution_*` | ✓ analytical |
| **Composite risk score & red flags** | composite (below) | **＋ this spec** |
| **Knowledge-quality: contradictions, gaps, merge candidates** | quality scans (below) | **＋ this spec** |
| **Domain rollups: Issue/Resolution, numeric, comms** | aggregations (below) | **＋ this spec** |
| **Action queues & leads (investigate-next, review queue)** | assembled/ranked (below) | **＋ this spec** |
| **External cross-reference (internal vs registry/anchor)** | Wikibase/WDQS | **＋ this spec (Arc 6)** |

The net-new families (＋) are detailed in §2–§6.

## 2. Risk & red-flag signals (composite)

**`risk_score(entity)`** — a transparent weighted composite of normalized
components, **materialized offline** (mirror the centrality materialization in
the analytical spec §5) as `e.risk_score`, read cheaply online. Components
(each 0..1, configurable weights):

- **affiliation** — shares an identifier with ≥N other entities
  (`shared_identifier_entities`);
- **brokerage** — high betweenness (bridges otherwise-separate clusters);
- **controversy** — high share of `negated`/`uncertain` facts;
- **volatility** — burst of new links (`first_seen` edges in window);
- **opacity** — connected only via identifiers / thin descriptive profile
  (shell signal).

`risk_score` is a **heuristic, not ground truth** — its provenance lists
*which components fired and with what value*, so a human sees the "why", not a
black-box number.

Discrete red flags (boolean, each with provenance):
- **Circular ownership:**
  ```cypher
  MATCH p=(a:__Entity__)-[:OWNS*2..6]->(a) RETURN [n IN nodes(p)|n.name] AS cycle LIMIT $top_n
  ```
- **Nominee/bridge:** high betweenness + low degree-to-content + identifier
  links only.
- **Shell signal:** an `Organization` whose only edges are to identifier
  nodes.

## 3. Knowledge-quality signals (gaps & contradictions) — net-new, high value

Data-quality treated as *insight* (what to review/trust), not just hygiene.

- **Contradictions** — the same fact asserted and denied:
  ```cypher
  MATCH (a:__Entity__)-[r1]->(b:__Entity__), (a)-[r2]->(b)
  WHERE type(r1)=type(r2) AND r1.polarity='affirmed' AND r2.polarity='negated' AND id(r1)<id(r2)
  RETURN a.name, type(r1) AS rel, b.name, r1.source_chunks, r2.source_chunks
  ```
  **Caveat (must handle):** affirmed-then-negated *over time*
  (`valid_from` ordering) is a **change**, not a contradiction — only flag
  overlapping/contemporaneous validity windows. (Reuses `valid_from`/
  `valid_to` + `polarity`.)
- **Incomplete dossiers** — expected attributes missing by type (per-type
  expected set in config; e.g. `Organization` expects INN/OGRN/address/phone):
  ```cypher
  MATCH (e:__Entity__:Organization)
  OPTIONAL MATCH (e)-[]-(id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $EXPECTED)
  WITH e, collect(DISTINCT [l IN labels(id) WHERE l IN $EXPECTED][0]) AS have
  RETURN e.name, [x IN $EXPECTED WHERE NOT x IN have] AS missing ORDER BY size(missing) DESC
  ```
  → a `completeness_score = filled/expected` per entity.
- **Orphans / under-connected** — degree below a floor (noise or
  under-documented).
- **Duplicate-name candidates → recommended merges** — reuse the
  `graph_stats` dup detection + `entity_resolution` similarity to produce a
  *ranked merge queue* (actionable, not just a count).
- **Coverage gaps** — topics/entities with thin support (few chunks/edges) →
  "we know little here".

## 4. Domain rollups / mini-BI — net-new

Practical aggregates tuned to the corpus (reports / emails / transcripts):

- **Issue/Resolution analytics** (transcript domain) — open vs resolved,
  resolution rate, recurring issues, by type/product:
  ```cypher
  MATCH (i:__Entity__:Issue)
  OPTIONAL MATCH (i)-[:RESOLVED_BY]-(r:__Entity__:Resolution)
  WITH i, count(r) AS res
  RETURN count(i) AS total, sum(CASE WHEN res=0 THEN 1 ELSE 0 END) AS unresolved
  ```
  (Confirm `RESOLVED_BY` direction in `schema.py`.)
- **Numeric rollups over `Amount`/`Metric`** — sum/distribution of contract
  amounts per counterparty (analytical Arc 1).
- **Communication-network stats** (email domain) — who-talks-to-whom
  intensity, response patterns (over `CONTACT`/`RESPONDED_TO`), if present.
- **Sentiment/polarity per entity/topic** over time (`polarity_evolution`).

## 5. Action queues & leads — net-new, the most "practical"

These are **assembled, ranked outputs**, not single queries — the operational
payoff:

- **Investigate-next** — `risk_score` high × `completeness_score` low,
  ranked. "Who deserves attention and is under-documented."
- **Review queue** — contradictions + structural anomalies + merge candidates,
  prioritized into one actionable list.
- **Recommended merges** — the dup-candidate queue from §3.
- **Watchlist hits** — new connections to flagged entities (ties to event
  spec `first_seen` + analytical Arc 2 alerts).

## 6. External cross-reference (verification) — net-new (Arc 6)

Internal graph vs the **Wikibase anchor / external registry** (`INN →
official company data` over WDQS/SPARQL): emit **mismatch** flags (internal
fact contradicts registry) and **confirmation** flags (corroborated). Depends
on the Wikibase populator being on.

## 7. How it computes / where it lives

- **Composite scores** (`risk_score`, `completeness_score`) — materialized
  **offline** (reuse `AnalyticsMaterializeWorkflow`, analytical §5) → read
  online as node properties.
- **Quality flags + rollups** — analytical primitives in a new **"quality &
  action"** family (online read-only Cypher).
- **Queues/leads** — assembly+ranking on top of the above (a thin use case;
  may be a primitive that composes others).
- **Surfaces** — the `analyze` endpoint primitives + a dedicated **review
  queue / leads** view + Arc 2 alerts. All carry **provenance**: every signal
  lists its component evidence + source chunks.

## 8. Composite signal definitions (concrete)

- `risk_score = Σ wᵢ·normalize(componentᵢ)`, banded (low/medium/high) by
  configurable thresholds; provenance = the per-component values that fired.
  Weights/thresholds in config (`SIGNALS_RISK_WEIGHTS`,
  `SIGNALS_RISK_BANDS`).
- `completeness_score = |filled ∩ expected| / |expected|` per type;
  `SIGNALS_EXPECTED_ATTRS` maps type → expected attribute set.

## 9. Phasing

- **P1 — knowledge-quality flags** (contradictions, completeness, orphans,
  merge candidates): cheap, online, high value, **no LLM, no materialization**.
- **P2 — composite `risk_score` + leads** (investigate-next, review queue):
  needs offline materialization.
- **P3 — domain rollups** (Issue/Resolution, numeric, comms, sentiment).
- **P4 — external cross-reference** (needs Wikibase populated).

## 10. Integration points

- New **"quality & action"** primitive family in the analytical catalog
  (`contradictions`, `incomplete_entities`, `merge_candidates`, `orphans`,
  `risk_score` read, `issue_resolution_stats`, `investigate_next`,
  `review_queue`).
- Offline scoring in `AnalyticsMaterializeWorkflow` (risk/completeness).
- Config `SIGNALS_*` (weights, bands, expected-attrs-by-type, thresholds).
- Provenance chain (analytical §7) extended with "component evidence".

## 11. Testing

- **Composite scoring** = pure functions: deterministic given component
  inputs; assert bands/weights, provenance lists firing components.
- **Quality scans** with `_FakeStore`: contradictions (incl. the temporal
  caveat — a change must NOT be flagged), completeness, orphans.
- **Queue assembly/ranking** tests (ordering, caps).
- Fail-soft throughout (store/error → empty), per `analysis.py` style.

## 12. Risks & open questions

- **`risk_score` is judgment, not truth** — keep it transparent and
  configurable (weights/bands in config, provenance per component); never
  present as a verdict. It's a triage heuristic.
- **Contradiction false positives** — negation scope + temporal change must be
  handled (only contemporaneous overlapping windows count); otherwise normal
  fact-updates flood the queue.
- **Per-type expected attributes** need a curated schema (config) — wrong
  expectations → noisy completeness.
- **Domain rollups depend on relations actually present** (Issue/Resolution,
  CONTACT/RESPONDED_TO) and their direction — verify against `schema.py`.
- **External cross-reference** depends on the Wikibase anchor being populated
  and on a reliable `INN → registry` mapping.
- **Action queues can become noise** without good thresholds/dedup — tie
  ranking to provenance strength and suppress already-reviewed items.

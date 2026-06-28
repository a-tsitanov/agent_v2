# Analytical layer — design

Date: 2026-06-24
Status: **Part 1 (core) approved** · Part 2 (runtime/provenance/surfaces/testing/
phasing) proposed, pending review
Branch: `feat/analytical-layer` (proposed — not yet created)

## Goal

Add a **second query mode alongside retrieval search**: a natural-language
**analytical Q&A** layer that turns an analytical question into actual
**computation** over the knowledge graph (aggregations, graph algorithms,
temporal slices, paths) and returns a synthesized answer **plus a
deterministic provenance chain**.

The retrieval layer (`/search/{local,global,drift,auto}`) answers *"what do
the documents say about X"* — it finds and paraphrases chunk text. The
analytical layer answers *"what is true about the whole corpus if you
compute it"* — counts, rankings, centrality, connection paths, time series —
**answers that exist in no single document**.

This is **additive**: a new workflow + a new `src/analytics/` package + new
surfaces. It reuses existing infrastructure (Temporal, GDS projection
helpers, community materialization, the LLM planner pattern, the date-filter
plumbing) and introduces **no schema migration** for the online tier.

## Locked decisions (from brainstorming)

1. **Capability** = NL analytical Q&A (a "computing/reasoning" mode), not BI
   dashboards and not just more raw graph endpoints.
2. **Execution** = **hybrid**: an LLM planner maps the question to a
   **catalog of safe parameterized primitives** first; a **guarded,
   read-only text-to-Cypher fallback** handles the long tail. Catalog-first
   keeps the deterministic core dominant (consistent with the project's
   deliberate "deterministic retrieval, not a ReAct loop" stance).
3. **Runtime** = **two-tier**: light analytics run **online** (seconds);
   heavy GDS analytics (full-graph centrality, link prediction) are computed
   **offline and materialized** into Neo4j, then read cheaply online.
4. **Surfaces** = HTTP endpoint **+** MCP tool **+** CLI (all three).
5. **Families** (v1 target) = aggregations/rankings · connections &
   co-occurrence (incl. identifier-risk) · centrality/clusters · temporal
   dynamics — all four.
6. **Provenance** = **full chain**: answer + which primitive/Cypher ran +
   raw rows + source entities/chunks. For a "computing" layer, correctness
   and auditability outrank fluency.

---

## 1. Search vs Analytics — the boundary (contract)

**One line:** search *retrieves and quotes* text that already contains the
answer; analytics *computes* an answer that exists in no single document.

| Axis | Retrieval search (`local/global/drift`) | Analytical layer |
|---|---|---|
| Operation | lookup — find & paraphrase | compute — count/rank/walk/diff |
| Coverage | **sample**: top-k relevant chunks | **population**: whole graph / deterministic slice |
| Unit of truth | chunk text (prose) | graph structure + aggregates (edges, metrics, series) |
| Where the number comes from | LLM *read it* from prose (hallucination risk) | deterministic Cypher/GDS; LLM only **verbalizes** |
| Question type | qualitative / existential ("what is known") | quantitative / structural / temporal ("how many / who is most / how connected / how changed") |
| Reproducibility | retrieval+LLM can drift | same question → same numbers; auditable |
| Role of the graph | a retrieval aid (expand, seed walks) | **the object of measurement** |

**Discriminator (use this to route, document, and test):**
> If the answer could be copy-pasted from some chunk → **search**.
> If it requires join/count/ranking/path/diff across many entities and lives
> in no single passage → **analytics**.

Contrasting pairs (same topic):

| Search (in the text) | Analytics (not in any document) |
|---|---|
| "What is written about Romashka?" | "Who is Romashka connected to, and which of them is most central?" |
| "Were there supply problems?" | "How many `Issue`s of type supply, and what share are unresolved?" |
| "What is known about this phone?" | "Which *other* companies sit on this *same* phone?" |
| "Who is Ivanov?" | "Through whom is Ivanov connected to 'X Ltd' (the chain)?" |

**Honest overlap (do not over-claim novelty):**
- `global` search already does graph-ish map-reduce, **but** it summarizes
  *community-report text*, it does not compute exact quantities or run
  algorithms.
- `/admin/graph/*` (`pagerank`/`components`/`shortest_path` in
  [analysis.py](../../../src/graph/analysis.py)) **is already analytics** —
  but it is not NL-driven, not synthesized, admin-only, per-call, no planner.
  The analytical layer turns that point capability into an NL, planned,
  synthesized, provenance-backed product surface, and widens the catalog.
- The layers **compose**: "who is most central in Romashka *and* what is
  written about them" = analytics (centrality) → search (evidence). This is
  the argument for keeping them as **distinct primitives**, not one merged
  mode (see Roadmap, Arc 3).

---

## 2. Architecture

A new durable **`AnalyticalQueryWorkflow`** parallel to the search workflows
— the mirror of `SearchOrchestratorWorkflow`, but **plan → execute
(primitives | cypher) → synthesize + provenance** instead of
retrieve→synthesize.

New package **`src/analytics/`** (framework-light, in the style of
[analysis.py](../../../src/graph/analysis.py) + `src/retrieval/`):

```
src/analytics/
├── catalog.py          # primitive registry: name → fn + param-schema + LLM description
│                       #   (single source of truth, like retrieval/atomic_tools.TOOL_DESCRIPTIONS)
├── primitives/         # the primitive functions (Cypher builders + fail-soft dispatchers,
│                       #   exact style of graph/analysis.py)
├── planner.py          # NL → AnalysisPlan (small-tier LLM; mirrors retrieval/query_planner.py)
├── cypher_guard.py     # guarded read-only text-to-Cypher (validate → read-tx → timeout → LIMIT)
└── provenance.py       # deterministic evidence assembly (numbers come from the executor, NOT the LLM)
```

**Data flow:**

```
POST /api/v1/analyze ─→ AnalyticalQueryWorkflow
  1. analytical_plan        (kb-search-small, small-LLM)  NL → AnalysisPlan
                            plan = [primitive(name,params)…]  OR  route="cypher"
  2. execute_step ×N        (kb-search-small)             catalog primitives (read-only Cypher)
     ├ light  → direct Neo4j query over indexes
     └ heavy  → READ materialized properties (e.pagerank, :Community.report)
     ── or ──
     execute_cypher         (kb-search-small)            guarded read-only text-to-Cypher
  3. synthesize_analytical  (kb-search-large, large-LLM)  raw results → NL answer
  4. provenance             (deterministic, no LLM)       primitive/Cypher + raw rows + entities/chunks
```

Heavy GDS computation (centrality, link prediction) does **not** live in the
online workflow. A separate offline **`AnalyticsMaterializeWorkflow`** on the
`kb-graph-build` queue (mirror of `CommunityBuildWorkflow`) computes metrics
and writes them back to Neo4j as node properties / relationships. This is the
"two-tier" split.

**Queues** (reuse existing): `analytical_plan` / `execute_step` /
`execute_cypher` on `kb-search-small`; `synthesize_analytical` on
`kb-search-large`; `AnalyticsMaterializeWorkflow` on `kb-graph-build`.

---

## 3. Execution model — planner + hybrid catalog/Cypher

- **Planner** (small-tier LLM, function-calling in the spirit of
  `query_planner.decompose`): input = question + the catalog (descriptions +
  param schemas); output = a validated `AnalysisPlan` — 1–3 primitive calls
  with params, **or** `route="cypher"`.
- **Determinism guardrail:** the planner output is **strictly validated**
  against the catalog (pydantic). Unknown primitive / bad params → do not
  execute; fall back to Cypher or an honest "cannot compute this". (Same
  strict-resolution discipline as the forecast registry in the sibling
  project.)
- **Cypher fallback is reached only when the catalog does not fit** — the
  deterministic core stays dominant. Guardrails:
  - **read-only:** execute in a read transaction (`execute_read` / access
    mode READ) **and** a write-keyword denylist
    (`CREATE/MERGE/SET/DELETE/REMOVE/CALL …{ …write }`, `apoc.*` writers)
    checked before running;
  - **schema card** (exact labels/rels/props/indexes) injected into the
    prompt — **generated from `src/graph/schema.py` at build time**, not
    hard-coded (see §6);
  - **timeout** (tx config) + forced `LIMIT` + row cap;
  - the generated Cypher is returned in provenance (auditable).
  - **kill switch:** `ANALYTICS_CYPHER_FALLBACK_ENABLED` (default decided at
    review — recommend **off** until hardened).

---

## 4. Primitive catalog

### Cross-cutting conventions (apply to every primitive)

- **Polarity:** default `WHERE r.polarity <> 'negated'`; flag
  `include_negated=false`.
- **Time (as-of / window):** where relevant,
  `(r.valid_from IS NULL OR r.valid_from <= $as_of)` and
  `(r.valid_to IS NULL OR r.valid_to >= $as_of)`; date windows reuse the
  existing date-filter plumbing (`main@90300ab`/`6d5c472`).
- **Identifiers:** constant `$ID_TYPES = [Email, PhoneNumber, PostalAddress,
  DocumentDate, Amount, ContractNumber, OrderNumber, InvoiceNumber, INN,
  OGRN, BIC, BankAccount]`. Many aggregates default `exclude_identifiers=true`.
- **Name resolution:** lookup via `entity_name_fulltext` → exact match; on
  duplicate names (counted by `graph_stats`) return candidates rather than
  silently picking the first.
- **Caps + fail-soft:** every primitive has `top_n`/`LIMIT` with a clamp;
  `store=None` / any error → empty result (style of `analysis.py`).
- **Tiers:** 🟢 online (light read-only Cypher over indexes) · 🟠 offline-mat
  (reads materialized properties; computed by the offline workflow).

> All Cypher below is **design-level** and matches the schema as mapped in
> §6. The exact relationship-type set, the `mention_count` property name, and
> the `IN_COMMUNITY` edge must be confirmed against `src/graph/schema.py` and
> `src/graph/communities.py` during implementation. GDS calls follow GDS 2.x
> and are UNVERIFIED against a live install (same caveat `analysis.py` already
> carries).

### Family 1 — Aggregations & rankings 🟢

**`count_entities(type?, tag?, polarity?, exclude_identifiers=true)`** — "how
many entities / organizations / by tag".
```cypher
MATCH (e:__Entity__)
WHERE ($type IS NULL OR $type IN labels(e))
  AND ($exclude_ids = false OR NONE(l IN labels(e) WHERE l IN $ID_TYPES))
RETURN count(e) AS n
```

**`count_relationships(rel_type?, polarity?)`** — "how many relations of type
X / how many negated claims".
```cypher
MATCH (:__Entity__)-[r]->(:__Entity__)
WHERE ($rel_type IS NULL OR type(r) = $rel_type)
  AND ($polarity IS NULL OR r.polarity = $polarity)
RETURN count(r) AS n
```

**`distribution_by_type(exclude_identifiers=false)`** — entity histogram by
type.
```cypher
MATCH (e:__Entity__)
WITH [l IN labels(e) WHERE l <> '__Entity__'][0] AS type
RETURN type, count(*) AS n ORDER BY n DESC
```

**`distribution_by_relation_type()`** — which relation types dominate.
`MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC`

**`distribution_by_polarity(rel_type?)`** — share of affirmed/negated/
uncertain (a "contentiousness" indicator).
```cypher
MATCH ()-[r]->() WHERE ($rel_type IS NULL OR type(r)=$rel_type)
RETURN r.polarity AS polarity, count(*) AS n ORDER BY n DESC
```

**`top_entities_by_mentions(type?, top_n, exclude_identifiers=true)`** —
frequency importance (reads `entity_mention_count`).
```cypher
MATCH (e:__Entity__)
WHERE ($type IS NULL OR $type IN labels(e))
  AND NONE(l IN labels(e) WHERE l IN $ID_TYPES)
RETURN e.name AS name, e.mention_count AS mentions
ORDER BY e.mention_count DESC LIMIT $top_n
```
Note: "mention frequency" ≠ "centrality" (Family 3) — different answers to
"who matters".

**`top_entities_by_degree(type?, top_n, polarity?)`** — connectivity.
```cypher
MATCH (e:__Entity__) WHERE ($type IS NULL OR $type IN labels(e))
OPTIONAL MATCH (e)-[r]-(:__Entity__) WHERE r.polarity <> 'negated'
WITH e, count(r) AS degree
RETURN e.name AS name, degree ORDER BY degree DESC LIMIT $top_n
```

### Family 2 — Connections & co-occurrence 🟢

**`entity_dossier(name, hops=1, top_n=25)`** ⭐ flagship — one portrait of an
entity and its surroundings. Composition of sub-queries in one activity:
```cypher
-- 1. core
MATCH (e:__Entity__ {name:$name}) RETURN e.name, e.description, labels(e), e.mention_count
-- 2. neighbors by relation type (top by weight, valid, not negated)
MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__)
WHERE r.polarity<>'negated' AND (r.valid_to IS NULL OR r.valid_to>=$now)
RETURN type(r) AS rel, n.name, [l IN labels(n) WHERE l<>'__Entity__'][0] AS ntype,
       r.weight AS w, r.valid_from, r.valid_to
ORDER BY w DESC LIMIT $top_n
-- 3. attached identifiers (phone/INN/email/…)
MATCH (e:__Entity__ {name:$name})-[r]-(id:__Entity__)
WHERE any(l IN labels(id) WHERE l IN $ID_TYPES)
RETURN [l IN labels(id) WHERE l IN $ID_TYPES][0] AS id_type, id.name AS value
-- 4. communities
MATCH (e:__Entity__ {name:$name})-[:IN_COMMUNITY]->(c:Community)
RETURN c.level, c.title
```
Returns `{core, connections[], identifiers[], communities[]}`. Separates
identifiers from semantic neighbors — the "dossier" for the identifier-heavy
domain.

**`neighbors_by_relation(name, rel_type, polarity?, top_n)`** — "who is X
linked to specifically by relation Y" (e.g. where Ivanov worked).
```cypher
MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__)
WHERE type(r)=$rel_type AND ($polarity IS NULL OR r.polarity=$polarity)
RETURN n.name, r.weight, r.valid_from, r.valid_to ORDER BY r.weight DESC LIMIT $top_n
```

**`cooccurrence(name, top_n)`** — who is most often mentioned together with X
(even without a direct edge — via shared chunks).
```cypher
MATCH (e:__Entity__ {name:$name})<-[:MENTIONS]-(c:Chunk)-[:MENTIONS]->(other:__Entity__)
WHERE other <> e
RETURN other.name, count(DISTINCT c) AS shared ORDER BY shared DESC LIMIT $top_n
```
Catches associations that have no explicit edge in the graph.

**`common_connections(a, b, top_n)`** — what/who two entities share.
```cypher
MATCH (x:__Entity__ {name:$a})-[r1]-(m:__Entity__)-[r2]-(y:__Entity__ {name:$b})
WHERE r1.polarity<>'negated' AND r2.polarity<>'negated'
RETURN m.name, [l IN labels(m) WHERE l<>'__Entity__'][0] AS type,
       collect(DISTINCT type(r1))+collect(DISTINCT type(r2)) AS via LIMIT $top_n
```

**`connection_path(source, target, max_hops=6)`** — how A and B are linked
(reuses existing `shortest_path`; hops clamped inline).
```cypher
MATCH (a:__Entity__ {name:$source}),(b:__Entity__ {name:$target})
MATCH p = shortestPath((a)-[*..6]-(b))
RETURN [n IN nodes(p)|n.name] AS path, [r IN relationships(p)|type(r)] AS rels, length(p) AS hops
```

**`shared_identifier_entities(id_type?, min_owners=2, top_n)`** ⭐ risk/dedup
— which distinct entities share one phone / INN / address / account (an
affiliation, duplicate, or fraud signal).
```cypher
MATCH (id:__Entity__) WHERE any(l IN labels(id) WHERE l IN $ID_TYPES)
  AND ($id_type IS NULL OR $id_type IN labels(id))
MATCH (id)-[]-(owner:__Entity__)
WHERE NONE(l IN labels(owner) WHERE l IN $ID_TYPES)
WITH id, [l IN labels(id) WHERE l IN $ID_TYPES][0] AS id_type, collect(DISTINCT owner.name) AS owners
WHERE size(owners) >= $min_owners
RETURN id.name AS value, id_type, owners ORDER BY size(owners) DESC LIMIT $top_n
```
Plays directly to the identifier domain; valuable for due-diligence.

**`identifier_lookup(value)`** — "whose phone is this / who owns INN
7701234567".
`MATCH (id:__Entity__ {name:$value})-[]-(e:__Entity__) WHERE NONE(... ID_TYPES ...) RETURN e.name, labels(e), type(r)`

### Family 3 — Centrality & clusters

**`top_central_entities(metric=pagerank|betweenness|eigenvector, type?, top_n)`** 🟠
— structural importance. Online read (property written by the offline
workflow):
```cypher
MATCH (e:__Entity__) WHERE e[$metric] IS NOT NULL AND ($type IS NULL OR $type IN labels(e))
RETURN e.name AS name, e[$metric] AS score ORDER BY score DESC LIMIT $top_n
```
Offline computation (in `AnalyticsMaterializeWorkflow`, mirrors communities):
```cypher
CALL gds.pageRank.stream($g,{relationshipWeightProperty:'weight'})
YIELD nodeId,score
WITH gds.util.asNode(nodeId) AS e, score SET e.pagerank = score
```
(`betweenness` → `gds.betweenness.stream`, `eigenvector` →
`gds.eigenvector.stream`.) Different `metric` = different "who matters":
pagerank=authority, betweenness=brokerage/bridges, eigenvector=ties to the
influential.

**`personalized_pagerank(seeds[], top_n)`** 🟢 — who is most connected
*relative to* these seeds. Already implemented in `analysis.py`; wrapped into
the catalog as-is (seed-biased, cheaper than a full run).

**`entity_communities(name)`** 🟠 — which thematic clusters an entity belongs
to. `MATCH (e {name:$name})-[:IN_COMMUNITY]->(c:Community) RETURN c.level, c.title, c.summary`.

**`community_overview(level=0, top_n)`** 🟠 — the large thematic clusters
(reads the Leiden hierarchy + ready reports — nearly free, already built by
`CommunityBuildWorkflow`).
`MATCH (c:Community {level:$level}) RETURN c.title, c.summary, c.member_count ORDER BY c.member_count DESC LIMIT $top_n`

**`link_prediction(name, top_n)`** 🟠 — probable but not-yet-recorded links
(a hypothesis, not a fact). Offline: GDS node similarity / Adamic-Adar,
materialized as `(:__Entity__)-[:LIKELY_LINK {score}]->(:__Entity__)`. Online
read: `MATCH (e {name:$name})-[l:LIKELY_LINK]->(m) RETURN m.name, l.score ORDER BY l.score DESC LIMIT $top_n`.
The heaviest, most cautious primitive — provenance must mark it as a
hypothesis.

### Family 4 — Temporal dynamics 🟢

(over edge `valid_from`/`valid_to` and chunk `doc_date` — the recent
plumbing)

**`relationship_timeline(name, rel_type?, granularity=month)`** — how an
entity's relations changed over time.
```cypher
MATCH (e:__Entity__ {name:$name})-[r]-(n:__Entity__)
WHERE r.valid_from IS NOT NULL
RETURN substring(r.valid_from,0,7) AS period, type(r) AS rel, n.name, r.polarity
ORDER BY period
```

**`whats_changed(date_from, date_to, entity?, top_n)`** — what appeared /
ended / flipped polarity in a window.
```cypher
MATCH (e:__Entity__)-[r]-(n:__Entity__)
WHERE ($entity IS NULL OR e.name=$entity)
  AND ((r.valid_from >= $from AND r.valid_from <= $to)
    OR (r.valid_to   >= $from AND r.valid_to   <= $to))
RETURN e.name, type(r) AS rel, n.name, r.polarity, r.valid_from, r.valid_to,
       CASE WHEN r.valid_from>=$from THEN 'appeared' ELSE 'ended' END AS change
ORDER BY coalesce(r.valid_from,r.valid_to) LIMIT $top_n
```

**`topic_trend(topic_or_tag, granularity=month)`** — mention frequency of a
topic over time (via chunk `doc_date`).
```cypher
MATCH (t:__Entity__ {name:$topic})<-[:MENTIONS]-(c:Chunk)
WHERE c.doc_date IS NOT NULL
RETURN substring(c.doc_date,0,7) AS period, count(DISTINCT c) AS mentions ORDER BY period
```

**`polarity_evolution(name|rel_type, granularity=quarter)`** — how
sentiment/certainty of facts shifted over time (group edges by
`substring(valid_from,…)` × `polarity` → count).

**`entity_activity(name, granularity=month)`** — when an entity was
active/discussed (mention bursts); same as `topic_trend` for an arbitrary
entity.

### Tier & dependency summary

| Primitive | Family | Tier | Depends on |
|---|---|---|---|
| count_entities / count_relationships / distribution_* | 1 | 🟢 | base schema |
| top_entities_by_mentions / _by_degree | 1 | 🟢 | `entity_mention_count` index |
| entity_dossier ⭐ / neighbors_by_relation / cooccurrence / common_connections / connection_path | 2 | 🟢 | `:MENTIONS`, edges, `shortest_path` |
| shared_identifier_entities ⭐ / identifier_lookup | 2 | 🟢 | identifier entities |
| top_central_entities / link_prediction | 3 | 🟠 | **offline materialization** (GDS) |
| personalized_pagerank | 3 | 🟢 | exists |
| entity_communities / community_overview | 3 | 🟠 | `CommunityBuildWorkflow` (exists) |
| relationship_timeline / whats_changed / topic_trend / polarity_evolution / entity_activity | 4 | 🟢 | `valid_from`/`valid_to`, `doc_date` |

~20 of ~24 primitives are 🟢 online and need no new infrastructure (only the
new package + workflow). The 🟠 block (centrality/link_prediction) needs the
offline `AnalyticsMaterializeWorkflow`; communities primitives are nearly
free (data already materialized).

Adding a primitive = new function + one line in `catalog.py` (open/closed).

---

## 5. Two-tier runtime & materialization

**Online tier** — `AnalyticalQueryWorkflow`, light primitives run direct
read-only Cypher over indexed properties; 🟠 primitives **read** materialized
values. Off the event loop via `asyncio.to_thread(_run_query, store, …)` (the
Neo4j driver is blocking — same idiom as `analysis.py`/`communities.py`).

**Offline tier** — `AnalyticsMaterializeWorkflow` on `kb-graph-build`
(mirror of `CommunityBuildWorkflow`):
1. project the weighted `__Entity__` graph (reuse `_new_graph_name` /
   `_project_cypher` / `_drop_cypher` from `communities.py`);
2. run the heavy GDS algorithm (pagerank/betweenness/eigenvector/
   node-similarity);
3. **write results back into Neo4j** as node properties (`e.pagerank`, …) /
   `:LIKELY_LINK` edges — idempotent `MERGE`/`SET`, exactly how
   communities write `:Community` reports;
4. drop the projection (fail-soft `finally`, like `_with_projection`).

**Triggering:** admin endpoint (mirror `POST /search/rebuild-communities`) +
a future Temporal **Schedule** (ARCHITECTURE §9 notes schedules are not yet
wired — this is the first natural consumer). Materialization is also a hook
for incremental recompute later (Roadmap, Arc 4).

**Storage choice:** Neo4j node properties / relationships (idiomatic here;
community reports already live on nodes). A Postgres `analytics_*` table is an
alternative only if a metric does not map onto a node (deferred).

---

## 6. Schema card (for text-to-Cypher) — generated, not hard-coded

The Cypher fallback injects a schema card into the prompt. It **must be
generated from `src/graph/schema.py`** at build/startup time (so it never
drifts), not pasted. Draft content to verify against `schema.py`:

- **Nodes:** `:__Entity__:<Type>` with `name`, `description`, `mention_count`;
  `:Chunk` (`doc_id`, `doc_date`, `position`, `text`); `:Community` (`id`,
  `level`, `title`, `summary`, `report`, `report_vec`, `member_count`).
- **Entity types:** Person, Organization, Location, Concept, Topic, Metric,
  Product, Document, Issue, Resolution, EventOrAction + identifier types in
  `$ID_TYPES`. **(Confirm the exact set in `schema.py`.)**
- **Relationship types** (between `:__Entity__`): WORKS_AT, MEMBER_OF, OWNS,
  AUTHORED, CONTACT, MENTIONS, DISCUSSES, PARTICIPATED_IN, RESPONDED_TO,
  PARTY_OF, DATED, AMOUNT_OF, ADDRESS_OF, TAX_ID_OF, REGISTRATION_OF,
  BANK_OF, REPORTED, RESOLVED_BY, AFFECTS, REFERENCES, RELATED_TO.
  **(Confirm the exact set + edge direction in `schema.py`.)**
- **Relationship props:** `weight` (co-occurrence count), `polarity`
  (affirmed/negated/uncertain), `valid_from`/`valid_to`, `tags`,
  `source_chunks`, `mention_count`.
- **Special edge:** `(:Chunk)-[:MENTIONS]->(:__Entity__)`.
- **Indexes:** fulltext `entity_name_fulltext` on `__Entity__.name`; range
  `entity_name`, `entity_mention_count`, `chunk_doc_id`; vector on
  `:Community.report_vec`.
- **Safety rules** (embedded in the prompt): always parameterize user input;
  clamp variable-length bounds before inlining; always add `LIMIT`; filter
  `polarity <> 'negated'` and expired edges unless asked otherwise.

---

## 7. Provenance & output contract

Frozen pydantic, in the style of `OrchestratorParams`/`SearchOutcome`
([contracts.py](../../../src/workflow/contracts.py)). The **invariant**: the
LLM writes `answer`; the executor produces `steps[].rows`/`cypher`. Numbers in
the answer are always checkable against `rows`.

```python
class AnalyzeRequest(BaseModel):
    query: str
    history: list[ChatTurn] = []
    date_from: str | None = None        # reuses the date-filter plumbing
    date_to:   str | None = None
    top_n: int = 20

class PrimitiveCall(BaseModel):
    primitive: str                       # name from the catalog
    params: dict[str, Any]               # validated by the primitive's param schema

class AnalysisPlan(BaseModel):
    route: Literal["catalog", "cypher"]
    steps: list[PrimitiveCall] = []      # 1–3 steps when route=catalog
    reason: str

class StepResult(BaseModel):
    primitive: str                       # or "cypher_fallback"
    params: dict[str, Any]
    cypher: str                          # the query actually executed (audit)
    rows: list[dict]
    row_count: int
    source_chunks: list[str] = []
    truncated: bool = False

class Provenance(BaseModel):
    route: Literal["catalog", "cypher"]
    plan_reason: str
    steps: list[StepResult]
    elapsed_ms: int

class AnalyzeResponse(BaseModel):
    query: str
    answer: str
    provenance: Provenance
    latency_ms: int
```

---

## 8. Surfaces

- **HTTP:** `POST /api/v1/analyze` (mirror `search_v2.py`; `X-API-Key`;
  starts `AnalyticalQueryWorkflow`, returns `AnalyzeResponse`). Admin:
  `POST /admin/graph/materialize` to trigger the offline workflow.
- **MCP:** a high-level workflow-backed tool `kb_analyze` (MCP-1 style), and
  the individual primitives also exposed as MCP-2 atomic tools (the existing
  `graph_pagerank` etc. join this set). Same registration pattern as
  `atomic_tools.py` + the MCP-2 server.
- **CLI:** an `analyze` subcommand alongside `check_ingestion.py`/`diag_kg.py`
  (manual runs/debug/admin), plus a `materialize` command.

---

## 9. Integration & conventions

- **Config:** new `AnalyticsSettings` namespace (`ANALYTICS_*`) nested into
  root `Settings` via `@cached_property` (the existing pattern in
  `config.py`). Keys: `ANALYTICS_CYPHER_FALLBACK_ENABLED`,
  `ANALYTICS_DEFAULT_TOP_N`, `ANALYTICS_MAX_STEPS`,
  `ANALYTICS_CYPHER_TIMEOUT_S`, `ANALYTICS_CYPHER_ROW_CAP`,
  `ANALYTICS_MATERIALIZE_CRON` (when the Schedule lands).
- **DI:** primitives/planner obtain the Neo4j store via
  `build_neo4j_graph_store()` (process-cached singleton; call off the event
  loop). Activities use module-level singletons (Temporal activities can't
  use request-scoped DI) — `get_llm_pool().get("plan")` /
  `get("synthesis")`, exactly as the search activities do.
- **Worker:** register `AnalyticalQueryWorkflow` +
  `AnalyticsMaterializeWorkflow` and their activities in
  `worker.py::_build_worker` (extend the `search` and `graph-build` groups /
  activity lists).
- **LLM:** planner = small tier (`"plan"` role), synthesis = large tier
  (`"synthesis"` role) via `BoundedLLM`/`LLMPool` (concurrency arbitration is
  automatic).

---

## 10. Testing strategy

Mirror the existing conventions (pytest, `asyncio_mode=auto`):
- **Pure helpers** (Cypher builders, plan validation, write-keyword denylist,
  provenance assembly): direct call, assert output. The denylist + read-only
  guard get adversarial tests (write attempts must be rejected).
- **Primitives:** `_FakeStore.structured_query` captures Cypher + params and
  returns canned rows — assert the generated Cypher and the fail-soft
  behavior (store=None / error → empty).
- **Planner:** stub the LLM, assert NL → validated `AnalysisPlan`; bad LLM
  output → rejected → fallback/honest-fail (no crash).
- **Workflow:** Temporal test env with stub activities (skip if Temporal not
  up), assert plan → execute → synthesize ordering and provenance assembly.
- **Numeric faithfulness eval** (extends `tests/eval/`): golden analytical
  Q&A; check (a) computed numbers are correct, (b) the synthesis does **not**
  introduce numbers absent from `rows` (anti-hallucination of quantities).
- **Quality gates:** `uv run ruff check src/` + `ruff format src/` +
  `pytest` (project's standard).

---

## 11. Phasing

- **v1a (online core, no new infra):** `src/analytics/` package, planner,
  catalog Families 1/2/4 + `personalized_pagerank`/communities reads,
  `AnalyticalQueryWorkflow`, `/api/v1/analyze` + MCP `kb_analyze` + CLI,
  full provenance, numeric-faithfulness eval. Cypher fallback **off** by
  default.
- **v1b (heavy tier):** `AnalyticsMaterializeWorkflow` + `top_central_entities`
  + `link_prediction` + admin trigger; (optional) the Temporal Schedule.
- **v1c (long tail):** enable the guarded Cypher fallback after the denylist/
  read-only guard is adversarially tested; log fallbacks for promotion
  (Arc 4).

---

## 12. Further development / Roadmap

v1 is "answer analytical questions on demand". From there the layer grows
along six arcs. **Almost every arc is additive** — it reuses infrastructure
that already exists (Temporal, GDS, communities, the Wikibase anchor, the
date plumbing, the alert pattern), rather than a rewrite. Each arc lists its
ideas and what it reuses.

### Arc 1 — Deeper compute (richer algorithms)

- **Structural node embeddings** (FastRP / node2vec via GDS) → a new
  primitive `similar_entities(name)`: "who is *structurally* similar to this
  company" — a similarity that the current description-vector search cannot
  give.
- **Numeric rollups over `Amount`/`Metric` identifier entities** → a mini-OLAP:
  "sum of contracts per counterparty", "distribution of amounts". Turns the
  graph into a small computable cube.
- **A real temporal graph** — not only `valid_from`/`valid_to` slices but
  proper snapshots over time → dynamic centrality, change-point detection
  ("when did this node suddenly gain links").
- **Motifs / patterns** — ownership cycles, triangles, "stars" around a
  single identifier.
- *Reuses:* GDS projection helpers, the materialization workflow (§5).

### Arc 2 — From Q&A to intelligence ⭐ (highest leverage for this domain)

The corpus is identifier-heavy (INN/OGRN/phone/account) — effectively a
due-diligence / AML domain. The natural evolution:

- **Per-entity `risk_score`** — a composite of signals: sharing an identifier
  with others (`shared_identifier_entities`) + high betweenness + a burst of
  new links + a high share of `negated`/`uncertain` facts → one score,
  materialized as `e.risk_score`.
- **Anomaly detection** — structural outliers (a node bridging unrelated
  clusters), identifier collisions, circular ownership.
- **Continuous monitoring + alerts** — standing analytical queries that
  **re-run on new ingest** and notify on change ("a new company appeared on a
  watched phone", "a watchlist entity's `risk_score` rose"). Turns
  "ask-and-answer" into "the system finds it for you".
- *Reuses:* **Temporal Schedule + materialization + dirty-marking** — exactly
  the patterns already in the codebase (the wiki-sweep dirty/scheduled
  rebuild; the daily-digest alerting in the sibling project).

### Arc 3 — Smarter planning

- **Multi-step *verified* reasoning** — the planner doesn't just pick 1–3
  primitives; it builds a plan → executes → **checks the numbers** → refines.
  Must stay **bounded/deterministic** ("plan-verify-refine", *not* a return
  to the free ReAct loop that was deliberately removed).
- **Comparative & counterfactual analytics** — "compare two entities /
  periods / communities"; "how does the network degrade if this node is
  removed" (resilience).
- **Hybrid analytics + search** — the natural fusion of the two layers:
  compute (centrality / anomaly) → pull **chunk evidence** about what was
  found → synthesize. "Who is most influential *and* what is written about
  them" in one answer.
- *Reuses:* the retrieval layer (for the evidence half), the planner pattern.

### Arc 4 — Self-improving catalog + trust

- **Promote Cypher-fallbacks into primitives** — log fallbacks; a recurring
  pattern becomes an automatic candidate for a first-class catalog primitive.
  The catalog grows from real demand, the core stays deterministic, and the
  hybrid architecture "compounds" over time (less reliance on text-to-Cypher).
- **Numeric-faithfulness eval** (extends `tests/eval/`) — golden analytical
  Q&A; verify (a) the computed numbers are correct, (b) the synthesis does
  **not** go beyond `rows` (anti-hallucination of quantities).
- **Calibration / uncertainty** — warnings like "based on only 3 edges",
  "name is ambiguous — 2 candidates" (`graph_stats` already counts
  duplicates); `link_prediction` scores surfaced as hypotheses.
- *Reuses:* the provenance chain (§7), the existing eval harness.

### Arc 5 — Surfaces & delivery

- **Return subgraphs for visualization** (ego-nets, paths, community maps) as
  renderable structures for a front-end (cytoscape/D3) — this brings back the
  deferred BI/viz path, but now as an *output of analytics*.
- **Graph-health & risk dashboards** in Grafana (the stack already exists):
  counts over time, `risk_score` distribution, duplicate growth.
- **Scheduled analytical digests** to users (Temporal Schedule) — "what
  changed / new risks this week".
- **Conversational analytics** — multi-turn sessions that remember
  intermediate results ("now break that down by year").
- *Reuses:* Grafana/Prometheus, Temporal Schedule, `history` /
  `contextualize_query`.

### Arc 6 — Data enrichment (feeding the analytics better data)

- **External registries via the Wikibase anchor** — `INN → official company
  data` over WDQS/SPARQL — cross-reference the internal graph with the
  curated anchor. **Uniquely available here** (the anchor is already built).
- **Source-weighted analytics** — weight edges by source reliability /
  recency, not only co-occurrence count.
- **Department / tenant slicing** — ARCHITECTURE §9 notes `department` flows
  through metadata but is not enforced; analytics is the natural place to
  apply it.
- *Reuses:* the Wikibase populator + WDQS, document metadata.

### Highest-leverage for this corpus (what to do first after v1)

1. **Arc 2 — continuous risk-monitoring + alerts.** Turns the layer from a
   tool into a *watch system*, hits the identifier/due-diligence essence of
   the corpus directly, and assembles almost entirely from existing patterns
   (Schedule + materialization + dirty-marking).
2. **Arc 3 — hybrid analytics + search.** The platform's unique value:
   neither a pure-RAG nor a pure-graph-analytics system gives "compute →
   prove with citations" in one answer.
3. **Arc 4 — fallback→primitive promotion.** A cheap mechanism that makes the
   catalog self-filling and steadily shrinks the need for the risky
   text-to-Cypher path.

### Horizons

| Horizon | Arcs | Why here |
|---|---|---|
| **near** | Arc 1, Arc 4 | grow directly on the v1 online infrastructure |
| **mid** | Arc 2, Arc 3 | need the materialization workflow + a Temporal Schedule |
| **far** | Arc 5, Arc 6 | viz front-end, external enrichment |

---

## 13. Risks & open questions

- **Schema accuracy:** the catalog Cypher and the schema card assume the
  mapped schema; the exact relationship-type set, the `mention_count`
  property, the `IN_COMMUNITY` edge, and edge directions must be verified
  against `schema.py`/`communities.py` (the schema card must be generated,
  §6).
- **GDS API unverified** against a live install (same caveat `analysis.py`
  carries) — needs a live smoke test for the materialization workflow.
- **Cypher fallback safety:** read-only enforcement is security-sensitive;
  keep it behind a flag (default off) until the denylist + read-tx guard are
  adversarially tested. Decide the default at review.
- **Name resolution / duplicates:** duplicate entity names can make
  single-entity primitives ambiguous — return candidates, don't silently
  pick one.
- **Synthesis faithfulness:** the LLM must not invent numbers beyond `rows`;
  enforced by the numeric-faithfulness eval.
- **Materialization freshness:** until the Schedule lands, 🟠 metrics are as
  stale as the last admin-triggered run — surface "computed at" in provenance.
- **Scope confirmation needed:** default of `ANALYTICS_CYPHER_FALLBACK_ENABLED`;
  whether v1 ships all four families or v1a/v1b split per §11.

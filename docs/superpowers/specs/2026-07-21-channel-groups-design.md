# Channel groups — design

- **Date:** 2026-07-21
- **Status:** Approved (design), pending implementation plan
- **Author:** ops (a.tzitanov) + Claude

## Motivation

Telegram source channels are being organised into six editorial **groups**:

```
news · analytics · digest · opinion · official · data
```

The group of a channel is a strong signal of source *type* and *trust*. We
want that signal to flow through the RAG pipeline and be used in three places:

1. **Search filter / scope** — a query may restrict to a set of groups
   (e.g. only `official`+`data`, or exclude `opinion`).
2. **Rerank weight** — groups bias ranking (e.g. `official`/`data` up,
   `opinion` down).
3. **Synthesis context** — the answer LLM sees each source's group so it can
   weigh credibility and cite precisely.

The design **mirrors the existing date-filter feature** (`doc_date_epoch`):
the label is stamped into chunk metadata at ingest, pushed down into the
Milvus vector search as a `MetadataFilters`, and mirrored as a graph/walk
post-filter.

## Timing / migration

A full data wipe just ran (2026-07-21), so all collections are empty and the
stack is re-ingesting from the live Telegram feed. Shipping this feature now
means **every re-ingested chunk carries `doc_group` from the start — zero
backfill/migration debt.** Chunks ingested before the feature lands (the
handful since the wipe) simply lack `doc_group`: an `IN` filter won't match
them, `NIN` won't exclude them, and rerank treats a missing group as weight
`1.0`. Acceptable; they age out as re-ingest proceeds.

## The group set

A single source of truth for the enum, imported everywhere:

```python
# src/retrieval/groups.py  (new)
GROUPS: tuple[str, ...] = ("news", "analytics", "digest", "opinion", "official", "data")
GROUP_SET = frozenset(GROUPS)
# Tie-break order when a channel is found in >1 group-folder (first wins):
GROUP_PRIORITY: tuple[str, ...] = GROUPS
```

> **Priority note:** the tie-break order is the tuple order above. If a
> different precedence is wanted (e.g. `official` first), it is a one-line
> reorder — defaults to the tuple order.

`""` (empty) is the sentinel for "no group" — valid everywhere a group string
is accepted, never stored as a Milvus filter value.

## Data flow

```
TG folder  ──tg_ingest──▶  /ingest?group=  ──▶  IngestParams.group
   │                                                    │
   │                                              fetch_source → Ctx.group
   │                                                    │
   │                                      parse_and_chunk: md["doc_group"]=group
   │                                                    │
   │                                        index_vector → Milvus dynamic field
   ▼                                                    ▼
(source of truth)                         search: MetadataFilters(doc_group IN …)
                                           rerank: score *= weight[doc_group]
                                           synthesis: prefix "[group]" per source
```

## Component 1 — source of truth: `scripts/tg_ingest.py`

Today `resolve_folders(...)` merges every configured TG folder's channels into
a **single** `include_ids` set (flat sync scope). It does not track which
folder a channel came from.

**Change:**

- `resolve_folders` (and the sync loop that calls it) additionally build a
  `channel_id → group` map, where the group is the **folder title** (matched
  case-insensitively against `GROUP_SET`; folders whose title is not a known
  group are ignored for grouping but may still scope the sync).
- Sync scope becomes the union of the six group-folders. This **replaces**
  `TG_INGEST_FOLDERS=Filtered` — set `TG_INGEST_FOLDERS=news,analytics,digest,opinion,official,data`
  (or a dedicated `TG_GROUP_FOLDERS` env; decided in plan, default reuse
  `TG_INGEST_FOLDERS`).
- **Disjoint rule:** if a channel resolves to >1 group-folder, log a WARNING
  and keep the first by `GROUP_PRIORITY`. A channel in none → `group=""`.
- `_message_to_doc` / the sync path resolves the message's channel id → group;
  `post_ingest` sends it: `data["group"] = group` (omit when empty).

## Component 2 — ingest plumbing

Mirror the `doc_date_epoch` path exactly.

| File | Change |
|------|--------|
| `src/api/routes/ingest.py` | Add `group: str = Form(default="")`. Validate `group in GROUP_SET or group == ""` → else `422`. Pass into `IngestParams(group=group)`. |
| `src/workflow/contracts.py` | `IngestParams`: add `group: str = ""`. `Ctx`: add `group: str = ""`. |
| `src/workflow/activities/fetch_source.py` | Propagate `params.group` into the `Ctx(...)` it builds (both construction sites, mirroring `doc_date_epoch`). |
| `src/workflow/activities/parse_and_chunk.py` | After the `doc_date_epoch` block (~L82): `if ctx.group: md["doc_group"] = ctx.group`. |

Result: every chunk node carries `metadata["doc_group"]`, indexed by
`index_vector` into Milvus as a dynamic field (same mechanism as
`doc_date_epoch`). No fixed-schema change, no dim impact.

> Postgres `documents` is **not** extended in this iteration (search reads
> Milvus chunk metadata, not postgres). Adding a `group` column for analytics
> is a possible follow-up, explicitly out of scope here (YAGNI).

## Component 3 — search filter (Milvus push-down + graph post-filter)

**API** (`src/api/routes/search_v2.py`): add to the local-search request model

```python
groups: list[str] | None = None          # include-list; None/[] = all groups
exclude_groups: list[str] | None = None  # optional exclude-list
```

Validate every element ∈ `GROUP_SET` → `422` otherwise. `groups` and
`exclude_groups` are mutually exclusive per request (validation error if both
non-empty) — keeps semantics unambiguous.

**Filter builder** (`src/retrieval/date_filters.py` or a sibling
`src/retrieval/group_filter.py`): extend the metadata-filter construction so
the retrieve activity combines date bounds AND group into **one**
`MetadataFilters(condition=AND)`:

- include: `MetadataFilter(key="doc_group", value=groups, operator=FilterOperator.IN)`
- exclude: `MetadataFilter(key="doc_group", value=exclude_groups, operator=FilterOperator.NIN)`

**Plumbing:** the search params carry `groups`/`exclude_groups` →
`retrieve_subquestion` builds the combined `MetadataFilters` and passes it to
`get_vector_retriever(top_k, filters=...)` (the seam we already added for
dates). Graph/graph-walk results (which don't go through Milvus) get a mirror
**post-filter**: drop nodes whose `metadata["doc_group"]` is excluded / not in
the include-set (reuse the `filter_nodes` pattern from the date post-filter).

## Component 4 — rerank weights

**Config** (`AgentSettings`, env_prefix `AGENT_`):

```python
group_weights: dict[str, float] = Field(default_factory=lambda: {
    "official": 1.30, "data": 1.25, "analytics": 1.10,
    "news": 1.00, "digest": 0.95, "opinion": 0.80,
})
```

Env override: `AGENT_GROUP_WEIGHTS` (JSON). Missing group / `""` → weight `1.0`.

**Apply** in `src/workflow/search/activities/rerank.py` `rerank_sources`: after
the reranker assigns scores and before the `top_n` sort/cut, multiply each
source's score by `group_weights.get(source.metadata.get("doc_group",""), 1.0)`.
On the fail-open branch (reranker unavailable) weighting is skipped (no scores
to scale) — acceptable, matches existing degradation.

## Component 5 — synthesis context

Sources reach synthesis via `build_synthesize_call` →
`SynthesizeParams.accumulated` (list of `SerializedNode`, each with
`metadata["doc_group"]`) → the `synthesize_answer` activity (large tier).

In `synthesize_answer`, where each source's text is rendered into the prompt
node, **prepend the group label**: `f"[{doc_group}] {text}"` (skip the prefix
when group is empty). This is the only synthesis-side change; it needs no new
params (the label already rides along in metadata).

## Config summary

| Setting | Location | Default |
|---------|----------|---------|
| group enum | `src/retrieval/groups.py` (new) | fixed 6 |
| `AGENT_GROUP_WEIGHTS` | `AgentSettings.group_weights` | official 1.30 / data 1.25 / analytics 1.10 / news 1.00 / digest 0.95 / opinion 0.80 |
| TG group folders | `TG_INGEST_FOLDERS` (env) | the six group titles |

## Edge cases

- **Unknown group at ingest** → `422` (fail fast, never a silent bad tag).
- **Channel in no group-folder** → `group=""`, ingested ungrouped, weight 1.0,
  never excluded by an include-filter (so a broad query still sees it? No — an
  include-list query excludes ungrouped chunks by definition; a no-filter query
  includes them). This is the intended semantics.
- **Channel in multiple group-folders** → WARN + first by priority.
- **Both `groups` and `exclude_groups` set** → `422`.
- **Pre-feature chunks (no `doc_group`)** → not matched by `IN`, not dropped by
  `NIN`, rerank weight 1.0.

## Testing

- `groups.py`: enum/priority stable.
- Filter builder: `IN`/`NIN` MetadataFilter shape; combined AND with date
  bounds; `None`/`[]` → no group filter.
- `tg_ingest`: `resolve_folders` returns correct `channel_id → group`;
  multi-folder channel → priority pick + warning; unknown folder ignored.
- `/ingest`: valid group passes to `IngestParams`; unknown → 422.
- `/search/local`: `groups` validated; both-set → 422; plumbs into retrieve
  filters.
- `parse_and_chunk`: `md["doc_group"]` set when `ctx.group` present, absent
  when empty.
- `rerank`: score scaled by group weight; missing group → ×1.0; fail-open
  branch unaffected.
- e2e: ingest two docs (`official`, `opinion`); query with `groups=["official"]`
  returns only official; unfiltered query ranks official above opinion.

## Non-goals (YAGNI)

- No postgres `documents.group` column (search doesn't need it).
- No auto/LLM group inference from query text (explicit API param only).
- No per-query weights (weights are config-driven).
- No multi-group-per-doc (single string; disjoint by construction).
- No reuse/overload of `department` (kept a distinct org field).

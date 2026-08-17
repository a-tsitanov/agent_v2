"""MCP-2: atomic retrieval tools.

Exposes 8 of the raw retrieval functions from
``src/retrieval/atomic_tools.py`` directly as MCP tools — no
Temporal workflow in between (``filter_by_metadata`` is intentionally
omitted; it operates on an in-process accumulator that stateless
tool-call clients don't maintain).  Used when the MCP client (an LLM
running in Hermes Agent / Claude Desktop / Cursor / OpenWebUI) wants
to drive its own ReAct loop and just needs primitives.

Every tool carries an 1800s (30 min) execution timeout (FastMCP
``@mcp.tool(timeout=...)``, seconds) so a slow graph walk or cold
retriever can't hang a client indefinitely, while still letting the
heaviest legitimate calls finish.

GPU/LLM protection: every LLM call — including those inside
``graph_search`` / ``find_*`` (which use ``LLMSynonymRetriever`` for
query normalisation) — is served by the per-process ``LLMPool``
(``src/retrieval/llm_pool.py``, configured via ``settings.llm_pool``),
which keeps each role BoundedLLM-gated.  Concurrent MCP clients
automatically serialise behind it.

Run::

    # Stdio
    uv run python -m src.mcp.tools_server --transport stdio

    # Streamable HTTP (endpoint /mcp) — OpenWebUI etc.
    uv run python -m src.mcp.tools_server --transport http --port 9002
"""

from __future__ import annotations

import asyncio
import json
from calendar import monthrange
from datetime import date, timedelta
from typing import Any

from fastmcp import FastMCP
from loguru import logger

from src.config import settings
from src.mcp._shared import (
    assert_api_key_env_set,
    build_sse_auth,
    log_banner,
    parse_args,
)
from src.retrieval import atomic_tools
from src.stats.align import GRANULARITIES

mcp = FastMCP(
    name="kb-llamaindex-tools",
    instructions=(
        "Atomic retrieval tools over the project knowledge base.  "
        "Each tool returns a JSON-serialisable dict.  Compose them "
        "yourself in your own LLM loop.  For an already-orchestrated "
        "answer, use the sibling MCP-1 server (kb_search) instead."
    ),
    auth=build_sse_auth(),  # evaluated at import time; see build_sse_auth docstring
)


# ── lazy DI bootstrap ──────────────────────────────────────────────


_lock = asyncio.Lock()
_deps: dict[str, Any] = {}


async def _init() -> None:
    """First-call init: build retriever / graph_retriever /
    chunk_repository / BoundedLLM-wrapped LLM via the same factories
    used by the FastAPI route handlers and the Temporal search-side
    activities (see ``src/workflow/_search_deps.py``)."""
    if _deps:
        return
    async with _lock:
        if _deps:
            return
        from src.ingestion.embeddings import build_embedding_model
        from src.retrieval.vector_index import (
            build_vector_index,
            build_vector_store,
        )
        from src.storage.chunk_repository import ChunkRepository
        from src.storage.postgres import AsyncPostgres

        embed = build_embedding_model()
        store = build_vector_store()
        index = build_vector_index(store, embed)
        _deps["retriever"] = index.as_retriever(similarity_top_k=10)

        from src.retrieval.llm_pool import get_llm_pool
        llm = get_llm_pool().get("search")
        _deps["llm"] = llm

        try:
            from src.graph.index import (
                build_kg_extractor,
                build_property_graph_index,
            )
            from src.graph.retriever import GraphRetriever
            from src.graph.store import build_graph_store
            gs = build_graph_store()
            if settings.graph.backend == "nebula":
                # Nebula has no LlamaIndex PropertyGraphStore; use the
                # store-only retriever (nGQL awalk/find). Mirrors
                # _search_deps._get_graph_retriever. Vector/synonym aretrieve
                # is Phase 3.
                _deps["graph"] = GraphRetriever.for_store(
                    gs,
                    similarity_top_k=settings.agent.graph_similarity_top_k,
                    filter_polarity_temporal=(
                        settings.agent.graph_walk_filter_polarity_temporal
                    ),
                )
            else:
                pg = build_property_graph_index(
                    graph_store=gs, embed_model=embed,
                    extractor=build_kg_extractor(llm), nodes=None,
                    llm=llm,  # local LiteLLM model for the retriever's synonym step
                )
                _deps["graph"] = GraphRetriever(
                    pg,
                    filter_polarity_temporal=(
                        settings.agent.graph_walk_filter_polarity_temporal
                    ),
                )
            # Raw store for the GDS analysis tools (need structured_query).
            _deps["graph_store"] = gs
        except Exception as exc:
            logger.warning(
                "MCP-2: graph_retriever disabled (Neo4j down?): {e}",
                e=exc,
            )
            _deps["graph"] = None
            _deps["graph_store"] = None

        _deps["chunks"] = ChunkRepository(pg=AsyncPostgres())


async def _r():
    await _init()
    return _deps["retriever"]


async def _g():
    await _init()
    return _deps["graph"]


async def _c():
    await _init()
    return _deps["chunks"]


async def _gs():
    await _init()
    return _deps.get("graph_store")


# ── MCP tools (one per atomic_tools.* function) ────────────────────


@mcp.tool(timeout=1800)
async def vector_search(query: str, top_k: int = 10) -> dict[str, Any]:
    """Hybrid (BM25 + dense vector + RRF) semantic search over document
    chunks. Returns the top_k matching chunks with text + metadata.

    USE FOR: factual / "what / how / why" questions answered by the text
    of documents. This is the default for content questions.
    NOT FOR: entity lookup or relationships — use the graph tools
    (`graph_search`, `find_entity_by_*`, `find_neighbours`, `graph_walk`).
    NEXT STEP: to read more around a good hit, call `get_chunks_by_doc_id`
    with its `doc_id`.
    """
    r = await atomic_tools.vector_search(await _r(), query=query, top_k=top_k)
    return {"sources": json.loads(r.observation)}


@mcp.tool(timeout=1800)
async def graph_search(query: str, depth: int = 1) -> dict[str, Any]:
    """Similarity search over the knowledge graph from a free-text query.
    Returns matched entities + their neighbours up to `depth` triplet-hops
    + related chunks.

    `depth` controls neighbour expansion (1-3, default 1): 1 = matched
    nodes + immediate relations; raise for wider context (more nodes /
    tokens / latency).
    USE FOR: graph / relationship questions when you have NO specific
    entity pinned yet — discovery ("что в графе связано с темой X").
    PREFER INSTEAD: `find_entity_by_id` (you have an exact identifier),
    `find_entity_by_name` (you have a partial name), `find_neighbours`
    (you have a known entity, want 1 hop), `graph_walk` (known entity,
    multi-hop chains). Returns empty lists if Neo4j is unavailable."""
    r = await atomic_tools.graph_search(await _g(), query=query, depth=depth)
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def graph_walk(
    start_entity: str, hops: int = 2, rel_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Bounded MULTI-hop traversal following real relationships outward
    from a KNOWN start entity (hard-capped nodes / edges / hops).

    USE FOR: transitive connection / chain questions — "how is X linked
    to Y", "trace X's network N hops out". Optionally restrict to
    `rel_filter` relationship types. Returns entities + relations.
    NOT FOR: a single hop (use `find_neighbours`) or discovery without a
    start entity (use `graph_search` / `find_entity_by_name` first)."""
    r = await atomic_tools.graph_walk(
        await _g(), start_entity=start_entity, hops=hops,
        rel_filter=rel_filter,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_entity_by_id(
    name: str, entity_type: str | None = None,
) -> dict[str, Any]:
    """Exact pinpoint lookup by a precise identifier or canonical name
    (E.164 phone, INN, OGRN, SNILS, email, exact entity name).

    USE FOR: "whose number is +7…", "find INN 7707…" — when the user
    gives an exact identifier. One entity expected.
    PREFER INSTEAD: `find_entity_by_name` when you only have a partial /
    approximate name; `graph_search` for non-name semantic queries."""
    r = await atomic_tools.find_entity_by_id(
        await _g(), name=name, entity_type=entity_type,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_entity_by_name(
    query: str, limit: int = 10,
) -> dict[str, Any]:
    """Fuzzy / partial NAME search via the full-text index ("Иванов" →
    "Иванов Иван Иванович"; "Ромаш" → "ООО Ромашка").

    USE FOR: resolving an approximate / partial name into canonical
    entities — usually the first step before drilling in with
    `find_neighbours` or `graph_walk`.
    PREFER INSTEAD: `find_entity_by_id` for an exact identifier;
    `graph_search` for non-name semantic queries. Returns entity rows
    only (no chunks)."""
    r = await atomic_tools.find_entity_by_name(
        await _g(), query=query, limit=limit,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def find_neighbours(
    entity_name: str, hops: int = 1,
) -> dict[str, Any]:
    """Neighbours + relations of a KNOWN entity, up to `hops` triplet-hops.

    `hops` controls neighbour depth (1-3, default 1 = immediate). Raising
    it widens context (more nodes / tokens / latency).
    USE FOR: "who / what is connected to X", and as the core of an entity
    dossier.
    PREFER INSTEAD: `graph_walk` for deterministic edge-following with
    rel-type filtering; `graph_search` if the entity isn't pinned down
    yet; `find_entity_*` to resolve the entity first."""
    r = await atomic_tools.find_neighbours(
        await _g(), entity_name=entity_name, hops=hops,
    )
    return json.loads(r.observation)


@mcp.tool(timeout=1800)
async def get_chunks_by_doc_id(
    doc_id: str, limit: int = 50, offset: int = 0,
) -> dict[str, Any]:
    """Fetch chunks of ONE document by `doc_id`, in position order,
    paginated via `limit` / `offset`.

    USE FOR: expanding context around a `vector_search` hit within the
    same source, or reading a document chunk-by-chunk.
    REQUIRES: a `doc_id` you already have (from `vector_search` or graph
    results). For the whole raw file at once, see `read_full_document`."""
    r = await atomic_tools.get_chunks_by_doc_id(
        await _c(), doc_id=doc_id, limit=limit, offset=offset,
    )
    # Observation is either a JSON list of chunks or an error dict.
    try:
        return {"chunks": json.loads(r.observation)}
    except json.JSONDecodeError:
        return {"error": r.observation}


@mcp.tool(timeout=1800)
async def read_full_document(
    doc_id: str, max_chars: int = 20000,
) -> dict[str, Any]:
    """Raw full text of the original uploaded file (pre-chunk,
    pre-translation), capped at `max_chars`.

    USE FOR: tables, code, or short documents where chunk-level
    retrieval splits content badly.
    REQUIRES: a `doc_id`. For large documents prefer
    `get_chunks_by_doc_id` (this returns one big blob, truncated at
    `max_chars`)."""
    r = await atomic_tools.read_full_document(
        await _c(), doc_id=doc_id, max_chars=max_chars,
    )
    if r.observation.startswith("Error"):
        return {"error": r.observation}
    return {"text": r.observation}


# ── graph analysis tools (Track 7b — read-only GDS) ────────────────
#
# Diagnostic / exploratory, NOT retrieval: they run GDS algorithms over
# the whole __Entity__ graph (heavier, projection-backed) and are
# fail-soft — Neo4j/GDS down or unavailable yields a safe empty result,
# never an error.  `from src.graph import analysis` is module-level.


@mcp.tool(timeout=1800)
async def graph_pagerank(top_n: int = 20) -> dict[str, Any]:
    """Most CENTRAL entities in the whole graph by weighted PageRank.

    USE FOR: "who/what are the most important / connected entities" —
    a global importance ranking, not tied to a query.
    PREFER INSTEAD: `graph_personalized_pagerank` when you want
    importance *relative to* specific seed entities. Returns top_n
    {name, score}. Empty if Neo4j/GDS is unavailable."""
    from src.graph import analysis
    return {"top": await analysis.pagerank(await _gs(), top_n=top_n)}


@mcp.tool(timeout=1800)
async def graph_personalized_pagerank(
    seeds: list[str], top_n: int = 20,
) -> dict[str, Any]:
    """Entities most relevant to the SEED entities by seed-biased
    (personalized) PageRank.

    USE FOR: "what's most connected to X and Y" — centrality relative to
    `seeds` (exact entity names). Resolve names first with
    `find_entity_by_name` if unsure. Empty seeds → empty result. Returns
    top_n {name, score}; empty if Neo4j/GDS is unavailable."""
    from src.graph import analysis
    return {"top": await analysis.personalized_pagerank(
        await _gs(), seeds, top_n=top_n,
    )}


@mcp.tool(timeout=1800)
async def graph_components() -> dict[str, Any]:
    """Weakly-connected-component count + size distribution of the graph.

    USE FOR: connectivity health — a large singleton fraction explains
    sparse/empty communities and weak multi-hop reach. Returns
    {component_count, distribution}; empty if Neo4j/GDS is unavailable."""
    from src.graph import analysis
    return await analysis.components(await _gs())


@mcp.tool(timeout=1800)
async def graph_shortest_path(
    source: str, target: str, max_hops: int = 6,
) -> dict[str, Any]:
    """Shortest undirected path between two entities by EXACT name.

    USE FOR: "how are X and Y connected" — the concrete chain of entities
    between them. Resolve names first with `find_entity_by_name` if
    unsure. Returns {path:[names], hops}; {path:[], hops:-1} when there's
    no path (or Neo4j unavailable)."""
    from src.graph import analysis
    return await analysis.shortest_path(
        await _gs(), source, target, max_hops=max_hops,
    )


@mcp.tool(timeout=1800)
async def graph_stats() -> dict[str, Any]:
    """Operational snapshot of the knowledge graph: entity / relationship
    counts, degree distribution (p50/p99/max), duplicate-name groups,
    community count.

    USE FOR: a quick health/size read of the graph.

    A field that could not be measured is `null`, NEVER 0, and the reason
    is under `errors` — do not read a null as "the graph has none of
    these". `source` says where the counts came from: `show_stats` means
    Nebula's own job results, exact but as old as `stats_computed_at`;
    `scan` means counted live. Degree and duplicate counts have no cheap
    equivalent and are `null` whenever the graph is too large to scan."""
    from src.graph import analysis
    return await analysis.graph_stats(await _gs())


# ── ingest statistics (Postgres `documents` pipeline) ──────────────
#
# NOT retrieval and NOT graph: a direct read over the `documents` table's
# per-channel / per-group processing state.  Deliberately BYPASSES _init()
# (no retriever/graph bootstrap needed for a plain Postgres aggregate) and
# calls AsyncPostgres — the SAME aggregation methods the /api/v1/stats REST
# routes and the message_stats CLI use, so all three surfaces report identical
# numbers.  The thin `_stats_by` / `_timeline` helpers hold the validation so
# they stay unit-testable without FastMCP tool-invocation internals.


async def _stats_by(
    group_by: str, since: str | None, until: str | None,
) -> dict[str, Any]:
    from datetime import date

    from src.storage.postgres import GROUP_BY_COLUMN, AsyncPostgres

    dimension = GROUP_BY_COLUMN.get(group_by)
    if dimension is None:
        return {"error": f"group_by must be one of {sorted(GROUP_BY_COLUMN)}"}
    try:
        s = date.fromisoformat(since) if since else None
        u = date.fromisoformat(until) if until else None
    except ValueError as exc:
        return {"error": f"since/until must be ISO YYYY-MM-DD: {exc}"}
    rows = await AsyncPostgres().status_counts_by(dimension, since=s, until=u)
    return {"group_by": group_by, "rows": rows}


async def _timeline(
    date_field: str, group_by: str | None, channel: str | None,
    group: str | None, since: str | None, until: str | None,
) -> dict[str, Any]:
    from datetime import date

    from src.storage.postgres import GROUP_BY_COLUMN, AsyncPostgres

    if date_field not in ("created_at", "doc_date"):
        return {"error": "date_field must be 'created_at' or 'doc_date'"}
    if group_by is not None and group_by not in GROUP_BY_COLUMN:
        return {"error": f"group_by must be one of {sorted(GROUP_BY_COLUMN)}"}
    try:
        s = date.fromisoformat(since) if since else None
        u = date.fromisoformat(until) if until else None
    except ValueError as exc:
        return {"error": f"since/until must be ISO YYYY-MM-DD: {exc}"}
    buckets = await AsyncPostgres().timeline_counts(
        date_field=date_field, group_by=group_by,
        channel=channel, group=group, since=s, until=u,
    )
    # `day` is a datetime.date → ISO-stringify for a JSON-safe payload.
    return {
        "date_field": date_field,
        "buckets": [{**b, "day": b["day"].isoformat()} for b in buckets],
    }


def _period_bounds(period: str, granularity: str) -> tuple[date, date]:
    """First and last day covered by a bucket key from
    ``epoch_days_to_period``."""
    if granularity == "year":
        y = int(period)
        return date(y, 1, 1), date(y, 12, 31)
    if granularity == "quarter":
        y, q = int(period[:4]), int(period[-1])
        m0 = 3 * (q - 1) + 1
        m1 = m0 + 2
        return date(y, m0, 1), date(y, m1, monthrange(y, m1)[1])
    if granularity == "month":
        y, m = int(period[:4]), int(period[5:7])
        return date(y, m, 1), date(y, m, monthrange(y, m)[1])
    start = date.fromisoformat(period)
    if granularity == "week":
        return start, start + timedelta(days=6)
    return start, start


def _period_in_window(
    period: str, granularity: str, since: str | None, until: str | None,
) -> bool:
    """Keep a bucket that OVERLAPS the window.

    Overlap rather than containment: a coarse bucket straddling a
    boundary still carries mentions from inside the window, and dropping
    it would silently lose them.  The previous implementation compared
    truncated ISO strings, which broke on quarter keys — ``'Q'`` sorts
    after every digit, so ``"2026-Q2"`` compared greater than every
    ``"YYYY-MM"`` bound.
    """
    start, end = _period_bounds(period, granularity)
    if since and end < date.fromisoformat(since):
        return False
    return not (until and start > date.fromisoformat(until))


# Backends whose store cannot serve `topic_trend` AT ALL.  The primitive
# calls `run_rows(store, cypher, {"topic": topic})`; nebula's
# `structured_query` raises NotImplementedError on a non-empty `param_map`
# (src/graph/nebula_store.py:268-272) and `run_rows` fail-softs every
# exception to `[]` (src/analytics/store_query.py:23-25).  The tool would
# therefore answer "0 mentions, ever" for every topic — indistinguishable
# from a real zero.  A missing answer is honest; a silent zero is not.
#
# DELETE THIS GUARD (and this constant) as soon as NebulaGraphStore binds
# nGQL parameters.  Nothing else needs to change.
_TOPIC_TREND_UNSUPPORTED_BACKENDS = frozenset({"nebula"})


async def _topic_trend(
    store: Any, topic: str, granularity: str,
    since: str | None, until: str | None, _fn: Any = None,
) -> dict[str, Any]:
    from datetime import date

    from src.analytics.primitives.dynamics import topic_trend as _primitive

    if not topic or not topic.strip():
        return {"error": "topic must be a non-empty string"}
    if granularity not in GRANULARITIES:
        return {"error": f"granularity must be one of {sorted(GRANULARITIES)}"}
    for name, value in (("since", since), ("until", until)):
        if value is not None:
            try:
                date.fromisoformat(value)
            except ValueError:
                return {"error": f"{name} must be ISO YYYY-MM-DD, got {value!r}"}
    if store is None:
        return {"error": "graph store unavailable"}
    if settings.graph.backend in _TOPIC_TREND_UNSUPPORTED_BACKENDS:
        return {"error": (
            f"topic_trend is unavailable on the {settings.graph.backend} graph "
            "backend: its query binds a parameter, which NebulaGraphStore "
            "does not support yet, and the analytics layer degrades that to "
            "an empty result — which would be reported as 'never mentioned'. "
            "No trend can be returned for any topic on this backend."
        )}
    fn = _fn or _primitive
    res = await fn(store, topic=topic.strip(), granularity=granularity)
    rows = []
    for r in res.rows:
        if not _period_in_window(r["period"], granularity, since, until):
            continue
        # `period` is a LABEL (`"2026-Q1"`, `"2026-03"`, `"2026"`), which
        # `date.fromisoformat` cannot parse.  MCP-3's `stat_align` wants
        # `{period_start, value}`, so emit those too — additively, and
        # from here, because this module is the one that knows both the
        # label format and the granularity.  MCP-3 must not learn either.
        rows.append({
            **r,
            "period_start": _period_bounds(r["period"], granularity)[0].isoformat(),
            "value": r["mentions"],
        })
    return {"topic": topic.strip(), "granularity": granularity, "rows": rows}


async def _polarity_evolution(
    store: Any, name: str | None, rel_type: str | None, _fn: Any = None,
) -> dict[str, Any]:
    """No `period_start`/`value` here, deliberately — see the tool docstring.

    Rows come from `build_dynamics_graph_ops(store).polarity_evolution`,
    which returns one row per (period, polarity) pair, not a series.  Its
    nebula implementation inlines literals into nGQL and passes no
    `param_map`, so it does NOT share `topic_trend`'s backend fault and
    is not guarded.
    """
    from src.analytics.primitives.dynamics import polarity_evolution as _primitive

    if not name and not rel_type:
        return {"error": "provide at least one of name / rel_type"}
    if store is None:
        return {"error": "graph store unavailable"}
    fn = _fn or _primitive
    res = await fn(store, name=name, rel_type=rel_type)
    return {"name": name, "rel_type": rel_type, "rows": res.rows}


@mcp.tool(timeout=120)
async def channel_message_stats(
    group_by: str = "channel",
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """Counts of INGESTED Telegram messages, grouped by source channel (or
    channel group), broken down by pipeline status (completed / pending /
    processing / failed / vector_only / skipped) plus a per-key total, sorted
    by total descending.

    USE FOR: "сколько сообщений обработано", "статистика по каналам", "how many
    posts per channel", pipeline-health questions (a channel's `pending` is its
    processing backlog). `group_by="channel"` (default) for per-channel;
    `group_by="group"` for per folder-group (news / analytics / …). Optional
    `since` / `until` are ISO `YYYY-MM-DD` bounds on ingest time (created_at).
    Documents with no channel tag are excluded.
    NOT FOR: message CONTENT / semantic search (use `vector_search`) or graph
    entities. NEXT STEP: for daily dynamics call `channel_message_timeline`."""
    return await _stats_by(group_by, since, until)


@mcp.tool(timeout=120)
async def channel_message_timeline(
    date_field: str = "created_at",
    group_by: str | None = None,
    channel: str | None = None,
    group: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """Daily counts of ingested Telegram messages — the ingest volume over
    time.

    USE FOR: "динамика ингеста", "сколько сообщений в день", ingest-trend
    questions. `date_field`: "created_at" (when processed, default) or
    "doc_date" (the post's original date). Optionally break out per key with
    `group_by="channel"|"group"`, and/or filter to a single `channel` /
    `group`. `since` / `until` are ISO `YYYY-MM-DD` bounds. Returns
    `{date_field, buckets:[{day, count[, key]}]}`, ordered by day.
    NOT FOR: per-channel status totals (use `channel_message_stats`)."""
    return await _timeline(date_field, group_by, channel, group, since, until)


@mcp.tool(timeout=1800)
async def topic_trend(
    topic: str,
    granularity: str = "month",
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """How often a topic is mentioned across ingested documents, per period
    — the channel-side ATTENTION series.

    USE FOR: "как часто писали про X", and as the channel-side input to
    the MCP-3 `stat_align` tool when comparing attention against a poll
    indicator.  `granularity`: day / week / month (default) / quarter /
    year.  `since` / `until` are ISO `YYYY-MM-DD` bounds applied to the
    returned buckets.  Returns `{topic, granularity, rows:[{period,
    mentions, period_start, value}]}`, where `period` is a human label
    (`"2026-Q1"`, `"2026-03"`) and `period_start` is that bucket's first
    day as an ISO date.  PASS THE ROWS STRAIGHT TO `stat_align`: it reads
    `period_start` and `value`, which is why both are here.
    If the graph backend cannot serve this query, you get `{"error": …}`
    rather than an empty series — do not read a missing answer as "this
    topic was never mentioned".
    NOT FOR: ingest volume (use `channel_message_timeline`) or message
    content (use `vector_search`)."""
    await _init()
    return await _topic_trend(
        _deps.get("graph_store"), topic, granularity, since, until,
    )


@mcp.tool(timeout=1800)
async def polarity_evolution(
    name: str | None = None, rel_type: str | None = None,
) -> dict[str, Any]:
    """How the polarity of an entity's relations shifts over time — the
    channel-side VALUATION series.

    USE FOR: "как менялось отношение к X". Provide at least one of
    `name` (entity) / `rel_type` (relation type).  Polarity is computed
    over graph EDGES, not per-message sentiment — it is a coarser signal
    than a poll's rating scale, so read it as direction rather than
    magnitude.

    NOT DIRECTLY USABLE BY `stat_align`, unlike `topic_trend`.  Rows are
    `{period, polarity, n}` — a per-polarity BREAKDOWN with several rows
    per period, not one series, and `period` is always a month label
    (`"2026-03"`) regardless of what you asked for.  There is no single
    `value` to align: which polarity counts as "the" series is a
    judgement, so this tool does not make it for you.  To compare tone
    against a poll you must pick one polarity, decide whether you want
    its count or its share of the period, and build `{period_start,
    value}` points yourself.
    NOT FOR: mention counts (use `topic_trend`, whose rows DO feed
    `stat_align` unchanged)."""
    await _init()
    return await _polarity_evolution(
        _deps.get("graph_store"), name, rel_type,
    )


# Note: filter_by_metadata is not exposed via MCP-2.  It only makes
# sense in the context of an existing accumulator, which atomic-MCP
# clients don't maintain — they pass each tool call as a fresh
# request and assemble context themselves.


def main() -> None:
    args = parse_args()
    assert_api_key_env_set()
    log_banner(
        "kb-llamaindex-tools",
        transport=args["transport"], host=args["host"], port=args["port"],
    )
    if args["transport"] == "stdio":
        mcp.run(transport="stdio")
    else:
        # Streamable HTTP (modern MCP transport; endpoint `/mcp`).  Replaces
        # the legacy SSE transport for MCP-2.
        mcp.run(
            transport="http",
            host=args["host"], port=args["port"],
        )


if __name__ == "__main__":
    main()

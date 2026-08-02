"""Offline community-build activities (Search R6, decision C2).

Two activities, both run on the dedicated ``kb-graph-build`` queue — never
on the query hot path:

  * ``detect_communities_activity`` — wraps ``detect_communities`` (GDS
    Leiden) and returns the detected communities.
  * ``summarize_community_activity`` — for ONE community, generate a
    STRUCTURED REPORT ``{title, summary, findings:[{statement,
    importance}]}`` via the SMALL-tier LLM (``get_llm_pool().get("retrieve")`` →
    small tier, gated by the global N semaphore), embed ``title + summary``
    and persist the report (JSON),
    its components, and a native ``report_vec`` on the ``:Community`` node
    (idempotent MERGE).  Built bottom-up: level-0 reports draw on member
    entities/relations; level k>0 reports draw on CHILD reports (cheaper
    than re-reading every leaf member).  Batchable: the workflow fans out
    one call per community with bounded parallelism.

Both are fail-safe by construction — a store/LLM error is logged and
returns an empty/non-persisted result rather than raising through the
Temporal boundary (the offline build must never crash; a partial rebuild
is fine and the next run reconciles).
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from temporalio import activity

from src.graph.community_summarize import build_community_summarize
from src.graph.community_vector_store import build_community_report_vector_store
from src.workflow.contracts import (
    DetectCommunitiesParams,
    DetectCommunitiesResult,
    DetectedCommunity,
    SummarizeCommunityParams,
    SummarizeCommunityResult,
)

# Small-tier, /no_think, Russian-friendly.  Returns a STRUCTURED report as
# JSON.  Importance is a 1-100 salience score the dynamic-selection phase
# (Phase 4) will rank findings by.
_REPORT_PROMPT = (
    "/no_think\n"
    "Ты аналитик графа знаний. Ниже — контекст одного сообщества "
    "(тесно связанная группа сущностей либо сводки дочерних сообществ). "
    "Составь СТРУКТУРИРОВАННЫЙ отчёт о сообществе на русском языке.\n"
    "\n"
    "Верни СТРОГО валидный JSON-объект (и больше ничего) такого вида:\n"
    '{{"title": "...", "summary": "...", '
    '"findings": [{{"statement": "...", "importance": 50}}]}}\n'
    "\n"
    "Где:\n"
    "- title — короткое название сообщества (3-7 слов);\n"
    "- summary — связное резюме на 3-5 предложений: что это за группа, "
    "какие сущности в неё входят и как они связаны;\n"
    "- findings — 2-5 ключевых выводов; каждый: statement (одно "
    "предложение) и importance (целое 1-100, важность вывода).\n"
    "Имена и цитаты оставляй в языке оригинала. Без markdown, без "
    "пояснений вне JSON.\n"
    "\n"
    "Контекст сообщества:\n"
    "{context}\n"
)

# Max rendered-context size (chars) fed to the small LLM in ONE call.  A
# root/level-0 community can hold ~all entities → its member context runs to
# hundreds of thousands of tokens and overflows the small model (gemma
# 262144).  When the context exceeds this budget we MAP-REDUCE it: split the
# rendered body into <=budget batches, summarize each into a partial report,
# then reduce the partials into the final report.  Conservative (well under
# 262144 tokens even at ~1 token/char for Cyrillic) with headroom for the
# prompt template + output.  Module-level so tests can monkeypatch it small.
_CONTEXT_CHAR_BUDGET = 150_000


def _parse_report(text: str) -> dict:
    """Tolerantly parse the LLM's structured-report JSON.

    On any parse failure return a fallback shape that still carries the
    raw text as the summary — never raises, so the activity stays
    fail-safe and we always have *something* to embed/persist.
    """
    raw = (text or "").strip()
    fallback = {"title": "", "summary": raw, "findings": []}
    if not raw:
        return fallback

    candidate = raw
    # Strip a ```json … ``` fence if the model added one.
    fence = re.search(r"```(?:json)?\s*(.+?)```", raw, re.DOTALL)
    if fence:
        candidate = fence.group(1).strip()
    else:
        # Otherwise grab the outermost {...} span if there's surrounding prose.
        start, end = candidate.find("{"), candidate.rfind("}")
        if start != -1 and end > start:
            candidate = candidate[start : end + 1]

    try:
        obj = json.loads(candidate)
    except Exception:
        return fallback
    if not isinstance(obj, dict):
        return fallback

    title = obj.get("title")
    summary = obj.get("summary")
    findings_in = obj.get("findings")

    findings: list[dict] = []
    if isinstance(findings_in, list):
        for f in findings_in:
            if not isinstance(f, dict):
                continue
            stmt = f.get("statement")
            if not isinstance(stmt, str) or not stmt.strip():
                continue
            imp = f.get("importance")
            try:
                imp_int = int(imp)
            except (TypeError, ValueError):
                imp_int = 0
            findings.append({"statement": stmt.strip(), "importance": imp_int})

    return {
        "title": title.strip() if isinstance(title, str) else "",
        "summary": summary.strip() if isinstance(summary, str) else raw,
        "findings": findings,
    }


def _get_store() -> Any | None:
    """Build the Neo4j graph store (or ``None`` when unreachable).

    Indirected through a module-level fn so tests can monkeypatch it
    without touching the heavy Neo4j factory."""
    try:
        from src.graph.store import build_graph_store

        return build_graph_store()
    except Exception as exc:
        activity.logger.warning("community: graph store unavailable: %s", exc)
        return None


def _get_summary_llm() -> Any:
    """Small-tier LLM for community summaries.

    Uses the ``retrieve`` role (small tier per ``_DEFAULT_ROLE_TIERS``) so
    summaries NEVER occupy the large synthesis model.  Indirected for
    monkeypatching in tests.  Returns the pooled LLM via
    ``get_llm_pool().get('retrieve')`` so the global N semaphore counts it."""
    from src.retrieval.llm_pool import get_llm_pool

    return get_llm_pool().get("retrieve")


def _get_embed_model() -> Any:
    """Embedding model for report vectors (same model as ER / ingest).

    Indirected through a module-level fn so tests can monkeypatch it."""
    from src.ingestion.embeddings import build_embedding_model

    return build_embedding_model()


def _build_member_context(rows: list[dict]) -> str:
    """Render member-context rows (level-0) into the prompt context body."""
    lines = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name") or ""
        desc = (row.get("description") or "").replace("\n", " ")[:400]
        rels = ", ".join(str(t) for t in (row.get("rel_types") or []) if t)
        line = f"- {name}"
        if desc:
            line += f": {desc}"
        if rels:
            line += f"  (связи: {rels})"
        lines.append(line)
    return "Сущности сообщества:\n" + "\n".join(lines)


def _build_child_context(rows: list[dict]) -> str:
    """Render child-report rows (level>0) into the prompt context body."""
    lines = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = (row.get("title") or "").replace("\n", " ").strip()
        summary = (row.get("summary") or "").replace("\n", " ").strip()[:600]
        if not title and not summary:
            continue
        if title:
            lines.append(f"- {title}: {summary}" if summary else f"- {title}")
        else:
            lines.append(f"- {summary}")
    return "Дочерние сообщества (их сводки):\n" + "\n".join(lines)


def _split_context_into_batches(context: str, budget: int) -> list[str]:
    """Split a rendered context body into ``<=budget``-char batches, each
    keeping the FIRST (header) line so every map prompt stays well-formed.

    Generic over member- and child-context (both are ``header\\n- item…``).
    A single item line longer than the budget still forms its own batch
    (never dropped) — member/child lines are bounded (desc[:400] /
    summary[:600]) so this is a safety net, not the common path."""
    lines = context.split("\n")
    if not lines:
        return []
    header = lines[0]
    items = [ln for ln in lines[1:] if ln.strip()]
    base = len(header) + 1
    batches: list[str] = []
    cur: list[str] = []
    cur_len = base
    for ln in items:
        add = len(ln) + 1
        if cur and cur_len + add > budget:
            batches.append(header + "\n" + "\n".join(cur))
            cur, cur_len = [], base
        cur.append(ln)
        cur_len += add
    if cur:
        batches.append(header + "\n" + "\n".join(cur))
    return batches


async def _summarize_once(llm: Any, context: str) -> str:
    """One structured-report LLM call over a (batch-sized) context body."""
    resp = await llm.acomplete(_REPORT_PROMPT.format(context=context))
    return (getattr(resp, "text", None) or str(resp)).strip()


async def _reduce_partials(llm: Any, partials: list[dict]) -> str:
    """Reduce per-batch partial reports into ONE report.  Partials are fed
    as child-report context (title + summary) — the same shape a level>0
    community consumes.  If the combined child context itself exceeds the
    budget (very many batches), reduce HIERARCHICALLY until it fits (bounded
    by a small guard so a pathological graph can't loop forever)."""
    guard = 0
    while True:
        guard += 1
        ctx = _build_child_context(partials)
        if len(ctx) <= _CONTEXT_CHAR_BUDGET or len(partials) <= 1 or guard > 5:
            return await _summarize_once(llm, ctx)
        collapsed: list[dict] = []
        for batch in _split_context_into_batches(ctx, _CONTEXT_CHAR_BUDGET):
            rep = _parse_report(await _summarize_once(llm, batch))
            if rep.get("summary") or rep.get("title"):
                collapsed.append({
                    "title": rep.get("title", ""),
                    "summary": rep.get("summary", ""),
                })
        partials = collapsed or partials[:1]


async def _summarize_context(llm: Any, context: str) -> str:
    """Produce the report text for ONE community's rendered context.

    Fits the budget → a single LLM call (unchanged fast path).  Over budget
    → MAP each batch to a partial report, then REDUCE the partials.  Keeps
    every physical LLM call under the small model's context cap so the build
    never depends on the large-tier fallback to survive a huge community."""
    if len(context) <= _CONTEXT_CHAR_BUDGET:
        return await _summarize_once(llm, context)

    batches = _split_context_into_batches(context, _CONTEXT_CHAR_BUDGET)
    if len(batches) <= 1:
        return await _summarize_once(llm, batches[0] if batches else context)

    activity.logger.info(
        "summarize_community_activity  map-reduce  ctx_chars=%d  batches=%d",
        len(context), len(batches),
    )
    partials: list[dict] = []
    for batch in batches:
        rep = _parse_report(await _summarize_once(llm, batch))
        if rep.get("summary") or rep.get("title"):
            partials.append({
                "title": rep.get("title", ""),
                "summary": rep.get("summary", ""),
            })
    if not partials:
        return ""
    return await _reduce_partials(llm, partials)


@activity.defn
async def detect_communities_activity(
    params: DetectCommunitiesParams,
) -> DetectCommunitiesResult:
    """Run GDS Leiden detection + materialise ``:Community`` nodes.

    ``max_levels == 1`` (default) → single-level/back-compat
    (``detect_communities``); ``max_levels > 1`` → the full dendrogram
    HIERARCHY (``detect_hierarchy``).  After detection (either path) the
    native report vector index is ensured ONCE here — detect runs before
    any ``summarize_community_activity`` on the same ``kb-graph-build``
    queue, so the per-community ensure is redundant (fail-open)."""
    activity.heartbeat({
        "stage": "detect",
        "min_size": params.min_size,
        "max_levels": params.max_levels,
    })

    from src.workflow.heartbeat import heartbeat_every

    store = _get_store()
    async with heartbeat_every(30.0, {"stage": "detect"}):
        if params.max_levels > 1:
            from src.graph.communities import detect_hierarchy

            communities = await detect_hierarchy(
                store, max_levels=params.max_levels, min_size=params.min_size,
                gamma=params.gamma, concurrency=params.concurrency,
            )
        else:
            from src.graph.communities import detect_communities

            communities = await detect_communities(
                store, min_size=params.min_size, level=params.level,
                gamma=params.gamma, concurrency=params.concurrency,
            )

    # Ensure the report vector index ONCE (one-shot, before the summarize
    # fan-out reads/writes report_vec).  Fail-open — a missing index only
    # disables report-vector search (degrades to lexical), never the build.
    # This is now the SOLE ensure (the per-community net was removed), so a
    # failure here leaves the whole rebuild's vectors unindexed until the
    # next full build — log at ERROR so it's visible, not buried.
    if store is not None:
        try:
            from src.config import settings
            from src.graph.index import ensure_community_report_vector_index

            ok = await asyncio.to_thread(
                ensure_community_report_vector_index, store, settings.milvus.dim,
            )
            if not ok:
                activity.logger.error(
                    "detect_communities_activity  report vector index NOT "
                    "ensured — report-vector search will be DEGRADED to lexical "
                    "until the index is created on a later build",
                )
        except Exception as exc:
            activity.logger.error(
                "detect_communities_activity  report index ensure raised "
                "(search degraded) err=%s", exc,
            )

    activity.logger.info(
        "detect_communities_activity  detected=%d  min_size=%d  max_levels=%d",
        len(communities), params.min_size, params.max_levels,
    )
    # Return SLIM refs (counts only): membership is already persisted in
    # Neo4j by detect_* above, so the result stays O(num communities) and
    # never trips Temporal's payload size limit on large graphs.
    return DetectCommunitiesResult(communities=[
        DetectedCommunity(
            community_id=c.community_id,
            level=c.level,
            member_count=c.member_count,
            parent_id=c.parent_id,
            needs_report=c.needs_report,
        )
        for c in communities
    ])


async def _gather_context(store, params: SummarizeCommunityParams) -> str:
    """Build the LLM context body for ONE community.

    level 0  → from member entities/relations (``summ.read_member_context``).
    level>0  → from CHILD reports (``summ.read_child_reports``); falls back
               to member context when no child reports exist yet.
    """
    summ = build_community_summarize(store)

    if params.level > 0:
        try:
            child_rows = await asyncio.to_thread(
                summ.read_child_reports,
                community_id=params.community_id, level=params.level,
            )
            child_rows = list(child_rows or [])
        except Exception as exc:
            activity.logger.warning(
                "summarize_community_activity  cid=%s  child fetch err=%s",
                params.community_id, exc,
            )
            child_rows = []
        if child_rows:
            return _build_child_context(child_rows)
        # No child reports yet — fall back to member context below.

    try:
        rows = await asyncio.to_thread(
            summ.read_member_context,
            community_id=params.community_id, level=params.level,
        )
        rows = list(rows or [])
    except Exception as exc:
        activity.logger.warning(
            "summarize_community_activity  cid=%s  context fetch err=%s",
            params.community_id, exc,
        )
        rows = []
    return _build_member_context(rows)


async def _embed_report(title: str, summary: str) -> list[float] | None:
    """Embed ``title + "\\n" + summary`` once; ``None`` on any failure
    (fail-open — the report is still persisted, just without report_vec)."""
    text = (f"{title}\n{summary}").strip()
    if not text:
        return None
    try:
        embed_model = _get_embed_model()
        vec = await embed_model.aget_text_embedding(text)
        return list(vec) if vec else None
    except Exception as exc:
        activity.logger.warning(
            "summarize_community_activity  embed err=%s", exc,
        )
        return None


@activity.defn
async def summarize_community_activity(
    params: SummarizeCommunityParams,
) -> SummarizeCommunityResult:
    """Generate a STRUCTURED REPORT for ONE community via the small LLM,
    embed it, and persist report/title/summary/report_vec on the
    ``:Community`` node (idempotent).  Fail-safe — any error returns a
    non-persisted result (never raises through the Temporal boundary)."""
    activity.heartbeat({
        "stage": "summarize",
        "community_id": params.community_id,
        "level": params.level,
        "members": params.member_count,
    })
    if params.member_count <= 0:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    store = _get_store()
    if store is None:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    # 1. Gather context (members for level 0, child reports for level>0).
    context = await _gather_context(store, params)

    # 2. Generate the structured report (small tier) + tolerant parse.
    #    Fits the context budget → one call; over budget (huge root
    #    community) → map-reduce so no single call overflows the small model.
    try:
        llm = _get_summary_llm()
        text = await _summarize_context(llm, context)
    except Exception as exc:
        activity.logger.warning(
            "summarize_community_activity  cid=%s  llm err=%s",
            params.community_id, exc,
        )
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    report = _parse_report(text)
    title = report.get("title", "")
    summary = report.get("summary", "")
    if not summary:
        return SummarizeCommunityResult(
            community_id=params.community_id, summary="", persisted=False,
        )

    # 3. Embed title + summary (fail-open — None ⇒ persist without vec).
    #    The report vector index is ensured ONCE in detect_communities_activity
    #    (detect runs before any summarize on the same queue), so there's no
    #    per-community ensure here.
    report_vec = await _embed_report(title, summary)

    # 3b. Mirror the report vector through the CommunityReportVectorStore
    #     seam (fail-open — never blocks persistence below).  On the
    #     default neo4j/native backend this is a no-op (report_vec is
    #     persisted on the :Community node by summ.write_report below);
    #     under an opt-in Milvus backend this is the actual write.
    if report_vec is not None:
        try:
            report_store = build_community_report_vector_store(store)
            await asyncio.to_thread(report_store.upsert, [{
                "community_id": params.community_id,
                "level": params.level,
                "summary": summary,
                "embedding": report_vec,
            }])
        except Exception as exc:
            activity.logger.warning(
                "summarize_community_activity  cid=%s  report vec upsert err=%s",
                params.community_id, exc,
            )

    # 4. Persist the report (idempotent MERGE on id+level).
    persisted = False
    try:
        summ = build_community_summarize(store)
        await asyncio.to_thread(
            summ.write_report,
            community_id=params.community_id, level=params.level,
            report=json.dumps(report, ensure_ascii=False),
            title=title, summary=summary, report_vec=report_vec,
        )
        persisted = True
    except Exception as exc:
        activity.logger.warning(
            "summarize_community_activity  cid=%s  persist err=%s",
            params.community_id, exc,
        )

    activity.logger.info(
        "summarize_community_activity  cid=%s  level=%d  chars=%d  "
        "findings=%d  embedded=%s  persisted=%s",
        params.community_id, params.level, len(summary),
        len(report.get("findings", [])), report_vec is not None, persisted,
    )
    return SummarizeCommunityResult(
        community_id=params.community_id, summary=summary, persisted=persisted,
    )


__all__ = [
    "detect_communities_activity",
    "summarize_community_activity",
]

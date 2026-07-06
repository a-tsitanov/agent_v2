"""WikiSweepWorkflow — select dirty entities, (re)write each article."""
from __future__ import annotations

import asyncio
import enum
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.config import settings


class ArticleOutcome(str, enum.Enum):
    WRITTEN = "written"
    SKIPPED = "skipped"
    FAILED = "failed"


def _tally(outcomes) -> dict[str, int]:
    res = {"written": 0, "skipped_unchanged": 0, "failed": 0}
    for o in outcomes:
        if o == ArticleOutcome.WRITTEN:
            res["written"] += 1
        elif o == ArticleOutcome.SKIPPED:
            res["skipped_unchanged"] += 1
        elif o == ArticleOutcome.FAILED:
            res["failed"] += 1
    return res


@activity.defn
async def select_dirty_entities(limit: int) -> list[str]:
    from src.graph.store import build_neo4j_graph_store
    from src.graph.wiki_dirty import select_dirty
    # sync Neo4j — off the shared loop.
    return await asyncio.to_thread(select_dirty, build_neo4j_graph_store(), limit)


@activity.defn
async def write_entity_article(name: str) -> str:
    from src.graph.store import build_neo4j_graph_store
    from src.graph.wiki_context import (
        read_citations,
        read_entity_subgraph,
        read_source_docs,
        subgraph_hash,
    )
    from src.graph.wiki_dirty import clear_dirty
    from src.retrieval.llm_pool import get_llm_pool
    from src.workflow.wiki._deps import get_mediawiki
    from src.workflow.wiki.article import render_bot_section, splice_bot_section

    store = build_neo4j_graph_store()
    # All Neo4j reads/writes here use the sync driver — off the shared loop.
    ctx = await asyncio.to_thread(
        read_entity_subgraph, store, name, settings.wiki.max_relations)
    docs = await asyncio.to_thread(read_source_docs, store, name)
    h = subgraph_hash(ctx, docs)
    # change-detection: skip if the facts + source set are unchanged since
    # the last write.
    cur_hash_rows = await asyncio.to_thread(
        store.structured_query,
        "MATCH (e:__Entity__ {name:$n}) RETURN coalesce(e.wiki_hash,'') AS h",
        param_map={"n": name})
    if cur_hash_rows and cur_hash_rows[0]["h"] == h:
        await asyncio.to_thread(clear_dirty, store, name, h)
        return ArticleOutcome.SKIPPED.value

    cites = await asyncio.to_thread(
        read_citations, store, name, settings.wiki.citations_top_k)
    llm = get_llm_pool().get("synthesis")
    bot_md = await render_bot_section(
        ctx, cites, llm=llm, source_doc_ids=docs,
        docs_base_url=settings.wiki.docs_base_url)

    mw = await get_mediawiki()
    title = ctx.page_title
    current = await mw.get_page(title)
    new = splice_bot_section(current, bot_md)
    if new != current:
        await mw.upsert_page(title, new, summary="KB bot: updated from graph")
    try:
        await mw.ensure_sitelink(ctx.wikibase_qid, title)
    except Exception as exc:
        activity.logger.warning("ensure_sitelink failed name=%s: %s", name, exc)
    # persist page title + hash + clear dirty
    await asyncio.to_thread(
        store.structured_query,
        "MATCH (e:__Entity__ {name:$n}) SET e.wiki_page_title=$t",
        param_map={"n": name, "t": title})
    await asyncio.to_thread(clear_dirty, store, name, h)
    return ArticleOutcome.WRITTEN.value


_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2), maximum_attempts=5)


@workflow.defn
class WikiSweepWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        log = workflow.logger
        names = await workflow.execute_activity(
            select_dirty_entities, settings.wiki.sweep_batch,
            start_to_close_timeout=timedelta(minutes=2),
            schedule_to_close_timeout=timedelta(minutes=10), retry_policy=_RETRY)
        log.info("wiki sweep  dirty=%d", len(names))
        outcomes: list[str] = []
        for name in names:
            try:
                outcomes.append(await workflow.execute_activity(
                    write_entity_article, name,
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=5),
                    schedule_to_close_timeout=timedelta(hours=2),
                    retry_policy=_RETRY))
            except Exception as exc:
                log.warning("write_entity_article failed name=%s: %s", name, exc)
                outcomes.append(ArticleOutcome.FAILED.value)
        result = _tally([ArticleOutcome(o) if o else None for o in outcomes])
        log.info("wiki sweep done  %s", result)
        return result

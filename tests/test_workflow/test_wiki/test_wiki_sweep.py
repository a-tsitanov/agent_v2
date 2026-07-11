from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import settings
from src.graph.wiki_context import EntityContext
from src.workflow.wiki.wiki_sweep import ArticleOutcome, _tally, write_entity_article


def test_tally_counts_outcomes():
    res = _tally([
        ArticleOutcome.WRITTEN, ArticleOutcome.SKIPPED,
        ArticleOutcome.WRITTEN, ArticleOutcome.FAILED, None,
    ])
    assert res == {"written": 2, "skipped_unchanged": 1, "failed": 1}


@pytest.mark.asyncio
async def test_write_article_threads_sources_and_max_relations():
    ctx = EntityContext(
        name="X", label="Org", description="d", wikibase_qid="Q5",
        page_title="X", relations=[])

    store = MagicMock()

    # fake WikiGraphOps: change-detection read returns a hash that differs
    # from subgraph_hash -> CHANGED (not skipped).
    ops = MagicMock()
    ops.read_wiki_hash.return_value = "OLD_HASH"
    m_build_ops = MagicMock(return_value=ops)

    mw = AsyncMock()
    mw.get_page.return_value = "human content"

    llm_pool = MagicMock()
    llm_pool.get.return_value = MagicMock()

    with patch("src.graph.store.build_graph_store", return_value=store), \
         patch("src.graph.wiki_graph_ops.build_wiki_graph_ops", m_build_ops), \
         patch("src.graph.wiki_context.read_entity_subgraph",
               return_value=ctx) as m_subgraph, \
         patch("src.graph.wiki_context.read_source_docs",
               return_value=["d1"]) as m_docs, \
         patch("src.graph.wiki_context.subgraph_hash",
               return_value="NEW_HASH") as m_hash, \
         patch("src.graph.wiki_context.read_citations", return_value=[]), \
         patch("src.graph.wiki_dirty.clear_dirty"), \
         patch("src.retrieval.llm_pool.get_llm_pool", return_value=llm_pool), \
         patch("src.workflow.wiki._deps.get_mediawiki",
               new=AsyncMock(return_value=mw)), \
         patch("src.workflow.wiki.article.render_bot_section",
               new=AsyncMock(return_value="BOT MD")) as m_render:
        out = await write_entity_article("X")

    assert out == ArticleOutcome.WRITTEN.value

    # hash-check + title-write route through the WikiGraphOps seam.
    m_build_ops.assert_called_once_with(store)
    ops.read_wiki_hash.assert_called_once_with("X")
    ops.write_page_title.assert_called_once_with("X", ctx.page_title)

    # max_relations threaded into read_entity_subgraph (positional or kwarg).
    args, kwargs = m_subgraph.call_args
    passed = kwargs.get("max_relations", args[2] if len(args) > 2 else None)
    assert passed == settings.wiki.max_relations

    # read_source_docs called for the entity.
    m_docs.assert_called_once_with(store, "X")

    # the doc list is folded into subgraph_hash.
    h_args, h_kwargs = m_hash.call_args
    h_docs = h_kwargs.get("source_doc_ids", h_args[1] if len(h_args) > 1 else None)
    assert h_docs == ["d1"]

    # the doc list + base url reach render_bot_section.
    r_kwargs = m_render.call_args.kwargs
    assert r_kwargs["source_doc_ids"] == ["d1"]
    assert r_kwargs["docs_base_url"] == settings.wiki.docs_base_url


@pytest.mark.asyncio
async def test_write_article_skips_when_seam_hash_matches():
    ctx = EntityContext(
        name="X", label="Org", description="d", wikibase_qid="Q5",
        page_title="X", relations=[])

    store = MagicMock()

    # fake WikiGraphOps: change-detection read returns the SAME hash as the
    # freshly computed subgraph_hash -> SKIPPED (no MediaWiki call).
    ops = MagicMock()
    ops.read_wiki_hash.return_value = "SAME_HASH"
    m_build_ops = MagicMock(return_value=ops)

    with patch("src.graph.store.build_graph_store", return_value=store), \
         patch("src.graph.wiki_graph_ops.build_wiki_graph_ops", m_build_ops), \
         patch("src.graph.wiki_context.read_entity_subgraph",
               return_value=ctx), \
         patch("src.graph.wiki_context.read_source_docs", return_value=["d1"]), \
         patch("src.graph.wiki_context.subgraph_hash",
               return_value="SAME_HASH"), \
         patch("src.graph.wiki_dirty.clear_dirty") as m_clear, \
         patch("src.workflow.wiki._deps.get_mediawiki") as m_get_mw:
        out = await write_entity_article("X")

    assert out == ArticleOutcome.SKIPPED.value
    m_build_ops.assert_called_once_with(store)
    ops.read_wiki_hash.assert_called_once_with("X")
    ops.write_page_title.assert_not_called()
    m_clear.assert_called_once_with(store, "X", "SAME_HASH")
    m_get_mw.assert_not_called()

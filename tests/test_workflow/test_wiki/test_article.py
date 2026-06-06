import inspect

import pytest
from unittest.mock import AsyncMock

from src.graph.wiki_context import EntityContext
from src.workflow.wiki.article import splice_bot_section, BOT_START, BOT_END, render_bot_section


def test_insert_into_pageless_creates_marked_section():
    out = splice_bot_section("", "Hello bot.")
    assert BOT_START in out and BOT_END in out and "Hello bot." in out


def test_preserves_human_content_outside_markers():
    page = "Human intro.\n" + BOT_START + "\nold bot\n" + BOT_END + "\nHuman outro."
    out = splice_bot_section(page, "new bot")
    assert "Human intro." in out and "Human outro." in out
    assert "new bot" in out and "old bot" not in out


def test_idempotent_twice_equals_once():
    once = splice_bot_section("Human.\n", "B")
    twice = splice_bot_section(once, "B")
    assert once == twice


def test_human_only_page_gets_bot_section_prepended_without_loss():
    out = splice_bot_section("Just human text.", "B")
    assert "Just human text." in out and "B" in out and BOT_START in out


def _ctx():
    return EntityContext(
        name="ООО Альфа", label="Organization", description="A supplier.",
        wikibase_qid="Q5", page_title="ООО Альфа",
        relations=[("заключила договор", "out", "Договор № 17-К", "Document", "signed")],
    )


@pytest.mark.asyncio
async def test_render_grounds_on_facts_and_citations_not_prior_prose():
    captured = {}

    async def fake_acomplete(prompt, *a, **kw):
        captured["prompt"] = str(prompt)
        return "== ООО Альфа ==\nFacts [d1].\n[[Договор № 17-К]]"

    llm = AsyncMock()
    llm.acomplete = fake_acomplete
    cites = [("ООО Альфа заключила договор…", "d1")]
    out = await render_bot_section(_ctx(), cites, llm=llm)

    p = captured["prompt"]
    # Facts + citation snippet present in the prompt:
    assert "Договор № 17-К" in p and "d1" in p and "ООО Альфа заключила" in p
    # The neighbor wiki-link + citation appear in the rendered output:
    assert "[[Договор № 17-К]]" in out and "[d1]" in out
    # Anti-drift: there is no "prior article" channel — the function takes
    # no existing-prose argument (enforced by signature).
    params = set(inspect.signature(render_bot_section).parameters)
    assert "existing" not in params and "prior" not in params

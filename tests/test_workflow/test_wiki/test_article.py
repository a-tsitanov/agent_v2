from src.workflow.wiki.article import splice_bot_section, BOT_START, BOT_END


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

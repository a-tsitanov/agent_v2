"""Reply rendering.

Telegram rejects both an empty message and one over 4096 characters, so
every formatter owes a non-empty, bounded string for any input.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.bot.format import (
    TG_LIMIT,
    format_answer,
    format_channels,
    format_entities,
    format_fragments,
    format_history,
    format_timeline,
    format_users,
    split_for_telegram,
)

ALL = (
    lambda: format_channels([]),
    lambda: format_timeline([]),
    lambda: format_history([]),
    lambda: format_answer("", []),
    lambda: format_fragments([]),
    lambda: format_users([]),
    lambda: format_entities({}),
)


def test_every_formatter_says_something_for_empty_input():
    """An empty string is not a sendable Telegram message."""
    for make in ALL:
        assert make().strip()


# ── channels ─────────────────────────────────────────────────────────


def test_channels_are_ordered_by_volume():
    out = format_channels([
        {"key": "small", "total": 5, "completed": 5},
        {"key": "big", "total": 900, "completed": 800},
    ])
    assert out.index("big") < out.index("small")


def test_channels_say_how_many_were_hidden():
    """A silently truncated list reads as the whole list."""
    rows = [{"key": f"c{i}", "total": 100 - i, "completed": 1} for i in range(40)]
    out = format_channels(rows)
    assert "ещё 25" in out
    assert len(out) < TG_LIMIT


def test_channels_report_the_total_over_all_of_them_not_the_shown_ones():
    rows = [{"key": f"c{i}", "total": 10, "completed": 10} for i in range(40)]
    assert "400" in format_channels(rows)


# ── timeline ─────────────────────────────────────────────────────────


def test_timeline_keeps_the_most_recent_days_and_stays_within_the_cap():
    buckets = [{"day": f"2026-{m:02d}-{d:02d}", "count": d} for m in range(1, 13)
               for d in range(1, 29)]
    out = format_timeline(buckets)
    assert len(out) < TG_LIMIT
    assert "2026-12-28" in out          # newest kept
    assert "2026-01-01" not in out      # oldest dropped
    assert f"из {len(buckets)}" in out  # and it says so


def test_timeline_names_the_channel_when_filtered():
    out = format_timeline([{"day": "2026-08-01", "count": 3}], channel="tass")
    assert "tass" in out


# ── history ──────────────────────────────────────────────────────────


def test_history_shows_id_command_status_and_the_question():
    rows = [{
        "id": 7, "command": "/ask", "status": "done",
        "started_at": datetime(2026, 8, 17, 5, 30, tzinfo=UTC),
        "args": "что писали про урожай",
    }]
    out = format_history(rows)
    assert "#7" in out and "/ask" in out and "done" in out
    assert "урожай" in out
    assert "/repeat" in out


def test_history_trims_a_long_question():
    rows = [{"id": 1, "command": "/ask", "status": "done",
             "started_at": None, "args": "х" * 500}]
    assert len(format_history(rows)) < 400


# ── answer ───────────────────────────────────────────────────────────


def test_answer_appends_sources():
    out = format_answer("ответ", [{"metadata": {"file_name": "tass_1.txt"}}])
    assert "ответ" in out
    assert "tass_1.txt" in out
    assert "Источники (1)" in out


def test_answer_without_sources_has_no_empty_section():
    out = format_answer("ответ", [])
    assert out == "ответ"
    assert "Источники" not in out


def test_answer_distinguishes_no_synthesis_from_no_hits():
    """An empty synthesis WITH sources is a different failure from
    finding nothing, and the user should be able to tell."""
    assert format_answer("", []) != format_answer("", [{"chunk_id": "c1"}])


def test_answer_deduplicates_source_names_and_counts_the_rest():
    sources = [{"metadata": {"file_name": "a.txt"}} for _ in range(9)]
    out = format_answer("ответ", sources)
    assert out.count("a.txt") == 1
    assert "Источники (9)" in out


# ── fragments ────────────────────────────────────────────────────────


def test_fragments_stay_within_the_cap_and_say_how_many_are_shown():
    sources = [{"text": "ю" * 400} for _ in range(60)]
    out = format_fragments(sources)
    assert len(out) < TG_LIMIT
    assert "показаны первые" in out


def test_fragments_report_the_full_count():
    assert "3" in format_fragments([{"text": "a"}, {"text": "b"}, {"text": "c"}])


# ── users ────────────────────────────────────────────────────────────


def test_users_list_shows_status_and_the_admin_commands():
    out = format_users([
        {"telegram_id": 1, "status": "pending", "role": "client", "username": "vasya"},
    ])
    assert "pending" in out and "vasya" in out
    assert "/approve" in out and "/deny" in out


# ── splitting ────────────────────────────────────────────────────────


def test_split_never_yields_an_empty_message():
    assert split_for_telegram("") == ["(пустой ответ)"]
    assert split_for_telegram(None) == ["(пустой ответ)"]


def test_split_chunks_stay_under_the_limit():
    chunks = split_for_telegram("я" * 9500)
    assert len(chunks) == 3
    assert all(len(c) <= TG_LIMIT for c in chunks)
    assert "".join(chunks) == "я" * 9500


# ── the shape the API actually returns ───────────────────────────────
#
# Checked live on 2026-08-17, after the first deploy rendered "1. 2. 3."
# — ten fragments with no text in them. The search API's source is FLAT
# and its text field is `content`:
#
#   {chunk_id, content, doc_id, position, score, department, doc_type}
#
# There is no `metadata` dict. MCP's vector_search uses `text` instead,
# so both are accepted.

API_SOURCE = {
    "chunk_id": "17f59b5f-c60a-494a-916d-34b481037598",
    "doc_id": "ff48aba5-572c-4717-99f4-534fe61f2c55",
    "content": "Зерно российских аграриев оказалось никому не нужно",
    "position": 0,
    "score": 0.64,
    "department": "",
    "doc_type": "",
}


def test_fragments_render_the_api_content_field():
    out = format_fragments([API_SOURCE])
    assert "Зерно российских аграриев" in out


def test_fragments_still_render_the_mcp_text_field():
    assert "фрагмент" in format_fragments([{"text": "фрагмент"}])


def test_answer_labels_an_api_source_by_its_document():
    out = format_answer("ответ", [API_SOURCE])
    assert "ff48aba5" in out


def test_answer_falls_back_to_chunk_id_when_there_is_no_doc():
    out = format_answer("ответ", [{"chunk_id": "c-1"}])
    assert "c-1" in out


# ── the empty-synthesis marker ───────────────────────────────────────


def test_empty_response_marker_is_never_shown_to_the_user():
    """LlamaIndex returns the literal `Empty Response` when synthesis has
    no nodes. The first deploy relayed it verbatim as the answer AND
    recorded the request as done."""
    out = format_answer("Empty Response", [])
    assert "Empty Response" not in out
    assert "ничего не нашлось" in out


def test_empty_synthesis_with_sources_says_so_and_keeps_them():
    """Different failure from finding nothing: there ARE sources, the
    synthesis just produced no prose."""
    out = format_answer("Empty Response", [API_SOURCE])
    assert "Empty Response" not in out
    assert "не сформирован" in out
    assert "ff48aba5" in out


# ── entities ─────────────────────────────────────────────────────────


def test_entities_render_name_type_and_description():
    body = {"entities": [
        {"entity_name": "Украина", "entity_type": "Country", "description": "государство"},
    ]}
    out = format_entities(body, query="Украина")
    assert "Украина" in out and "Country" in out and "государство" in out


def test_entities_empty_explains_the_prefix_rule():
    """So the user knows to try another spelling rather than concluding
    the entity is absent."""
    out = format_entities({"entities": []}, query="Ромаш")
    assert "Ромаш" in out
    assert "НАЧАЛУ" in out


def test_entities_error_is_not_reported_as_absence():
    """An empty list WITH an error means the lookup could not run. Saying
    "not found" there is the exact defect this command was built after."""
    out = format_entities(
        {"entities": [], "error": "GraphMemoryExceeded: (-2600)"}, query="Украина",
    )
    assert "сбой" in out
    assert "GraphMemoryExceeded" in out
    assert "не найдено" not in out


def test_entities_bound_the_list_and_say_how_many_were_hidden():
    body = {"entities": [
        {"entity_name": f"E{i}", "entity_type": "X", "description": "d"}
        for i in range(30)
    ]}
    out = format_entities(body)
    assert len(out) < TG_LIMIT
    assert "ещё 20" in out

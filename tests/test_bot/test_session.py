"""Session window logic + in-memory session store (TDD)."""
from __future__ import annotations

from src.bot.session import InMemorySessionStore, Turn, trim_turns


def test_trim_keeps_last_n_messages():
    turns = [Turn(role="user", text=f"m{i}") for i in range(10)]
    out = trim_turns(turns, max_messages=4)
    assert [t.text for t in out] == ["m6", "m7", "m8", "m9"]


def test_trim_shorter_than_cap_is_unchanged():
    turns = [Turn(role="user", text="a"), Turn(role="assistant", text="b")]
    assert trim_turns(turns, max_messages=8) == turns


def test_store_append_and_load_in_order():
    store = InMemorySessionStore(max_messages=8)
    store.append(42, Turn(role="user", text="привет"))
    store.append(42, Turn(role="assistant", text="здравствуй"))
    assert store.load(42) == [
        Turn(role="user", text="привет"),
        Turn(role="assistant", text="здравствуй"),
    ]


def test_store_trims_to_max_messages():
    store = InMemorySessionStore(max_messages=3)
    for i in range(5):
        store.append(7, Turn(role="user", text=f"m{i}"))
    assert [t.text for t in store.load(7)] == ["m2", "m3", "m4"]


def test_store_isolates_chats():
    store = InMemorySessionStore(max_messages=8)
    store.append(1, Turn(role="user", text="a"))
    store.append(2, Turn(role="user", text="b"))
    assert [t.text for t in store.load(1)] == ["a"]
    assert [t.text for t in store.load(2)] == ["b"]


def test_store_unknown_chat_is_empty():
    assert InMemorySessionStore(max_messages=8).load(999) == []

"""Conversation session state for the Telegram Q&A bot.

A session is a rolling window of the last N messages per chat. The window
trimming is a pure function (``trim_turns``); the store wraps it with I/O.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Turn:
    role: str  # "user" | "assistant"
    text: str


def trim_turns(turns: list[Turn], *, max_messages: int) -> list[Turn]:
    """Keep only the last ``max_messages`` turns (oldest dropped first)."""
    if max_messages <= 0:
        return []
    return turns[-max_messages:]


class InMemorySessionStore:
    """Per-chat rolling window of turns, held in process memory.

    Sufficient for a single-instance bot (one process owns all chats). A
    Redis-backed store can drop in behind the same load/append interface for
    persistence across restarts or multi-replica scale-out.
    """

    def __init__(self, *, max_messages: int) -> None:
        self._max_messages = max_messages
        self._chats: dict[int, list[Turn]] = {}

    def load(self, chat_id: int) -> list[Turn]:
        return list(self._chats.get(chat_id, []))

    def append(self, chat_id: int, turn: Turn) -> None:
        turns = [*self._chats.get(chat_id, []), turn]
        self._chats[chat_id] = trim_turns(turns, max_messages=self._max_messages)

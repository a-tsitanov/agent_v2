"""Intent classification (TDD): analytical (graph aggregation) vs search."""
from __future__ import annotations

import pytest

from src.bot.intent import ANALYTICAL, SEARCH, classify_intent


@pytest.mark.parametrize("q", [
    "сколько сущностей по типам?",
    "покажи распределение по типам",
    "какие всплески событий за неделю?",
    "топ сущностей по связям",
    "динамика по дням",
    "статистика графа",
    "какие противоречия по SOCAR?",
    "у кого больше всего связей",
])
def test_analytical_questions(q):
    assert classify_intent(q) == ANALYTICAL


@pytest.mark.parametrize("q", [
    "что в базе про Украину?",
    "расскажи про санкции",
    "кто такой Юрий Игнат",
    "что известно про удар по SOCAR",
])
def test_search_questions(q):
    assert classify_intent(q) == SEARCH


def test_blank_defaults_to_search():
    assert classify_intent("") == SEARCH
    assert classify_intent("   ") == SEARCH

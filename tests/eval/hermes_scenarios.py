"""Golden interactive scenarios for the Hermes ↔ kb-llamaindex skill.

Each scenario names a user turn, the tool the skill should select, and
the response template that should be applied. The coverage test asserts
the set exercises every decision-tree branch (one per atomic tool, the
kb_search escape hatch, a multi-turn follow-up, and an entity dossier).

End-to-end execution is manual against a live Hermes (see
docs/runbook/hermes.md §5); this file is the versioned source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    name: str
    user_turn: str
    history: tuple[str, ...]      # prior turns; empty for single-shot
    expected_tool: str            # bare tool name the skill should pick
    expected_template: str        # factual | dossier | what_we_know


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        name="exact_identifier",
        user_turn="Чей это номер +7 495 123-45-67?",
        history=(),
        expected_tool="find_entity_by_id",
        expected_template="dossier",
    ),
    Scenario(
        name="relationship_walk",
        user_turn="Кто связан с ООО «Ромашка»?",  # noqa: RUF001
        history=(),
        expected_tool="find_neighbours",
        expected_template="what_we_know",
    ),
    Scenario(
        name="graph_unpinned_entity",
        user_turn="Что известно про связи поставщика из последнего договора?",
        history=(),
        expected_tool="graph_search",
        expected_template="what_we_know",
    ),
    Scenario(
        name="semantic_factual",
        user_turn="Какой порядок согласования отпуска?",
        history=(),
        expected_tool="vector_search",
        expected_template="factual",
    ),
    Scenario(
        name="surrounding_context",
        user_turn="Покажи раздел целиком, откуда это.",
        history=("Какой порядок согласования отпуска?",),
        expected_tool="get_chunks_by_doc_id",
        expected_template="factual",
    ),
    Scenario(
        name="full_document",
        user_turn="Дай полный текст приказа №14, там таблица.",
        history=(),
        expected_tool="read_full_document",
        expected_template="factual",
    ),
    Scenario(
        name="hard_multihop_escalation",
        user_turn="Сравни условия трёх договоров с этим контрагентом и найди расхождения.",  # noqa: RUF001
        history=(),
        expected_tool="kb_search",
        expected_template="factual",
    ),
    Scenario(
        name="entity_dossier",
        user_turn="Расскажи всё, что у нас есть про Иванова И.И.",  # noqa: RUF001
        history=(),
        expected_tool="find_neighbours",
        expected_template="dossier",
    ),
    Scenario(
        name="multiturn_followup",
        user_turn="А его телефон?",  # noqa: RUF001
        history=(
            "Расскажи всё, что у нас есть про Иванова И.И.",  # noqa: RUF001
            "Иванов Иван Иванович, менеджер, …",
        ),
        expected_tool="find_entity_by_id",
        expected_template="factual",
    ),
)

# Every tool branch the skill documents must be exercised by ≥1 scenario.
_REQUIRED_TOOL_COVERAGE = {
    "vector_search",
    "graph_search",
    "find_entity_by_id",
    "find_neighbours",
    "get_chunks_by_doc_id",
    "read_full_document",
    "kb_search",
}


def test_scenarios_cover_every_tool_branch():
    covered = {s.expected_tool for s in SCENARIOS}
    missing = _REQUIRED_TOOL_COVERAGE - covered
    assert not missing, f"no scenario exercises: {missing}"


def test_at_least_one_multiturn_scenario():
    assert any(s.history for s in SCENARIOS), "need a follow-up scenario"


def test_templates_are_known():
    allowed = {"factual", "dossier", "what_we_know"}
    bad = {s.expected_template for s in SCENARIOS} - allowed
    assert not bad, f"unknown templates: {bad}"

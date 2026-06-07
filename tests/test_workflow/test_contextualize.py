from src.workflow.contracts import ConversationTurnDict
from src.workflow.search.activities.contextualize import _bound_history, _build_prompt


def _t(role, content): return ConversationTurnDict(role=role, content=content)


def test_bound_history_caps_turns():
    turns = [_t("user", f"q{i}") for i in range(20)]
    out = _bound_history(turns, max_turns=6, max_chars=10_000)
    assert len(out) == 6 and out[-1].content == "q19"   # keeps the most recent


def test_bound_history_caps_chars():
    turns = [_t("user", "x" * 100) for _ in range(20)]
    out = _bound_history(turns, max_turns=40, max_chars=250)
    assert sum(len(t.content) for t in out) <= 250 and out[-1].content == "x" * 100


def test_build_prompt_includes_history_and_query():
    p = _build_prompt("а что по цене?", [_t("user", "расскажи про Продукт X")])
    assert "Продукт X" in p and "а что по цене?" in p

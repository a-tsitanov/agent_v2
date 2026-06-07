def test_search_request_accepts_history():
    from src.models.search import SearchRequest, ConversationTurn
    req = SearchRequest(query="q", history=[ConversationTurn(role="user", content="prev")])
    assert req.history[0].content == "prev"
    assert SearchRequest(query="q").history == []   # default empty


def test_contracts_have_history_and_contextualize():
    from src.workflow.contracts import (
        OrchestratorParams, GlobalSearchParams,
        ContextualizeParams, ContextualizeResult, ConversationTurnDict,
    )
    assert OrchestratorParams(query="q").history == []
    assert GlobalSearchParams(query="q").history == []
    cp = ContextualizeParams(query="q", history=[ConversationTurnDict(role="user", content="h")])
    assert cp.history[0].content == "h"
    assert ContextualizeResult(query="x").query == "x"


def test_config_history_knobs():
    from src.config import settings
    assert settings.agent.conversation_history_enabled in (True, False)
    assert settings.agent.history_max_turns == 6

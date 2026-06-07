"""Route → params history threading (Phase 3 wiring).

Pure tests: the request's conversation history must reach BOTH the local
``OrchestratorParams`` and the global ``GlobalSearchParams`` unchanged,
while the user query itself is passed through verbatim (contextualisation
happens later, inside the workflow — not in the route).
"""

from src.api.routes.search_v2 import _global_params, _local_params
from src.models.search import ConversationTurn, SearchRequest


def test_local_params_carries_history():
    req = SearchRequest(
        query="а что по цене?",
        history=[ConversationTurn(role="user", content="про Продукт X")],
    )
    p = _local_params(req)
    assert p.query == "а что по цене?"
    assert [t.content for t in p.history] == ["про Продукт X"]
    assert [t.role for t in p.history] == ["user"]


def test_global_params_carries_history():
    req = SearchRequest(
        query="а что по цене?",
        history=[ConversationTurn(role="user", content="про Продукт X")],
    )
    p = _global_params(req)
    assert p.query == "а что по цене?"
    assert [t.content for t in p.history] == ["про Продукт X"]


def test_local_params_empty_history_default():
    """Back-compat: no history on the request ⇒ empty history on params."""
    p = _local_params(SearchRequest(query="hi"))
    assert list(p.history) == []

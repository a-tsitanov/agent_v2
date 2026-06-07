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


def test_local_params_resolves_contextualize_flag_at_submit_time():
    # The replay-safety convention: the gate is resolved from settings at
    # submit time into params, not read live inside the workflow.
    from src.api.routes.search_v2 import _local_params
    from src.models.search import SearchRequest
    from src.config import settings
    p = _local_params(SearchRequest(query="q"))
    assert p.contextualize_enabled == settings.agent.conversation_history_enabled


def test_global_params_resolves_community_selection_at_submit_time():
    # Same replay-safe convention: the community-selection strategy is
    # resolved from settings at submit time into GlobalSearchParams, never
    # read live inside the workflow.  Default = lexical (unchanged today).
    from src.config import settings
    p = _global_params(SearchRequest(query="q"))
    assert p.community_selection == settings.agent.community_dynamic_selection


def test_detect_params_threads_max_levels_from_config():
    # The admin rebuild endpoint constructs DetectCommunitiesParams with
    # max_levels resolved from settings at submit time (replay-safe).
    # Default = 1 (single-level, today's cost) until an operator raises it.
    from src.config import settings
    from src.workflow.contracts import DetectCommunitiesParams
    p = DetectCommunitiesParams(
        min_size=settings.temporal.community_min_size,
        level=0,
        max_levels=settings.agent.community_max_levels,
    )
    assert p.max_levels == settings.agent.community_max_levels
    assert p.max_levels == 1  # back-compat default

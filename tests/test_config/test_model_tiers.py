from src.config import LiteLLMSettings


def test_roles_resolve_to_two_physical_models():
    cfg = LiteLLMSettings(model_small="gemma4:e4b", model_large="gpt-4o-mini")
    assert cfg.model_for("extraction") == "gemma4:e4b"
    assert cfg.model_for("judge") == "gemma4:e4b"
    assert cfg.model_for("search") == "gemma4:e4b"
    assert cfg.model_for("plan") == "gemma4:e4b"
    assert cfg.model_for("synthesis") == "gpt-4o-mini"


def test_role_tier_override_changes_resolution():
    cfg = LiteLLMSettings(
        model_small="gemma4:e4b", model_large="gpt-4o-mini",
        role_tiers={"plan": "large"},
    )
    assert cfg.model_for("plan") == "gpt-4o-mini"
    assert cfg.model_for("synthesis") == "gpt-4o-mini"

from src.config import settings


def test_analytics_layer_settings_defaults():
    assert settings.analytics.default_top_n == 20
    assert settings.analytics.max_steps == 3
    assert settings.analytics.cypher_fallback_enabled is False  # ships OFF


def test_events_settings_defaults():
    assert settings.events.first_seen_enabled is False  # OFF until backfill run
    assert settings.events.new_window_days == 14
    assert settings.events.backfill_sentinel == 0


def test_signals_settings_defaults():
    assert settings.signals.orphan_min_degree == 1
    assert "Organization" in settings.signals.expected_attrs

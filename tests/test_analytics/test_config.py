from src.config import settings


def test_analytics_layer_settings_defaults():
    assert settings.analytics.default_top_n == 20
    assert settings.analytics.max_steps == 3
    assert settings.analytics.cypher_fallback_enabled is False  # ships OFF


def test_events_settings_defaults():
    # CODE default is OFF (until backfill run). Check the field default,
    # not the resolved instance — EventsSettings loads `.env`, and a dev
    # machine with EVENTS_FIRST_SEEN_ENABLED=true would flake this test.
    from src.config import EventsSettings

    assert EventsSettings.model_fields["first_seen_enabled"].default is False
    assert settings.events.new_window_days == 14
    assert settings.events.backfill_sentinel == 0


def test_signals_settings_defaults():
    assert settings.signals.orphan_min_degree == 1
    assert "Organization" in settings.signals.expected_attrs

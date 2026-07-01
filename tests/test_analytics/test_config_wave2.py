"""Wave 2 config tests: EventsSettings (extraction) + MonitorSettings."""

from src.config import settings


def test_events_extraction_defaults_off():
    assert settings.events.extraction_enabled is False
    assert "deal" in settings.events.taxonomy or len(settings.events.taxonomy) >= 1


def test_monitor_settings_defaults():
    m = settings.monitor
    assert m.enabled is False and m.task_queue == "kb-monitor"
    assert m.sweep_interval_minutes >= 1 and 0.0 < m.risk_rise_delta <= 1.0


def test_monitor_wave3_defaults():
    m = settings.monitor
    assert m.burst_enabled is False
    assert m.burst_window_days >= 1 and m.burst_baseline_windows >= 1
    assert m.burst_ratio > 1.0 and m.burst_min_count >= 1
    assert m.webhook_url == "" and m.webhook_timeout_s > 0 and m.deliver_batch >= 1

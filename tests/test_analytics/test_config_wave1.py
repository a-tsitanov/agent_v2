from src.config import settings


def test_materialize_and_risk_settings_defaults():
    assert settings.temporal.analytics_materialize_concurrency >= 1
    w = settings.signals.risk_weights
    assert set(w) == {"affiliation", "brokerage", "controversy", "volatility", "opacity"}
    assert abs(sum(w.values()) - 1.0) < 1e-9  # weights normalized
    assert settings.signals.risk_bands["high"] >= settings.signals.risk_bands["medium"]
    assert settings.signals.link_prediction_top_k >= 1

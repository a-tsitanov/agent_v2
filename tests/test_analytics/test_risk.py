from src.analytics.risk import RiskResult, compute_risk, normalize

_W = {"affiliation": 0.3, "brokerage": 0.2, "controversy": 0.2, "volatility": 0.15, "opacity": 0.15}
_B = {"high": 0.66, "medium": 0.33}


def test_normalize():
    assert normalize(5, 0, 10) == 0.5
    assert normalize(-1, 0, 10) == 0.0 and normalize(99, 0, 10) == 1.0
    assert normalize(5, 10, 10) == 0.0  # degenerate range → 0


def test_compute_risk_weighted_and_banded():
    r = compute_risk({"affiliation": 1.0, "brokerage": 1.0}, weights=_W, bands=_B)
    assert isinstance(r, RiskResult)
    assert abs(r.score - 0.5) < 1e-9  # 0.3*1 + 0.2*1 = 0.5
    assert r.band == "medium"  # 0.5 >= 0.33, < 0.66
    assert set(r.fired) == {"affiliation", "brokerage"}


def test_bands_low_and_high():
    assert compute_risk({}, weights=_W, bands=_B).band == "low"
    assert compute_risk({k: 1.0 for k in _W}, weights=_W, bands=_B).band == "high"  # sum=1.0


def test_unknown_component_ignored():
    r = compute_risk({"affiliation": 1.0, "bogus": 1.0}, weights=_W, bands=_B)
    assert abs(r.score - 0.3) < 1e-9  # bogus has no weight → ignored

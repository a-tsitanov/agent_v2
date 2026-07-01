"""Pure composite risk scoring (no I/O). Components arrive already normalized to 0..1."""

from __future__ import annotations

from dataclasses import dataclass, field


def normalize(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (float(value) - lo) / (hi - lo)))


@dataclass(frozen=True)
class RiskResult:
    score: float
    band: str
    fired: dict[str, float] = field(default_factory=dict)


def compute_risk(
    components: dict[str, float],
    *,
    weights: dict[str, float],
    bands: dict[str, float],
) -> RiskResult:
    score = 0.0
    fired: dict[str, float] = {}
    for name, w in weights.items():
        v = float(components.get(name, 0.0) or 0.0)
        score += w * v
        if v > 0:
            fired[name] = v
    score = max(0.0, min(1.0, score))
    if score >= bands.get("high", 0.66):
        band = "high"
    elif score >= bands.get("medium", 0.33):
        band = "medium"
    else:
        band = "low"
    return RiskResult(score=round(score, 6), band=band, fired=fired)

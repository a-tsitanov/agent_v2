"""Pure contradiction detection over claims (TDD, no I/O)."""
from __future__ import annotations

from src.analytics.contradictions import NEGATED, Claim, detect_contradictions


def test_agreement_yields_no_contradiction():
    claims = [
        Claim(subject="удар по SOCAR", attribute="число_ударов", value="дважды", source="A"),
        Claim(subject="удар по SOCAR", attribute="число_ударов", value="дважды", source="B"),
    ]
    assert detect_contradictions(claims) == []


def test_value_conflict_detected_with_consensus_by_source_count():
    claims = [
        Claim(subject="удар", attribute="погибло", value="5", source="A"),
        Claim(subject="удар", attribute="погибло", value="5", source="B"),
        Claim(subject="удар", attribute="погибло", value="8", source="C"),
    ]
    out = detect_contradictions(claims)
    assert len(out) == 1
    c = out[0]
    assert (c.subject, c.attribute) == ("удар", "погибло")
    assert c.disputed is True
    assert c.consensus == "5"  # 2 distinct sources vs 1
    # versions ranked by distinct-source count desc
    assert c.versions[0].value == "5" and c.versions[0].count == 2
    assert set(c.versions[0].sources) == {"A", "B"}
    assert c.versions[1].value == "8" and c.versions[1].count == 1


def test_tie_has_no_single_consensus():
    claims = [
        Claim(subject="x", attribute="a", value="v1", source="A"),
        Claim(subject="x", attribute="a", value="v2", source="B"),
    ]
    out = detect_contradictions(claims)
    assert len(out) == 1
    assert out[0].disputed is True
    assert out[0].consensus is None  # 1 vs 1 — no majority


def test_polarity_conflict_on_same_value_is_a_contradiction():
    # A says Иванов participated; B says he did NOT — same value, opposite polarity.
    claims = [
        Claim(subject="Иванов", attribute="участие", value="в сделке", source="A"),
        Claim(subject="Иванов", attribute="участие", value="в сделке",
              polarity=NEGATED, source="B"),
    ]
    out = detect_contradictions(claims)
    assert len(out) == 1
    assert out[0].disputed is True
    assert out[0].polarity_split == ("в сделке",)


def test_asserted_only_same_value_no_polarity_conflict():
    claims = [
        Claim(subject="Иванов", attribute="участие", value="в сделке", source="A"),
        Claim(subject="Иванов", attribute="участие", value="в сделке", source="B"),
    ]
    assert detect_contradictions(claims) == []

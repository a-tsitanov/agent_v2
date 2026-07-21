# tests/test_analytics/test_entity_quality_gate.py
from src.analytics.ids import is_meaningful_entity


def test_drops_identifier_types():
    assert not is_meaningful_entity("+7 900 000", "PhoneNumber")
    assert not is_meaningful_entity("822", "Amount")
    assert is_meaningful_entity("822", "Amount", exclude_identifiers=False)  # opt-in override


def test_drops_degenerate_names():
    assert not is_meaningful_entity("60%", "Metric")
    assert not is_meaningful_entity("7,2 трлн", "Metric")
    assert not is_meaningful_entity("Concept", "Concept")   # name == type
    assert not is_meaningful_entity("X", "Person")          # single char
    assert not is_meaningful_entity("", "Organization")
    assert not is_meaningful_entity(None, "Organization")


def test_keeps_real_entities():
    assert is_meaningful_entity("BAE Systems", "Organization")
    assert is_meaningful_entity("Вячеслав Володин", "Person")
    assert is_meaningful_entity("Дагестан", "Location")
    assert is_meaningful_entity("Anti-Access And Area-Denial", "Concept")


def test_drops_url_named_entities():
    assert not is_meaningful_entity("https://kod.ru/niva", "Concept")
    assert not is_meaningful_entity("http://x.io", "Document")
    assert not is_meaningful_entity("www.example.com/page", "Topic")
    # opt-out still surfaces raw identifier-ish rows, including URLs
    assert is_meaningful_entity("https://kod.ru/niva", "Concept", exclude_identifiers=False)


def test_keeps_dotted_real_names_not_mistaken_for_urls():
    assert is_meaningful_entity("U.S.A.", "Location")
    assert is_meaningful_entity("BAE Systems", "Organization")
    assert is_meaningful_entity("Anti-Access And Area-Denial", "Concept")

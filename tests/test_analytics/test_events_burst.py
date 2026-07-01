from src.analytics.events_burst import build_burst_cypher


def test_burst_cypher_core_shape():
    c = build_burst_cypher(watched_only=False)
    assert "EventOrAction)-[r:PARTICIPATED_IN]->(p:__Entity__)" in c
    assert "r.polarity IS NULL OR r.polarity <> 'negated'" in c
    assert "$since_recent" in c and "$since_baseline" in c
    assert "burst_score" in c and "$min_count" in c and "$ratio" in c
    assert "ORDER BY burst_score DESC" in c and "LIMIT $top_n" in c
    assert "p.watched = true" not in c


def test_burst_cypher_watched_only_adds_clause():
    assert "p.watched = true" in build_burst_cypher(watched_only=True)

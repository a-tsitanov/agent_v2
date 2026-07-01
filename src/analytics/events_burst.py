"""E3 — shared burst computation over event created_at (single source for the
trending_events primitive and the in-sweep burst detector)."""

from __future__ import annotations


def build_burst_cypher(*, watched_only: bool) -> str:
    """Parameterized burst query grouped by (participant entity, event_type).

    recent  = events with created_at >= $since_recent
    baseline_rate = events in [$since_baseline, $since_recent) / $baseline_windows
    burst_score = recent / max(baseline_rate, 1)
    Filtered by recent >= $min_count AND burst_score >= $ratio.
    """
    watched = "AND p.watched = true " if watched_only else ""
    return (
        "MATCH (e:__Entity__:EventOrAction)-[r:PARTICIPATED_IN]->(p:__Entity__) "
        "WHERE (r.polarity IS NULL OR r.polarity <> 'negated') "
        "AND e.created_at >= $since_baseline "
        f"{watched}"
        "WITH p.name AS entity, e.event_type AS event_type, "
        "sum(CASE WHEN e.created_at >= $since_recent THEN 1 ELSE 0 END) AS recent, "
        "sum(CASE WHEN e.created_at < $since_recent THEN 1 ELSE 0 END) AS baseline_total "
        "WITH entity, event_type, recent, "
        "(toFloat(baseline_total) / $baseline_windows) AS baseline_rate "
        "WITH entity, event_type, recent, baseline_rate, "
        "(toFloat(recent) / (CASE WHEN baseline_rate < 1 THEN 1 ELSE baseline_rate END)) "
        "AS burst_score "
        "WHERE recent >= $min_count AND burst_score >= $ratio "
        "RETURN entity, event_type, recent, baseline_rate, burst_score "
        "ORDER BY burst_score DESC, recent DESC LIMIT $top_n"
    )

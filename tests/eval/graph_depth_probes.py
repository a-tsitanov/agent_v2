"""Probe set for benchmarking graph neighbour depth (path_depth 1 vs 2).

Purpose: decide — with evidence — whether to raise the default
``graph_search`` depth / ``settings.agent.graph_search_path_depth`` above
1. These are connection-oriented questions where a 1-hop neighbourhood is
expected to MISS the answer and a 2-hop neighbourhood should surface it.

Running the comparison is MANUAL (needs a populated Neo4j graph): for each
probe, call ``graph_search(query, depth=1)`` then ``depth=2`` and compare
the returned entities/relations against ``expects_at_2hops``. Score recall
uplift and the extra node/token cost; only raise the default if the uplift
is worth the cost. This module just versions the probe set + guards that it
stays non-trivial; it does not hit a live store in CI.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DepthProbe:
    name: str
    query: str
    # What a 2-hop neighbourhood should surface that 1-hop typically won't
    # (free-text expectation for manual grading).
    expects_at_2hops: str


PROBES: tuple[DepthProbe, ...] = (
    DepthProbe(
        name="indirect_employer",
        query="С кем связан директор ООО «Ромашка»?",
        expects_at_2hops="контрагенты/сотрудники на 2-м хопе от директора",
    ),
    DepthProbe(
        name="shared_counterparty",
        query="Есть ли общий контрагент у Иванова и Петрова?",
        expects_at_2hops="общий узел-контрагент, достижимый за 2 хопа от обоих",
    ),
    DepthProbe(
        name="chain_via_org",
        query="Как связаны Иванов и компания «Лютик»?",
        expects_at_2hops="путь Иванов → организация → «Лютик»",
    ),
    DepthProbe(
        name="beneficiary_through_owner",
        query="Кто бенефициар через цепочку владения для «Василёк»?",
        expects_at_2hops="владелец владельца (2 хопа по OWNS)",
    ),
)


def test_probe_set_is_nontrivial():
    assert len(PROBES) >= 4
    assert all(p.query.strip() and p.expects_at_2hops.strip() for p in PROBES)


def test_probe_names_unique():
    names = [p.name for p in PROBES]
    assert len(names) == len(set(names))

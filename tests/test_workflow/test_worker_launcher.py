"""Pure launcher helpers: group selection + per-process metrics port.

The worker entrypoint forks one child process per worker pool so a block
in one pool can't freeze the others (separate event loop + GIL). These
helpers decide WHICH pools a process runs and WHICH metrics port it binds
(7 processes must not fight over one Prometheus port)."""

from __future__ import annotations

import pytest

from src.workflow.worker import (
    WORKER_GROUPS,
    metrics_port_for,
    select_groups,
)


def test_default_selects_all_groups_in_canonical_order():
    assert select_groups(None) == WORKER_GROUPS
    assert select_groups("") == WORKER_GROUPS


def test_explicit_subset_preserves_canonical_order_and_trims():
    # request out of order + whitespace → canonical order, deduped
    out = select_groups(" merge , llm , llm ")
    assert out == [g for g in WORKER_GROUPS if g in {"llm", "merge"}]


def test_unknown_group_raises():
    with pytest.raises(ValueError, match="bogus"):
        select_groups("llm,bogus")


def test_metrics_port_is_distinct_per_group():
    base = 9000
    ports = {g: metrics_port_for(base, g) for g in WORKER_GROUPS}
    # all distinct
    assert len(set(ports.values())) == len(WORKER_GROUPS)
    # deterministic offset from base by canonical index
    assert ports[WORKER_GROUPS[0]] == base
    assert ports[WORKER_GROUPS[1]] == base + 1


def test_metrics_port_unknown_group_raises():
    with pytest.raises(ValueError):
        metrics_port_for(9000, "bogus")

"""Guard: the analytics materialize heartbeat window must exceed its GIL-held
compute — the same failure `test_community_build_hardening` already covers for
leidenalg, which materialize never got.

`materialize_centrality` computes centrality in-worker with python-igraph
(`g.pagerank` / `g.betweenness` / `g.eigenvector_centrality`).  Those are C calls
that HOLD THE GIL for their whole duration, so the `heartbeat_every(30.0)` pulse
inside the activity cannot fire — `asyncio.to_thread` does not help, because the
worker thread holds the GIL and the event loop never runs.

Measured on the production graph (V=78829, E=123908):

    export edges          5.4s
    pagerank              0.8s
    eigenvector           0.5s
    betweenness        1877.3s   (= 31.3 min, one GIL-held call)

`_HB` was 2 minutes, so the activity died at exactly 120s on every single run —
three consecutive AnalyticsMaterializeWorkflow failures, all
`activity Heartbeat timeout`, never once reaching the link-prediction or risk
stage.  betweenness is O(V*E) and cannot be dropped: risk scoring reads it for
the `brokerage` component and the `centrality` analytics primitive exposes it.
"""

from __future__ import annotations

from src.workflow.analytics.materialize_workflow import _HB, _S2C, _START

# Longest measured GIL-held compute (betweenness on the production graph).
_MEASURED_BETWEENNESS_S = 1877.3


def test_heartbeat_window_exceeds_the_gil_held_centrality_compute():
    # Must clear the measured compute with headroom — a pulse cannot be emitted
    # from inside the igraph C call, so the window IS the only thing keeping a
    # healthy-but-slow run alive.
    assert _HB.total_seconds() > _MEASURED_BETWEENNESS_S
    # ...and stay under the start-to-close ceiling, which remains the real
    # bound on a genuinely stuck run.
    assert _HB <= _START


def test_start_to_close_leaves_room_for_graph_growth():
    # betweenness is O(V*E): doubling the graph roughly quadruples the compute,
    # so a ceiling merely above today's 31min would break on the next milestone.
    assert _START.total_seconds() >= 3 * _MEASURED_BETWEENNESS_S
    assert _START <= _S2C

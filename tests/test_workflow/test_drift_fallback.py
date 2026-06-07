from src.workflow.contracts import SearchOutcome
from src.workflow.search.router_wf import _drift_local_fallback


def _outcome(**kw):
    base = dict(query="q", mode="local", answer="A", sources=[], documents=["d1"])
    base.update(kw)
    return SearchOutcome(**base)


def test_drift_fallback_relabels_local_as_drift():
    local = _outcome(mode="local", answer="local ans", documents=["d1"])
    out = _drift_local_fallback(local)
    assert out.mode == "drift"
    assert out.answer == "local ans"
    assert out.documents == ["d1"]

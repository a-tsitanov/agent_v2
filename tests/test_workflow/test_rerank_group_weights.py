from src.workflow.contracts import SerializedNode
from src.workflow.search.activities.rerank import apply_group_weights


def _n(cid, score, group):
    return SerializedNode(chunk_id=cid, text=cid, score=score, metadata={"doc_group": group})


def test_weight_reorders_by_group():
    # opinion starts higher but is down-weighted; official is boosted.
    pool = [_n("op", 1.00, "opinion"), _n("of", 0.90, "official")]
    weights = {"opinion": 0.8, "official": 1.3}
    out = apply_group_weights(pool, weights)
    assert [n.chunk_id for n in out] == ["of", "op"]  # 0.90*1.3=1.17 > 1.00*0.8=0.80


def test_missing_group_weight_is_identity():
    pool = [_n("a", 0.5, ""), _n("b", 0.4, "news")]
    out = apply_group_weights(pool, {"official": 1.3})
    assert [n.chunk_id for n in out] == ["a", "b"]  # order unchanged (both ×1.0)

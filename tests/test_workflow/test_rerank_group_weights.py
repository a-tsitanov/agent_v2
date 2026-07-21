from src.workflow.contracts import SerializedNode
from src.workflow.search.activities.rerank import apply_group_weights


def _n(cid, score, group):
    return SerializedNode(chunk_id=cid, text=cid, score=score, metadata={"doc_group": group})


def test_weight_reorders_by_group():
    # opinion starts higher but is down-weighted; official is boosted.
    # Scores are sigmoid-normalized before weighting:
    #   sigmoid(0.90)*1.3 = 0.7110*1.3 = 0.9242
    #   sigmoid(1.00)*0.8 = 0.7311*0.8 = 0.5848  -> official still wins.
    pool = [_n("op", 1.00, "opinion"), _n("of", 0.90, "official")]
    weights = {"opinion": 0.8, "official": 1.3}
    out = apply_group_weights(pool, weights)
    assert [n.chunk_id for n in out] == ["of", "op"]


def test_missing_group_weight_is_identity():
    pool = [_n("a", 0.5, ""), _n("b", 0.4, "news")]
    out = apply_group_weights(pool, {"official": 1.3})
    assert [n.chunk_id for n in out] == ["a", "b"]  # order unchanged (both ×1.0)


def test_negative_logits_do_not_invert_group_direction():
    """Regression test: raw cross-encoder scores are UNBOUNDED LOGITS and
    frequently negative for lower-ranked chunks. Before sigmoid-normalizing,
    multiplying a negative score by a boost (>1) made it MORE negative
    (demoting it) and by a penalty (<1) made it LESS negative (promoting
    it) — inverting the intended direction. With the same raw score of
    -2.0 for both, the old code ranked `opinion` (x0.80 -> -1.6) ABOVE
    `official` (x1.30 -> -2.6). Sigmoid-normalizing first fixes this:
    both map to sigmoid(-2.0) ~= 0.1192 before weighting, so official
    (x1.30 -> 0.1550) correctly outranks opinion (x0.80 -> 0.0954).
    """
    pool = [_n("op", -2.0, "opinion"), _n("of", -2.0, "official")]
    weights = {"opinion": 0.8, "official": 1.3}
    out = apply_group_weights(pool, weights)
    assert [n.chunk_id for n in out] == ["of", "op"]

from src.graph.entity_resolution import _partition_cached, _verdict_key


class _Item:
    def __init__(self, norm, label="Person"):
        self.norm, self.label = norm, label


def test_verdict_key_is_order_insensitive():
    a, b = _Item("ivanov"), _Item("ivanoff")
    assert _verdict_key(a, b) == _verdict_key(b, a)


def test_partition_cached_splits_known_from_unknown():
    a, b, c = _Item("x"), _Item("y"), _Item("z")
    pairs = [(a, b), (a, c)]
    cache = {_verdict_key(a, b): True}
    cached, uncached = _partition_cached(pairs, cache)
    assert cached == [((a, b), True)]
    assert uncached == [(a, c)]

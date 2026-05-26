import asyncio
from src.graph.canonical_linker import CanonicalLinker, CanonicalCandidate


class _FakeIndex:
    def __init__(self, alias_hit=None, knn=None):
        self._alias_hit = alias_hit
        self._knn = knn or []

    def alias_lookup(self, surface, label):
        return self._alias_hit

    def knn(self, embedding, label, k):
        return self._knn


def test_exact_alias_links_without_llm():
    idx = _FakeIndex(alias_hit=CanonicalCandidate(qid="Q5", name="Сбербанк", score=1.0))
    linker = CanonicalLinker(index=idx, llm=None, embed=None)
    qid = asyncio.run(linker.link("Сбер", "Organization", embedding=[0.0]))
    assert qid == "Q5"


def test_no_candidate_returns_none_to_mint_new():
    idx = _FakeIndex(alias_hit=None, knn=[])
    linker = CanonicalLinker(index=idx, llm=None, embed=None)
    qid = asyncio.run(linker.link("Новая Фирма", "Organization", embedding=[0.1]))
    assert qid is None


def test_high_cosine_same_script_links_without_llm():
    cand = CanonicalCandidate(qid="Q9", name="Acme Corp", score=0.93)
    idx = _FakeIndex(alias_hit=None, knn=[cand])
    linker = CanonicalLinker(index=idx, llm=None, embed=None, auto_link_threshold=0.9)
    qid = asyncio.run(linker.link("Acme Corporation", "Organization", embedding=[0.2]))
    assert qid == "Q9"

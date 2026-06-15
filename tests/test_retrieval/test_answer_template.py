"""Unit tests for server-side answer templates (Track 6, variant a)."""

from __future__ import annotations

from src.retrieval.answer_template import build_query, load_template


def test_no_template_falls_back_to_ru_preamble():
    q = build_query("кто такой Иванов?", "")
    assert "на русском" in q.lower()
    assert "Иванов" in q


def test_inline_template_is_framed_into_query():
    q = build_query("вопрос?", "СТРОКА1\nСТРОКА2")
    assert "СТРОКА1" in q and "СТРОКА2" in q
    assert "вопрос?" in q
    assert "формат" in q.lower()


def test_named_template_loads_from_disk():
    t = load_template("dossier")
    assert "Каноническое имя" in t


def test_unknown_name_is_treated_as_inline_not_file():
    # path traversal / unknown name must NOT read a file off disk
    t = load_template("../../etc/passwd")
    assert "root:" not in t
    assert t.startswith("../../etc/passwd") or t == "../../etc/passwd"


def test_oversized_template_is_capped():
    big = "x" * 50_000
    t = load_template(big)
    assert len(t) < 50_000


def test_build_synthesize_call_threads_template():
    from src.workflow.search.orchestrator import build_synthesize_call
    _, params = build_synthesize_call(
        query="q", sources=[], max_refinements=3, answer_template="dossier",
    )
    assert params.answer_template == "dossier"


def test_local_params_maps_answer_template():
    from src.api.routes.search_v2 import _local_params
    from src.models.search import SearchRequest
    p = _local_params(SearchRequest(query="q", answer_template="dossier"))
    assert p.answer_template == "dossier"

from tests.eval.ner_eval import NERStats, score_case


def test_score_case_counts_tp_fp_fn_per_type():
    expected = {"Person": ["Иванов И.П."], "Organization": ["ООО Ромашка"]}
    predicted = [("Иванов И.П.", "Person"), ("ЗАО Лютик", "Organization")]
    stats: dict[str, NERStats] = {}
    score_case(expected, predicted, stats, lang="ru")

    assert stats["Person"].tp == 1
    assert stats["Person"].fn == 0
    assert stats["Organization"].tp == 0       # wrong surface
    assert stats["Organization"].fn == 1       # expected one, missed it
    assert stats["Organization"].fp == 1       # predicted a wrong one


def test_f1_is_harmonic_mean():
    s = NERStats(tp=8, fp=2, fn=2)
    assert abs(s.precision - 0.8) < 1e-9
    assert abs(s.recall - 0.8) < 1e-9
    assert abs(s.f1 - 0.8) < 1e-9

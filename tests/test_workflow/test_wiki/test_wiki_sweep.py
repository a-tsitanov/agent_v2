from src.workflow.wiki.wiki_sweep import _tally, ArticleOutcome


def test_tally_counts_outcomes():
    res = _tally([
        ArticleOutcome.WRITTEN, ArticleOutcome.SKIPPED,
        ArticleOutcome.WRITTEN, ArticleOutcome.FAILED, None,
    ])
    assert res == {"written": 2, "skipped_unchanged": 1, "failed": 1}

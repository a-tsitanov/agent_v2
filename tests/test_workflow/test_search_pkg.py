def test_search_package_importable():
    import src.workflow.search as s
    import src.workflow.search.activities as a
    assert s is not None and a is not None

from src.graph.wiki_context import EntityContext, subgraph_hash


def _ctx(**kw):
    base = dict(
        name="ООО Альфа", label="Organization", description="A supplier.",
        wikibase_qid="Q5", page_title="ООО Альфа",
        relations=[
            ("заключила договор", "out", "Договор № 17-К", "Document", "signed"),
            ("контакт", "out", "+74951234567", "PhoneNumber", ""),
        ],
    )
    base.update(kw)
    return EntityContext(**base)


def test_hash_is_deterministic_and_order_independent():
    a = _ctx()
    b = _ctx(relations=list(reversed(_ctx().relations)))  # reordered
    assert subgraph_hash(a) == subgraph_hash(b)


def test_hash_changes_when_a_relation_changes():
    a = _ctx()
    b = _ctx(relations=_ctx().relations + [("платит", "out", "X", "Amount", "")])
    assert subgraph_hash(a) != subgraph_hash(b)


def test_hash_changes_on_description_change():
    assert subgraph_hash(_ctx()) != subgraph_hash(_ctx(description="changed"))


def test_hash_ignores_qid_and_page_title():
    assert subgraph_hash(_ctx(wikibase_qid="Q9", page_title="other")) == \
           subgraph_hash(_ctx())

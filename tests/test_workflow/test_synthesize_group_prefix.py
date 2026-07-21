from src.workflow.contracts import SerializedNode
from src.workflow.activities.synthesize_answer import with_group_prefix


def test_prefixes_group():
    sn = SerializedNode(chunk_id="c", text="body", metadata={"doc_group": "official"})
    assert with_group_prefix(sn).text == "[official] body"


def test_no_group_is_identity():
    sn = SerializedNode(chunk_id="c", text="body", metadata={})
    assert with_group_prefix(sn).text == "body"
